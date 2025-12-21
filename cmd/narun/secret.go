package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/akhenakh/narun/internal/crypto"
	"github.com/akhenakh/narun/internal/noderunner"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Secret Command Handling
func handleSecretCmd(args []string) {
	if len(args) < 1 {
		printSecretUsage()
		os.Exit(1)
	}

	subcommand := args[0]
	subcommandArgs := args[1:]

	switch subcommand {
	case "set":
		handleSecretSetCmd(subcommandArgs)
	case "list":
		handleSecretListCmd(subcommandArgs)
	case "delete":
		handleSecretDeleteCmd(subcommandArgs)
	case "help", "-h", "--help":
		printSecretUsage()
	default:
		fmt.Fprintf(os.Stderr, "Error: Unknown secret subcommand '%s'\n\n", subcommand)
		printSecretUsage()
		os.Exit(1)
	}
}

func printSecretUsage() {
	fmt.Fprintf(os.Stderr, `Usage: %s secret <subcommand> [options] [arguments...]

Manage encrypted secrets stored in NATS KV.

Subcommands:
  set       Add or update a secret. Requires master key.
  list      List the names of stored secrets. Does not require master key.
  delete    Delete a secret. Does not require master key.
  help      Show this help message.

Common Options (apply to all subcommands):
  -nats <url>     NATS server URL (default: %s)
  -nkey-file <path>  Path to NKey seed file
  -timeout <dur>  Timeout for NATS operations (default: %s)

Options for 'set':
  -master-key <key>   Base64 encoded AES-256 master key (REQUIRED, or use NARUN_MASTER_KEY env var).
  -master-key-path <path> Path to file containing the master key.
  <name>              The name/key of the secret (required argument).
  <value>             The plaintext value of the secret (required argument).
  -desc <text>        Optional description for the secret.

Options for 'list':
  (No specific options)

Options for 'delete':
  -y                  Skip confirmation prompt.
  <name>              The name/key of the secret to delete (required argument).

`, os.Args[0], DefaultNatsURL, DefaultTimeout)
}

// --- Subcommand Handlers ---
func handleSecretSetCmd(args []string) {
	setFlags := flag.NewFlagSet("secret set", flag.ExitOnError)
	natsURL := setFlags.String("nats", DefaultNatsURL, "NATS server URL")
	nkeyFile := setFlags.String("nkey-file", os.Getenv("NARUN_NKEY_FILE"), "Path to NKey seed file")
	timeout := setFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	masterKeyBase64 := setFlags.String("master-key", os.Getenv("NARUN_MASTER_KEY"), "Base64 encoded AES-256 master key")
	masterKeyPath := setFlags.String("master-key-path", os.Getenv("NARUN_MASTER_KEY_PATH"), "Path to file containing the master key")
	description := setFlags.String("desc", "", "Optional description for the secret")

	setFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s secret set [options] <name> <value>

Adds or updates an encrypted secret in the NATS KV store '%s'.

Arguments:
  <name>    The name/key of the secret (required).
  <value>   The plaintext value of the secret (required).

Options:
`, os.Args[0], noderunner.SecretKVBucket)
		setFlags.PrintDefaults()
	}

	if err := setFlags.Parse(args); err != nil {
		return
	}
	if setFlags.NArg() != 2 {
		slog.Error("Error: 'secret set' requires exactly two arguments: <name> and <value>")
		setFlags.Usage()
		os.Exit(1)
	}
	secretName := setFlags.Arg(0)
	secretValue := setFlags.Arg(1)

	if *masterKeyPath != "" {
		keyBytes, err := os.ReadFile(*masterKeyPath)
		if err != nil {
			slog.Error("Failed to read master key file", "path", *masterKeyPath, "error", err)
			os.Exit(1)
		}
		*masterKeyBase64 = strings.TrimSpace(string(keyBytes))
	}

	if *masterKeyBase64 == "" {
		slog.Error("Error: Master key is required for 'secret set'. Use -master-key, -master-key-path, or NARUN_MASTER_KEY env var.")
		os.Exit(1)
	}
	if secretName == "" {
		slog.Error("Error: Secret name cannot be empty.")
		os.Exit(1)
	}

	masterKey, err := crypto.DeriveKeyFromBase64(*masterKeyBase64)
	if err != nil {
		slog.Error("Failed to derive master key", "error", err)
		os.Exit(1)
	}

	slog.Info("Encrypting secret...", "name", secretName)
	aad := []byte(secretName)
	ciphertext, nonce, err := crypto.Encrypt(masterKey, []byte(secretValue), aad)
	if err != nil {
		slog.Error("Encryption failed", "name", secretName, "error", err)
		os.Exit(1)
	}

	now := time.Now()
	storedSecret := noderunner.StoredSecret{
		Name:        secretName,
		Ciphertext:  ciphertext,
		Nonce:       nonce,
		Description: *description,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	nc, js, err := connectNATS(ctx, *natsURL, *nkeyFile, "narun-cli-secret-set")
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	kvSecrets, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: noderunner.SecretKVBucket, History: 1,
	})
	if err != nil {
		slog.Error("Failed to access/create secrets KV store", "bucket", noderunner.SecretKVBucket, "error", err)
		os.Exit(1)
	}

	existingEntry, getErr := kvSecrets.Get(ctx, secretName)
	if getErr == nil {
		var existingSecret noderunner.StoredSecret
		if json.Unmarshal(existingEntry.Value(), &existingSecret) == nil {
			storedSecret.CreatedAt = existingSecret.CreatedAt
		} else {
			slog.Warn("Could not unmarshal existing secret to preserve CreatedAt", "name", secretName)
		}
	} else if !errors.Is(getErr, jetstream.ErrKeyNotFound) {
		slog.Warn("Could not check for existing secret", "name", secretName, "error", getErr)
	}

	secretData, err := json.Marshal(storedSecret)
	if err != nil {
		slog.Error("Failed to marshal secret data", "name", secretName, "error", err)
		os.Exit(1)
	}

	rev, err := kvSecrets.Put(ctx, secretName, secretData)
	if err != nil {
		slog.Error("Failed to put secret into KV store", "name", secretName, "error", err)
		os.Exit(1)
	}
	slog.Info("Secret set successfully", "name", secretName, "revision", rev)
}

func handleSecretListCmd(args []string) {
	listFlags := flag.NewFlagSet("secret list", flag.ExitOnError)
	natsURL := listFlags.String("nats", DefaultNatsURL, "NATS server URL")
	nkeyFile := listFlags.String("nkey-file", os.Getenv("NARUN_NKEY_FILE"), "Path to NKey seed file")
	timeout := listFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")

	listFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s secret list [options]

Lists the names of secrets stored in the NATS KV store '%s'.

Options:
`, os.Args[0], noderunner.SecretKVBucket)
		listFlags.PrintDefaults()
	}
	if err := listFlags.Parse(args); err != nil {
		return
	}
	if listFlags.NArg() != 0 {
		slog.Error("Error: 'secret list' takes no arguments.")
		listFlags.Usage()
		os.Exit(1)
	}

	slog.Info("Listing secrets...")
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	nc, js, err := connectNATS(ctx, *natsURL, *nkeyFile, "narun-cli-secret-list")
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	kvSecrets, err := js.KeyValue(ctx, noderunner.SecretKVBucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, nats.ErrBucketNotFound) {
			slog.Info("Secrets KV store not found. No secrets to list.", "bucket", noderunner.SecretKVBucket)
			return
		}
		slog.Error("Failed to access secrets KV store", "bucket", noderunner.SecretKVBucket, "error", err)
		os.Exit(1)
	}

	keysLister, err := kvSecrets.ListKeys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			slog.Info("No secrets found in the store.")
			return
		}
		slog.Error("Failed to list secret keys", "error", err)
		os.Exit(1)
	}
	defer keysLister.Stop()

	secretNames := make([]string, 0)
	keysChan := keysLister.Keys()
listLoop:
	for {
		select {
		case <-ctx.Done():
			slog.Warn("Timed out listing secret keys", "error", ctx.Err())
			break listLoop
		case key, ok := <-keysChan:
			if !ok {
				break listLoop
			}
			if key != "" {
				secretNames = append(secretNames, key)
			}
		}
	}
	if ctx.Err() != nil {
		os.Exit(1)
	}
	if len(secretNames) == 0 {
		slog.Info("No secrets found.")
		return
	}
	sort.Strings(secretNames)
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "SECRET NAME")
	fmt.Fprintln(tw, "-----------")
	for _, name := range secretNames {
		fmt.Fprintf(tw, "%s\n", name)
	}
	tw.Flush()
	slog.Info(fmt.Sprintf("Found %d secret(s).", len(secretNames)))
}

func handleSecretDeleteCmd(args []string) {
	deleteFlags := flag.NewFlagSet("secret delete", flag.ExitOnError)
	natsURL := deleteFlags.String("nats", DefaultNatsURL, "NATS server URL")
	nkeyFile := deleteFlags.String("nkey-file", os.Getenv("NARUN_NKEY_FILE"), "Path to NKey seed file")
	timeout := deleteFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	skipConfirm := deleteFlags.Bool("y", false, "Skip confirmation prompt")

	deleteFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s secret delete [options] <name>

Deletes a secret from the NATS KV store '%s'.

Arguments:
  <name>    The name/key of the secret to delete (required).

Options:
`, os.Args[0], noderunner.SecretKVBucket)
		deleteFlags.PrintDefaults()
	}
	if err := deleteFlags.Parse(args); err != nil {
		return
	}
	if deleteFlags.NArg() != 1 {
		slog.Error("Error: 'secret delete' requires exactly one argument: <name>")
		deleteFlags.Usage()
		os.Exit(1)
	}
	secretName := deleteFlags.Arg(0)

	if !*skipConfirm {
		fmt.Printf("WARNING: This will permanently delete the secret '%s'.\n", secretName)
		fmt.Print("Are you sure you want to proceed? (yes/no): ")
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(response) != "yes" {
			slog.Info("Delete operation cancelled.")
			os.Exit(0)
		}
	}

	slog.Info("Deleting secret...", "name", secretName)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	nc, js, err := connectNATS(ctx, *natsURL, *nkeyFile, "narun-cli-secret-delete")
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	kvSecrets, err := js.KeyValue(ctx, noderunner.SecretKVBucket)
	if err != nil {
		slog.Error("Failed to access secrets KV store", "bucket", noderunner.SecretKVBucket, "error", err)
		os.Exit(1)
	}

	err = kvSecrets.Purge(ctx, secretName)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			slog.Error(fmt.Sprintf("Secret '%s' not found. Nothing to delete.", secretName))
			os.Exit(0)
		}
		slog.Error("Failed to delete secret", "name", secretName, "error", err)
		os.Exit(1)
	}
	slog.Info("Secret deleted successfully", "name", secretName)
}
