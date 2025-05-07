package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/akhenakh/narun/internal/noderunner"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

// Configuration Constants (Shared) - Copied from noderunner/config.go
const (
	DefaultNatsURL = "nats://localhost:4222"
	DefaultTimeout = 15 * time.Second
)

// LogEntry structure (for logs command)
type logEntry struct {
	InstanceID string    `json:"instance_id"`
	AppName    string    `json:"app_name"`
	NodeID     string    `json:"node_id"`
	Stream     string    `json:"stream"`
	Message    string    `json:"message"`
	Timestamp  time.Time `json:"timestamp"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	logLevel := slog.LevelInfo
	if levelStr := os.Getenv("LOG_LEVEL"); levelStr != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(levelStr)); err == nil {
			logLevel = level
		}
	}
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true,
	})

	logger := slog.New(logHandler).With("service", "narun-cli")
	slog.SetDefault(logger)

	command := os.Args[1]
	args := os.Args[2:]

	// Special case for 'deploy -h' or 'deploy --help'
	if command == "deploy" && len(args) > 0 && (args[0] == "-h" || args[0] == "--help") {
		printDeployUsage()
		os.Exit(0)
	}
	if command == "service" && len(args) > 0 && (args[0] == "-h" || args[0] == "--help") {
		printServiceUsage()
		os.Exit(0)
	}
	if command == "service" && len(args) > 1 && (args[0] == "list" || args[0] == "delete" || args[0] == "info") && (args[1] == "-h" || args[1] == "--help") {
		switch args[0] {
		case "list":
			printServiceListUsage()
		case "delete":
			printServiceDeleteUsage()
		case "info":
			printServiceInfoUsage()
		}
		os.Exit(0)
	}

	// Normal Command Handling
	switch command {
	case "deploy":
		handleDeployCmd(args)
	case "logs":
		handleLogsCmd(args)
	case "list-images": // This command remains as is
		handleListImagesCmd(args)
	case "service": // New top-level command for service management
		handleServiceCmd(args)
	case "secret":
		handleSecretCmd(args)
	case "files":
		handleFilesCmd(args)
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Error: Unknown command '%s'\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Narun Management CLI

Usage: %s <command> [options] [arguments...]

Commands:
  deploy        Upload application binaries and configuration. Run '%s deploy -h' for details.
  service       Manage deployed services (list, info, delete). Run '%s service -h' for details.
  logs          Stream logs from node runners.
  list-images   List application binaries stored in NATS Object Store.
  secret        Manage encrypted secrets (set, list, delete). Run '%s secret help' for details.
  files         Manage shared files (add, list, delete). Run '%s files help' for details.
  help          Show this help message.

Common Options (apply to multiple commands where relevant):
  -nats <url>     NATS server URL (default: %s)
  -timeout <dur>  Timeout for NATS operations (default: %s)

`, os.Args[0], os.Args[0], os.Args[0], os.Args[0], os.Args[0], DefaultNatsURL, DefaultTimeout)
}

func printDeployUsage() {
	deployFlags := flag.NewFlagSet("deploy", flag.ExitOnError)
	deployFlags.String("nats", DefaultNatsURL, "NATS server URL")
	deployFlags.String("config", "", "Path to the application ServiceSpec YAML configuration file (optional).")
	deployFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	deployFlags.String("name", "", "Application name (required if -config is not used)")
	deployFlags.String("tag", "", "Binary tag (required if -config is not used)")
	deployFlags.String("node", "local", "Target node name (used if -config is not used)")
	deployFlags.Int("replicas", 1, "Number of replicas on the target node (used if -config is not used)")
	fmt.Fprintf(os.Stderr, `Usage: %s deploy [options] <binary_path> [<binary_path>...]

Uploads application binaries and configuration.

Arguments:
  <binary_path>   Path to the application binary file (at least one required).

Options:
`, os.Args[0])
	deployFlags.PrintDefaults()
	fmt.Fprintln(os.Stderr, "\nNote: If -config is provided, the -name, -tag, -node, and -replicas flags are ignored.")
}

// --- Service Command Handling ---
func handleServiceCmd(args []string) {
	if len(args) < 1 {
		printServiceUsage()
		os.Exit(1)
	}

	subcommand := args[0]
	subcommandArgs := args[1:]

	switch subcommand {
	case "list":
		handleServiceListCmd(subcommandArgs)
	case "delete":
		handleServiceDeleteCmd(subcommandArgs)
	case "info":
		handleServiceInfoCmd(subcommandArgs)
	case "help", "-h", "--help":
		printServiceUsage()
	default:
		fmt.Fprintf(os.Stderr, "Error: Unknown service subcommand '%s'\n\n", subcommand)
		printServiceUsage()
		os.Exit(1)
	}
}

func printServiceUsage() {
	fmt.Fprintf(os.Stderr, `Usage: %s service <subcommand> [options] [arguments...]

Manage deployed services (applications).

Subcommands:
  list      List deployed services and their status on nodes.
  info      Display detailed information and status for a specific service.
  delete    Delete a service configuration from NATS KV.
  help      Show this help message.

Common Options (apply to all 'service' subcommands):
  -nats <url>     NATS server URL (default: %s)
  -timeout <dur>  Timeout for NATS operations (default: %s)

Use "%s service <subcommand> -h" for subcommand-specific help.
`, os.Args[0], DefaultNatsURL, DefaultTimeout, os.Args[0])
}

func printServiceListUsage() {
	fmt.Fprintf(os.Stderr, `Usage: %s service list [options]

Lists deployed services and their status on nodes.

Options:
  -nats <url>     NATS server URL (default: %s)
  -timeout <dur>  Timeout for NATS operations (default: %s)
`, os.Args[0], DefaultNatsURL, DefaultTimeout)
}

func printServiceInfoUsage() {
	fmt.Fprintf(os.Stderr, `Usage: %s service info [options] <service_name>

Displays detailed information and status for a specific service.

Arguments:
  <service_name>  Name of the service to inspect (required).

Options:
  -nats <url>     NATS server URL (default: %s)
  -timeout <dur>  Timeout for NATS operations (default: %s)
`, os.Args[0], DefaultNatsURL, DefaultTimeout)
}

func printServiceDeleteUsage() {
	fmt.Fprintf(os.Stderr, `Usage: %s service delete [options] <service_name>

Deletes a service configuration from NATS KV. This will cause node runners
to stop and remove instances of this service.

Arguments:
  <service_name>   Name of the service configuration to delete (required).

Options:
  -nats <url>     NATS server URL (default: %s)
  -timeout <dur>  Timeout for NATS operations (default: %s)
  -y              Skip confirmation prompt.
`, os.Args[0], DefaultNatsURL, DefaultTimeout)
}

// Deploy Command Logic
func handleDeployCmd(args []string) {
	deployFlags := flag.NewFlagSet("deploy", flag.ExitOnError)
	natsURL := deployFlags.String("nats", DefaultNatsURL, "NATS server URL")
	configFile := deployFlags.String("config", "", "Path to the application ServiceSpec YAML configuration file (optional). If provided, overrides -name, -tag, -node, -replicas.")
	timeout := deployFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	appNameFlag := deployFlags.String("name", "", "Application name (required if -config is not used)")
	tagFlag := deployFlags.String("tag", "", "Binary tag tag (required if -config is not used)")
	nodeFlag := deployFlags.String("node", "local", "Target node name (used if -config is not used)")
	replicasFlag := deployFlags.Int("replicas", 1, "Number of replicas on the target node (used if -config is not used)")

	deployFlags.Usage = printDeployUsage // Use the specific usage function

	if err := deployFlags.Parse(args); err != nil {
		return
	}
	binaryFiles := deployFlags.Args()
	if len(binaryFiles) == 0 {
		slog.Error("Deploy Error: At least one binary file path must be provided.")
		deployFlags.Usage()
		os.Exit(1)
	}

	var spec noderunner.ServiceSpec
	var configData []byte
	var err error

	if *configFile != "" {
		slog.Info("Starting Deployment using Config File", "config", *configFile, "binaries", binaryFiles, "nats", *natsURL)
		if _, err = os.Stat(*configFile); os.IsNotExist(err) {
			slog.Error("Deploy Error: Config file not found", "path", *configFile)
			os.Exit(1)
		}
		configData, err = os.ReadFile(*configFile)
		if err != nil {
			slog.Error("Deploy Error: reading config file", "path", *configFile, "error", err)
			os.Exit(1)
		}
		if err = yaml.Unmarshal(configData, &spec); err != nil {
			slog.Error("Deploy Error: parsing ServiceSpec YAML", "path", *configFile, "error", err)
			os.Exit(1)
		}
		if spec.Name == "" {
			slog.Error("Deploy Error: 'name' field is missing or empty in config file", "path", *configFile)
			os.Exit(1)
		}
		if spec.Tag == "" {
			slog.Error("Deploy Error: 'tag' field is missing or empty in config file", "path", *configFile)
			os.Exit(1)
		}
		for i, nodeSpec := range spec.Nodes {
			if strings.TrimSpace(nodeSpec.Name) == "" {
				slog.Error("Deploy Error in config: node selector name empty", "path", *configFile, "index", i)
				os.Exit(1)
			}
			if nodeSpec.Replicas <= 0 {
				slog.Error("Deploy Error in config: node selector replicas must be positive", "path", *configFile, "node_name", nodeSpec.Name)
				os.Exit(1)
			}
		}
		slog.Info("Deploying ServiceSpec from file",
			"config_file", *configFile, "app_name", spec.Name, "tag", spec.Tag, "target_nodes", fmt.Sprintf("%+v", spec.Nodes))

	} else {
		slog.Info("Starting Deployment using Flags", "binaries", binaryFiles, "nats", *natsURL)
		if *appNameFlag == "" {
			slog.Error("Deploy Error: -name flag is required when -config is not provided.")
			deployFlags.Usage()
			os.Exit(1)
		}
		if *tagFlag == "" {
			slog.Error("Deploy Error: -tag flag is required when -config is not provided.")
			deployFlags.Usage()
			os.Exit(1)
		}
		if *replicasFlag <= 0 {
			slog.Error("Deploy Error: -replicas must be positive.")
			deployFlags.Usage()
			os.Exit(1)
		}
		if strings.TrimSpace(*nodeFlag) == "" {
			slog.Error("Deploy Error: -node cannot be empty.")
			deployFlags.Usage()
			os.Exit(1)
		}

		spec = noderunner.ServiceSpec{
			Name: *appNameFlag,
			Tag:  *tagFlag,
			Nodes: []noderunner.NodeSelectorSpec{
				{
					Name:     *nodeFlag,
					Replicas: *replicasFlag,
				},
			},
		}
		slog.Info("Deploying Generated ServiceSpec",
			"app_name", spec.Name, "tag", spec.Tag, "target_node", spec.Nodes[0].Name, "replicas", spec.Nodes[0].Replicas)
		configData, err = yaml.Marshal(spec)
		if err != nil {
			slog.Error("Deploy Error: failed to marshal generated ServiceSpec", "error", err)
			os.Exit(1)
		}
	}

	appName := spec.Name
	tag := spec.Tag

	ctxAll, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()
	connectCtx, connectCancel := context.WithTimeout(ctxAll, *timeout)
	nc, js, err := connectNATS(connectCtx, *natsURL, "narun-cli-deploy")
	connectCancel()
	if err != nil {
		slog.Error("Deploy Error: NATS connection failed", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	objStore, err := js.ObjectStore(ctxAll, noderunner.AppBinariesOSBucket)
	if err != nil {
		objStore, err = js.CreateObjectStore(ctxAll, jetstream.ObjectStoreConfig{Bucket: noderunner.AppBinariesOSBucket, Description: "Narun binaries"})
		if err != nil {
			slog.Error("Deploy Error: accessing/creating Object Store", "bucket", noderunner.AppBinariesOSBucket, "error", err)
			os.Exit(1)
		}
	}

	uploadErrors := false
	for _, binaryPath := range binaryFiles {
		slog.Info("--- Processing binary ---", "path", binaryPath)
		if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
			slog.Error("Error: Binary file not found. Skipping.", "path", binaryPath)
			uploadErrors = true
			continue
		}

		operSys, goarch, err := detectBinaryPlatform(binaryPath)
		if err != nil {
			slog.Error("Error: Could not detect platform. Skipping.", "path", binaryPath, "error", err)
			uploadErrors = true
			continue
		}
		slog.Info("Detected Platform", "os", operSys, "arch", goarch)

		objectName := fmt.Sprintf("%s-%s-%s", tag, operSys, goarch)
		slog.Info("--> NATS Object Store Name", "name", objectName)

		binaryBaseName := filepath.Base(binaryPath)
		slog.Info("Uploading binary to Object Store...", "original_name", binaryBaseName, "object_name", objectName)

		fileHandle, err := os.Open(binaryPath)
		if err != nil {
			slog.Error("Error opening binary. Skipping.", "path", binaryPath, "error", err)
			uploadErrors = true
			continue
		}

		putCtx, putCancel := context.WithTimeout(ctxAll, *timeout*2)
		meta := jetstream.ObjectMeta{
			Name:        objectName,
			Description: fmt.Sprintf("Binary for %s (%s/%s) tag %s", appName, operSys, goarch, tag),
			Metadata: map[string]string{
				"narun-goos":          operSys,
				"narun-goarch":        goarch,
				"narun-version-tag":   tag,
				"narun-original-file": binaryBaseName,
			},
		}
		objInfo, err := objStore.Put(putCtx, meta, fileHandle)
		putCancel()
		fileHandle.Close()
		if err != nil {
			slog.Error("Error uploading binary. Skipping.", "original_name", binaryBaseName, "object_name", objectName, "error", err)
			uploadErrors = true
			continue
		}
		slog.Info("Binary uploaded", "name", objInfo.Name, "size", objInfo.Size, "digest", objInfo.Digest)
		slog.Info(" --> Metadata", "goos", objInfo.Metadata["narun-goos"], "goarch", objInfo.Metadata["narun-goarch"], "tag", objInfo.Metadata["narun-version-tag"])
		slog.Info("--- Finished processing binary ---", "path", binaryPath)
	}

	if uploadErrors {
		slog.Error("Deployment failed: One or more binary uploads encountered errors.")
		os.Exit(1)
	}

	kvStore, err := js.KeyValue(ctxAll, noderunner.AppConfigKVBucket)
	if err != nil {
		kvStore, err = js.CreateKeyValue(ctxAll, jetstream.KeyValueConfig{Bucket: noderunner.AppConfigKVBucket, Description: "Narun configs"})
		if err != nil {
			slog.Error("Deploy Error: accessing/creating Key-Value Store", "bucket", noderunner.AppConfigKVBucket, "error", err)
			os.Exit(1)
		}
	}
	slog.Info("Updating configuration in KV store...", "key", appName)
	putCtx, putCancel := context.WithTimeout(ctxAll, 10*time.Second)
	revision, err := kvStore.Put(putCtx, appName, configData)
	putCancel()
	if err != nil {
		slog.Error("Deploy Error: updating configuration key", "key", appName, "error", err)
		os.Exit(1)
	}
	slog.Info("Configuration updated", "key", appName, "revision", revision)
	slog.Info("Deployment of application completed successfully.", "app_name", appName, "tag", tag)
}

func handleLogsCmd(args []string) {
	logsFlags := flag.NewFlagSet("logs", flag.ExitOnError)
	natsURL := logsFlags.String("nats", DefaultNatsURL, "NATS server URL")
	appName := logsFlags.String("app", "", "Filter logs by application name")
	nodeID := logsFlags.String("node", "", "Filter logs by node ID")
	instanceID := logsFlags.String("instance", "", "Filter logs by specific instance ID (requires -app)")
	follow := logsFlags.Bool("f", false, "Follow the log stream continuously")
	logsFlags.BoolVar(follow, "follow", false, "Follow the log stream continuously") // Alias
	raw := logsFlags.Bool("raw", false, "Output raw JSON log messages")
	timeout := logsFlags.Duration("timeout", 1*time.Minute, "Timeout for NATS connection/initial setup")

	logsFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s logs [options]

Streams logs from running application instances via NATS.

Options:
`, os.Args[0])
		logsFlags.PrintDefaults()
	}

	if err := logsFlags.Parse(args); err != nil {
		return
	}
	if *instanceID != "" && *appName == "" {
		slog.Error("Logs Error: -instance flag requires the -app flag to be set.")
		os.Exit(1)
	}

	subjectParts := []string{noderunner.LogSubjectPrefix}
	if *appName != "" {
		subjectParts = append(subjectParts, *appName)
	} else {
		subjectParts = append(subjectParts, "*")
	}
	if *nodeID != "" {
		subjectParts = append(subjectParts, *nodeID)
	} else {
		subjectParts = append(subjectParts, "*")
	}
	natsSubject := strings.Join(subjectParts, ".")

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	nc, _, err := connectNATS(ctx, *natsURL, "narun-cli-logs")
	if err != nil {
		slog.Error("Logs Error: NATS connection failed", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	slog.Info("Subscribing to NATS logs", "subject", natsSubject)
	msgChan := make(chan *nats.Msg, 64)
	sub, err := nc.ChanSubscribe(natsSubject, msgChan)
	if err != nil {
		slog.Error("Logs Error: Failed to subscribe", "subject", natsSubject, "error", err)
		os.Exit(1)
	}
	defer func() {
		slog.Info("Unsubscribing...")
		if err := sub.Unsubscribe(); err != nil {
			slog.Error("Logs Error: Failed to unsubscribe", "error", err)
		}
		close(msgChan)
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	slog.Info("Waiting for logs... Press Ctrl+C to exit.")

	for {
		select {
		case msg := <-msgChan:
			if msg == nil {
				slog.Info("Subscription channel closed.")
				return
			}
			if *raw {
				fmt.Println(string(msg.Data))
				continue
			}
			var entry logEntry
			if err := json.Unmarshal(msg.Data, &entry); err != nil {
				slog.Warn("WARN: Failed to parse log JSON", "error", err, "data", string(msg.Data))
				continue
			}
			if *instanceID != "" && entry.InstanceID != *instanceID {
				continue
			}
			fmt.Printf("[%s] [%s] [%s] %s\n", entry.Timestamp.Format(time.RFC3339Nano), entry.InstanceID, entry.Stream, entry.Message)
		case <-sigChan:
			slog.Info("Received interrupt signal. Exiting...")
			return
		}
	}
}

// List Images Command Logic
func handleListImagesCmd(args []string) {
	listImagesFlags := flag.NewFlagSet("list-images", flag.ExitOnError)
	natsURL := listImagesFlags.String("nats", DefaultNatsURL, "NATS server URL")
	timeout := listImagesFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")

	listImagesFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s list-images [options]

Lists application binaries stored in the NATS Object Store '%s'.

Options:
`, os.Args[0], noderunner.AppBinariesOSBucket)
		listImagesFlags.PrintDefaults()
	}

	if err := listImagesFlags.Parse(args); err != nil {
		return
	}

	slog.Info("Listing images from NATS Object Store", "bucket", noderunner.AppBinariesOSBucket)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	nc, js, err := connectNATS(ctx, *natsURL, "narun-cli-list-images")
	if err != nil {
		slog.Error("Error connecting to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	objStore, err := js.ObjectStore(ctx, noderunner.AppBinariesOSBucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, nats.ErrBucketNotFound) {
			slog.Info("Object Store not found. No images to list.", "bucket", noderunner.AppBinariesOSBucket)
			return
		}
		slog.Error("Error accessing Object Store", "bucket", noderunner.AppBinariesOSBucket, "error", err)
		os.Exit(1)
	}

	objects, err := objStore.List(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			slog.Info("No images found in the object store.")
			return
		}
		slog.Error("Error listing objects in store", "bucket", noderunner.AppBinariesOSBucket, "error", err)
		os.Exit(1)
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "OBJECT NAME\tSIZE\tMODIFIED\tTAG\tOS\tARCH\tDIGEST")
	fmt.Fprintln(tw, "-----------\t----\t--------\t---\t--\t----\t------")

	count := 0
	for _, objInfo := range objects {
		if objInfo == nil {
			continue
		}
		count++
		tag := objInfo.Metadata["narun-version-tag"]
		goos := objInfo.Metadata["narun-goos"]
		goarch := objInfo.Metadata["narun-goarch"]
		if tag == "" {
			tag = "-"
		}
		if goos == "" {
			goos = "-"
		}
		if goarch == "" {
			goarch = "-"
		}
		modTime := objInfo.ModTime.Local().Format(time.RFC3339)
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
			objInfo.Name, objInfo.Size, modTime, tag, goos, goarch, objInfo.Digest)
	}
	tw.Flush()
	slog.Info(fmt.Sprintf("Found %d image(s).", count))
}

// --- Renamed and Updated: handleServiceListCmd (formerly handleListAppsCmd) ---
func handleServiceListCmd(args []string) {
	listServicesFlags := flag.NewFlagSet("service list", flag.ExitOnError)
	natsURL := listServicesFlags.String("nats", DefaultNatsURL, "NATS server URL")
	timeout := listServicesFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")

	listServicesFlags.Usage = printServiceListUsage // Use specific usage

	if err := listServicesFlags.Parse(args); err != nil {
		return
	}

	slog.Debug("Listing deployed services and node status...")

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	nc, js, err := connectNATS(ctx, *natsURL, "narun-cli-service-list")
	if err != nil {
		slog.Error("Error connecting to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	nodeStates := make(map[string]noderunner.NodeState)
	kvNodeStates, err := js.KeyValue(ctx, noderunner.NodeStateKVBucket)
	if err != nil {
		slog.Warn("Could not access Node State KV. Node status will be unavailable.", "bucket", noderunner.NodeStateKVBucket, "error", err)
	} else {
		nodeKeysWatcher, err := kvNodeStates.ListKeys(ctx)
		if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
			slog.Warn("Failed to list node keys", "bucket", noderunner.NodeStateKVBucket, "error", err)
		} else if err == nil {
			nodeKeysChan := nodeKeysWatcher.Keys()
		nodeLoop:
			for {
				select {
				case <-ctx.Done():
					slog.Warn("Timed out listing node keys", "error", ctx.Err())
					break nodeLoop
				case nodeKey, ok := <-nodeKeysChan:
					if !ok {
						break nodeLoop
					}
					if nodeKey == "" {
						continue
					}
					entry, err := kvNodeStates.Get(ctx, nodeKey)
					if err != nil {
						slog.Warn("Failed to get node state for key", "key", nodeKey, "error", err)
						continue
					}
					var state noderunner.NodeState
					if err := json.Unmarshal(entry.Value(), &state); err != nil {
						slog.Warn("Failed to unmarshal node state for key", "key", nodeKey, "error", err)
						continue
					}
					nodeStates[nodeKey] = state
				}
			}
			nodeKeysWatcher.Stop()
		}
	}
	slog.Debug(fmt.Sprintf("Found %d active node(s).", len(nodeStates)))

	appConfigs := make(map[string]noderunner.ServiceSpec)
	kvAppConfigs, err := js.KeyValue(ctx, noderunner.AppConfigKVBucket)
	if err != nil {
		slog.Warn("Could not access App Config KV. Cannot list app details.", "bucket", noderunner.AppConfigKVBucket, "error", err)
	} else {
		appKeysWatcher, err := kvAppConfigs.ListKeys(ctx)
		if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
			slog.Warn("Failed to list app keys", "bucket", noderunner.AppConfigKVBucket, "error", err)
		} else if err == nil {
			appKeysChan := appKeysWatcher.Keys()
		appLoop:
			for {
				select {
				case <-ctx.Done():
					slog.Warn("Timed out listing app keys", "error", ctx.Err())
					break appLoop
				case appKey, ok := <-appKeysChan:
					if !ok {
						break appLoop
					}
					if appKey == "" {
						continue
					}
					entry, err := kvAppConfigs.Get(ctx, appKey)
					if err != nil {
						slog.Warn("Failed to get app config for key", "key", appKey, "error", err)
						continue
					}
					var spec noderunner.ServiceSpec
					if err := yaml.Unmarshal(entry.Value(), &spec); err != nil {
						slog.Warn("Failed to unmarshal app config for key", "key", appKey, "error", err)
						continue
					}
					appConfigs[appKey] = spec
				}
			}
			appKeysWatcher.Stop()
		}
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 1, ' ', 0) // Adjusted padding
	fmt.Fprintln(tw, "SERVICE\tTAG\tNODE\tNODE STATUS\tNODE PLATFORM\tDESIRED\tRUNNING\tINSTANCE IDS")
	fmt.Fprintln(tw, "-------\t---\t----\t-----------\t-------------\t-------\t-------\t------------")

	appNames := make([]string, 0, len(appConfigs))
	for name := range appConfigs {
		appNames = append(appNames, name)
	}
	sort.Strings(appNames)

	if len(appNames) == 0 && len(nodeStates) == 0 {
		fmt.Fprintln(tw, "(No services or nodes found)")
	} else {
		for _, appName := range appNames {
			spec := appConfigs[appName]
			nodesTargetedBySpec := make(map[string]noderunner.NodeSelectorSpec)
			for _, ns := range spec.Nodes {
				nodesTargetedBySpec[ns.Name] = ns
			}

			// Track nodes that have run this app to avoid listing them multiple times if app spec.Nodes is empty
			nodesProcessedForApp := make(map[string]bool)

			// Iterate through nodes defined in the spec first
			for _, nodeSpec := range spec.Nodes {
				nodeID := nodeSpec.Name
				desiredReplicas := nodeSpec.Replicas
				nodeInfo, nodeFound := nodeStates[nodeID]
				nodeStatus := "offline/unknown"
				nodePlatform := "-/-"
				runningInstanceIDs := make([]string, 0)
				runningCount := 0

				if nodeFound {
					nodeStatus = nodeInfo.Status
					nodePlatform = fmt.Sprintf("%s/%s", nodeInfo.OS, nodeInfo.Arch)
					for _, instanceID := range nodeInfo.ManagedInstances {
						if noderunner.InstanceIDToAppName(instanceID) == appName {
							runningInstanceIDs = append(runningInstanceIDs, instanceID)
						}
					}
					runningCount = len(runningInstanceIDs)
				}
				instanceIDsStr := "[]"
				if len(runningInstanceIDs) > 0 {
					instanceIDsStr = strings.Join(runningInstanceIDs, ", ")
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\n",
					appName, spec.Tag, nodeID, nodeStatus, nodePlatform, desiredReplicas, runningCount, instanceIDsStr)
				nodesProcessedForApp[nodeID] = true
			}

			// Check for instances running on nodes NOT in the spec (orphaned state or spec updated)
			for nodeID, nodeInfo := range nodeStates {
				if nodesProcessedForApp[nodeID] { // Already listed above
					continue
				}
				runningInstanceIDs := make([]string, 0)
				for _, instanceID := range nodeInfo.ManagedInstances {
					if noderunner.InstanceIDToAppName(instanceID) == appName {
						runningInstanceIDs = append(runningInstanceIDs, instanceID)
					}
				}
				if len(runningInstanceIDs) > 0 { // App is running on this node but node not in spec
					nodeStatus := nodeInfo.Status
					nodePlatform := fmt.Sprintf("%s/%s", nodeInfo.OS, nodeInfo.Arch)
					instanceIDsStr := strings.Join(runningInstanceIDs, ", ")
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
						appName, spec.Tag, nodeID, nodeStatus, nodePlatform, "0 (not in spec)", len(runningInstanceIDs), instanceIDsStr)
				}
			}
			if len(spec.Nodes) == 0 && !isAppRunningAnywhere(appName, nodeStates) {
				// App is configured but targets no nodes and is not running anywhere
				fmt.Fprintf(tw, "%s\t%s\t-\t-\t-\t0\t0\t[]\n", appName, spec.Tag)
			}
		}
	}
	tw.Flush()
}

func isAppRunningAnywhere(appName string, nodeStates map[string]noderunner.NodeState) bool {
	for _, state := range nodeStates {
		for _, instanceID := range state.ManagedInstances {
			if noderunner.InstanceIDToAppName(instanceID) == appName {
				return true
			}
		}
	}
	return false
}

// --- New: handleServiceInfoCmd ---
func handleServiceInfoCmd(args []string) {
	infoFlags := flag.NewFlagSet("service info", flag.ExitOnError)
	natsURL := infoFlags.String("nats", DefaultNatsURL, "NATS server URL")
	timeout := infoFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")

	infoFlags.Usage = printServiceInfoUsage // Use specific usage

	if err := infoFlags.Parse(args); err != nil {
		return
	}
	if infoFlags.NArg() != 1 {
		slog.Error("Error: 'service info' requires exactly one argument: <service_name>")
		infoFlags.Usage()
		os.Exit(1)
	}
	appName := infoFlags.Arg(0)

	slog.Info("Fetching service information", "service_name", appName)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	nc, js, err := connectNATS(ctx, *natsURL, "narun-cli-service-info")
	if err != nil {
		slog.Error("Error connecting to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	// 1. Fetch ServiceSpec
	kvAppConfigs, err := js.KeyValue(ctx, noderunner.AppConfigKVBucket)
	if err != nil {
		slog.Error("Could not access App Config KV", "bucket", noderunner.AppConfigKVBucket, "error", err)
		os.Exit(1)
	}
	entry, err := kvAppConfigs.Get(ctx, appName)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			slog.Error("Service configuration not found", "service_name", appName)
		} else {
			slog.Error("Failed to get service configuration", "service_name", appName, "error", err)
		}
		os.Exit(1)
	}
	var spec noderunner.ServiceSpec
	if err := yaml.Unmarshal(entry.Value(), &spec); err != nil {
		slog.Error("Failed to unmarshal service configuration", "service_name", appName, "error", err)
		os.Exit(1)
	}

	fmt.Printf("Service Information for: %s (Tag: %s)\n", spec.Name, spec.Tag)
	fmt.Println("--- Configuration (ServiceSpec) ---")
	fmt.Println(string(entry.Value())) // Print raw YAML

	// 2. Fetch NodeStates
	nodeStates := make(map[string]noderunner.NodeState)
	kvNodeStates, err := js.KeyValue(ctx, noderunner.NodeStateKVBucket)
	if err != nil {
		slog.Warn("Could not access Node State KV. Node status will be limited.", "bucket", noderunner.NodeStateKVBucket, "error", err)
	} else {
		nodeKeysWatcher, err := kvNodeStates.ListKeys(ctx)
		if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
			slog.Warn("Failed to list node keys", "bucket", noderunner.NodeStateKVBucket, "error", err)
		} else if err == nil {
			nodeKeysChan := nodeKeysWatcher.Keys()
		nodeLoop:
			for {
				select {
				case <-ctx.Done():
					slog.Warn("Timed out listing node keys", "error", ctx.Err())
					break nodeLoop
				case nodeKey, ok := <-nodeKeysChan:
					if !ok {
						break nodeLoop
					}
					if nodeKey == "" {
						continue
					}
					nsEntry, err := kvNodeStates.Get(ctx, nodeKey)
					if err != nil {
						slog.Warn("Failed to get node state", "key", nodeKey, "error", err)
						continue
					}
					var state noderunner.NodeState
					if err := json.Unmarshal(nsEntry.Value(), &state); err != nil {
						slog.Warn("Failed to unmarshal node state", "key", nodeKey, "error", err)
						continue
					}
					nodeStates[nodeKey] = state
				}
			}
			nodeKeysWatcher.Stop()
		}
	}

	// 3. Display Node Status & Instances
	fmt.Println("\n--- Node Status & Instances ---")
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 1, ' ', 0)
	fmt.Fprintln(tw, "NODE ID\tSTATUS\tPLATFORM\tDESIRED\tRUNNING\tINSTANCE IDS")
	fmt.Fprintln(tw, "-------\t------\t--------\t-------\t-------\t------------")

	nodesTargetedBySpec := make(map[string]bool)
	if len(spec.Nodes) > 0 {
		for _, nodeSpec := range spec.Nodes {
			nodesTargetedBySpec[nodeSpec.Name] = true
			nodeID := nodeSpec.Name
			desiredReplicas := nodeSpec.Replicas
			nodeInfo, nodeFound := nodeStates[nodeID]
			nodeStatus := "offline/unknown"
			nodePlatform := "-/-"
			runningInstanceIDs := make([]string, 0)
			runningCount := 0

			if nodeFound {
				nodeStatus = nodeInfo.Status
				nodePlatform = fmt.Sprintf("%s/%s", nodeInfo.OS, nodeInfo.Arch)
				for _, instanceID := range nodeInfo.ManagedInstances {
					if noderunner.InstanceIDToAppName(instanceID) == appName {
						runningInstanceIDs = append(runningInstanceIDs, instanceID)
					}
				}
				runningCount = len(runningInstanceIDs)
			}
			instanceIDsStr := "[]"
			if len(runningInstanceIDs) > 0 {
				instanceIDsStr = strings.Join(runningInstanceIDs, ", ")
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%s\n",
				nodeID, nodeStatus, nodePlatform, desiredReplicas, runningCount, instanceIDsStr)
		}
	} else {
		fmt.Fprintln(tw, "(No nodes explicitly targeted in ServiceSpec)")
	}
	tw.Flush()

	// 4. Orphaned Instances Check
	hasOrphans := false
	orphanedTw := tabwriter.NewWriter(os.Stdout, 0, 8, 1, ' ', 0) // Separate tabwriter for orphans
	fmt.Fprintln(orphanedTw, "\n--- Orphaned Instances (Running on nodes not in current spec) ---")
	fmt.Fprintln(orphanedTw, "NODE ID\tINSTANCE ID")
	fmt.Fprintln(orphanedTw, "-------\t-----------")

	for nodeID, nodeInfo := range nodeStates {
		if nodesTargetedBySpec[nodeID] { // Skip nodes already covered by the spec
			continue
		}
		for _, instanceID := range nodeInfo.ManagedInstances {
			if noderunner.InstanceIDToAppName(instanceID) == appName {
				fmt.Fprintf(orphanedTw, "%s\t%s\n", nodeID, instanceID)
				hasOrphans = true
			}
		}
	}
	if hasOrphans {
		orphanedTw.Flush()
	} else {
		fmt.Println("(No orphaned instances found)")
	}
	fmt.Println()
}

// --- Renamed: handleServiceDeleteCmd (formerly handleAppDeleteCmd) ---
func handleServiceDeleteCmd(args []string) {
	deleteServiceFlags := flag.NewFlagSet("service delete", flag.ExitOnError)
	natsURL := deleteServiceFlags.String("nats", DefaultNatsURL, "NATS server URL")
	timeout := deleteServiceFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	skipConfirm := deleteServiceFlags.Bool("y", false, "Skip confirmation prompt")

	deleteServiceFlags.Usage = printServiceDeleteUsage // Use specific usage

	if err := deleteServiceFlags.Parse(args); err != nil {
		return
	}

	if deleteServiceFlags.NArg() != 1 {
		slog.Error("Delete Service Error: Exactly one service name argument is required.")
		deleteServiceFlags.Usage()
		os.Exit(1)
	}
	appName := deleteServiceFlags.Arg(0)

	if !*skipConfirm {
		fmt.Printf("WARNING: This will permanently delete the configuration for service '%s'.\n", appName)
		fmt.Printf("Node runners watching this configuration will stop all instances of this service.\n")
		fmt.Print("Are you sure you want to proceed? (yes/no): ")
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(response) != "yes" {
			slog.Info("Delete operation cancelled.")
			os.Exit(0)
		}
	}

	slog.Debug("Deleting service configuration...", "service_name", appName)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	nc, js, err := connectNATS(ctx, *natsURL, "narun-cli-service-delete")
	if err != nil {
		slog.Error("Delete Service Error: NATS connection failed", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	kvStore, err := js.KeyValue(ctx, noderunner.AppConfigKVBucket)
	if err != nil {
		slog.Error("Delete Service Error: accessing Key-Value Store", "bucket", noderunner.AppConfigKVBucket, "error", err)
		os.Exit(1)
	}

	_, getErr := kvStore.Get(ctx, appName)
	if getErr != nil {
		if errors.Is(getErr, jetstream.ErrKeyNotFound) {
			fmt.Printf("Service configuration '%s' not found. Nothing to delete.\n", appName)
			os.Exit(0)
		}
		slog.Error("Delete Service Error: failed to check if key exists", "key", appName, "error", getErr)
		os.Exit(1)
	}

	slog.Debug("Deleting key from KV store...", "key", appName, "bucket", noderunner.AppConfigKVBucket)
	err = kvStore.Delete(ctx, appName) // Delete vs Purge? Delete keeps history, Purge removes.
	if err != nil {                    // Let's use Delete for now, allows rollback if needed.
		slog.Error("Delete Service Error: failed to delete key", "key", appName, "error", err)
		os.Exit(1)
	}
	fmt.Printf("Successfully deleted service configuration '%s'.\n", appName)
	slog.Debug("Node runners will now stop instances for this service.")
}

// Files Command Handling
func handleFilesCmd(args []string) {
	if len(args) < 1 {
		printFilesUsage()
		os.Exit(1)
	}

	subcommand := args[0]
	subcommandArgs := args[1:]

	switch subcommand {
	case "add":
		handleFilesAddCmd(subcommandArgs)
	case "list":
		handleFilesListCmd(subcommandArgs)
	case "delete":
		handleFilesDeleteCmd(subcommandArgs)
	case "help", "-h", "--help":
		printFilesUsage()
	default:
		fmt.Fprintf(os.Stderr, "Error: Unknown files subcommand '%s'\n\n", subcommand)
		printFilesUsage()
		os.Exit(1)
	}
}

func printFilesUsage() {
	fmt.Fprintf(os.Stderr, `Usage: %s files <subcommand> [options] [arguments...]

Manage shared files stored in the NATS Object Store bucket '%s'.
These files can be mounted into application instances via the 'mounts' section
in the ServiceSpec configuration.

Subcommands:
  add <name> <local_path>  Add or update a file with the given logical name.
  list                     List the names and details of stored files.
  delete <name>            Delete a file by its logical name.
  help                     Show this help message.

Common Options (apply to all subcommands):
  -nats <url>     NATS server URL (default: %s)
  -timeout <dur>  Timeout for NATS operations (default: %s)

Options for 'add':
  (No specific options other than common ones)

Options for 'list':
  (No specific options)

Options for 'delete':
  -y                  Skip confirmation prompt.

`, os.Args[0], noderunner.FileOSBucket, DefaultNatsURL, DefaultTimeout)
}

func handleFilesAddCmd(args []string) {
	addFlags := flag.NewFlagSet("files add", flag.ExitOnError)
	natsURL := addFlags.String("nats", DefaultNatsURL, "NATS server URL")
	timeout := addFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")

	addFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s files add [options] <name> <local_path>

Adds or updates a file in the NATS Object Store '%s'.

Arguments:
  <name>         The logical name/key for the file in the object store (required).
                 This name is used in the ServiceSpec 'mounts.source.objectStore'.
  <local_path>   The path to the local file to upload (required).

Options:
`, os.Args[0], noderunner.FileOSBucket)
		addFlags.PrintDefaults()
	}

	if err := addFlags.Parse(args); err != nil {
		return
	}

	if addFlags.NArg() != 2 {
		slog.Error("Error: 'files add' requires exactly two arguments: <name> and <local_path>")
		addFlags.Usage()
		os.Exit(1)
	}
	fileName := addFlags.Arg(0)
	localPath := addFlags.Arg(1)

	if strings.TrimSpace(fileName) == "" {
		slog.Error("Error: File name cannot be empty.")
		os.Exit(1)
	}
	if strings.ContainsAny(fileName, "/") {
		slog.Error("Error: File name cannot contain '/' characters.")
		os.Exit(1)
	}

	fileInfo, err := os.Stat(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Error("Error: Local file path does not exist.", "path", localPath)
		} else {
			slog.Error("Error accessing local file path.", "path", localPath, "error", err)
		}
		os.Exit(1)
	}
	if fileInfo.IsDir() {
		slog.Error("Error: Local path is a directory, not a file.", "path", localPath)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	nc, js, err := connectNATS(ctx, *natsURL, "narun-cli-files-add")
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	fileStore, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket: noderunner.FileOSBucket, Description: "Shared files for Narun applications",
	})
	if err != nil {
		slog.Error("Failed to access/create files object store", "bucket", noderunner.FileOSBucket, "error", err)
		os.Exit(1)
	}

	localFile, err := os.Open(localPath)
	if err != nil {
		slog.Error("Failed to open local file for reading", "path", localPath, "error", err)
		os.Exit(1)
	}
	defer localFile.Close()

	meta := jetstream.ObjectMeta{
		Name:        fileName,
		Description: fmt.Sprintf("Shared file '%s' uploaded from '%s'", fileName, filepath.Base(localPath)),
		Metadata: map[string]string{
			"original-filename": filepath.Base(localPath),
			"upload-timestamp":  time.Now().UTC().Format(time.RFC3339),
		},
	}

	hasher := sha256.New()
	if _, err = io.Copy(hasher, localFile); err != nil {
		slog.Error("Failed to hash local file", "path", localPath, "error", err)
		os.Exit(1)
	}
	if _, err = localFile.Seek(0, io.SeekStart); err != nil {
		slog.Error("Failed to seek local file after hashing", "path", localPath, "error", err)
		os.Exit(1)
	}

	slog.Info("Uploading file to object store...", "name", fileName, "local_path", localPath, "bucket", noderunner.FileOSBucket)

	objInfo, err := fileStore.Put(ctx, meta, localFile)
	if err != nil {
		slog.Error("Failed to upload file to object store", "name", fileName, "error", err)
		os.Exit(1)
	}
	slog.Info("File uploaded successfully", "name", objInfo.Name, "size", objInfo.Size, "digest", objInfo.Digest)
}

func handleFilesListCmd(args []string) {
	listFlags := flag.NewFlagSet("files list", flag.ExitOnError)
	natsURL := listFlags.String("nats", DefaultNatsURL, "NATS server URL")
	timeout := listFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")

	listFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s files list [options]

Lists files stored in the NATS Object Store '%s'.

Options:
`, os.Args[0], noderunner.FileOSBucket)
		listFlags.PrintDefaults()
	}
	if err := listFlags.Parse(args); err != nil {
		return
	}
	if listFlags.NArg() != 0 {
		slog.Error("Error: 'files list' takes no arguments.")
		listFlags.Usage()
		os.Exit(1)
	}

	slog.Info("Listing files from object store...", "bucket", noderunner.FileOSBucket)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	nc, js, err := connectNATS(ctx, *natsURL, "narun-cli-files-list")
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	fileStore, err := js.ObjectStore(ctx, noderunner.FileOSBucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, nats.ErrBucketNotFound) {
			slog.Info("Files object store not found. No files to list.", "bucket", noderunner.FileOSBucket)
			return
		}
		slog.Error("Failed to access files object store", "bucket", noderunner.FileOSBucket, "error", err)
		os.Exit(1)
	}

	objects, err := fileStore.List(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			fmt.Println("No files found in the object store.")
			return
		}
		slog.Error("Failed to list objects in file store", "error", err)
		os.Exit(1)
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tSIZE\tMODIFIED\tORIGINAL FILENAME\tDIGEST")
	fmt.Fprintln(tw, "----\t----\t--------\t-----------------\t------")

	count := 0
	for _, objInfo := range objects {
		if objInfo == nil {
			continue
		}
		count++
		modTime := objInfo.ModTime.Local().Format(time.RFC3339)
		origFilename := objInfo.Metadata["original-filename"]
		if origFilename == "" {
			origFilename = "-"
		}
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\n",
			objInfo.Name, objInfo.Size, modTime, origFilename, objInfo.Digest)
	}
	tw.Flush()
	slog.Info(fmt.Sprintf("Found %d file(s).", count))
}

func handleFilesDeleteCmd(args []string) {
	deleteFlags := flag.NewFlagSet("files delete", flag.ExitOnError)
	natsURL := deleteFlags.String("nats", DefaultNatsURL, "NATS server URL")
	timeout := deleteFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	skipConfirm := deleteFlags.Bool("y", false, "Skip confirmation prompt")

	deleteFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s files delete [options] <name>

Deletes a file from the NATS Object Store '%s'.

Arguments:
  <name>    The logical name/key of the file to delete (required).

Options:
`, os.Args[0], noderunner.FileOSBucket)
		deleteFlags.PrintDefaults()
	}
	if err := deleteFlags.Parse(args); err != nil {
		return
	}
	if deleteFlags.NArg() != 1 {
		slog.Error("Error: 'files delete' requires exactly one argument: <name>")
		deleteFlags.Usage()
		os.Exit(1)
	}
	fileName := deleteFlags.Arg(0)

	if !*skipConfirm {
		fmt.Printf("WARNING: This will permanently delete the file '%s' from the object store.\n", fileName)
		fmt.Print("Are you sure you want to proceed? (yes/no): ")
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(response) != "yes" {
			slog.Info("Delete operation cancelled.")
			os.Exit(0)
		}
	}

	slog.Info("Deleting file from object store...", "name", fileName, "bucket", noderunner.FileOSBucket)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	nc, js, err := connectNATS(ctx, *natsURL, "narun-cli-files-delete")
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	fileStore, err := js.ObjectStore(ctx, noderunner.FileOSBucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, nats.ErrBucketNotFound) {
			slog.Error("Files object store not found. Cannot delete.", "bucket", noderunner.FileOSBucket)
			os.Exit(1)
		}
		slog.Error("Failed to access files object store", "bucket", noderunner.FileOSBucket, "error", err)
		os.Exit(1)
	}

	err = fileStore.Delete(ctx, fileName)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			slog.Error(fmt.Sprintf("File '%s' not found. Nothing to delete.", fileName))
			os.Exit(0)
		}
		slog.Error("Failed to delete file from object store", "name", fileName, "error", err)
		os.Exit(1)
	}
	slog.Info("File deleted successfully", "name", fileName)
}

func connectNATS(ctx context.Context, url string, clientNamePrefix string) (*nats.Conn, jetstream.JetStream, error) {
	if clientNamePrefix == "" {
		clientNamePrefix = "narun-cli-unknown"
	}
	// Make client name more unique for multiple CLI invocations
	clientName := fmt.Sprintf("%s-%d", clientNamePrefix, time.Now().UnixNano()%10000)

	slog.Debug("Connecting to NATS server...", "url", url, "client_name", clientName)
	nc, err := nats.Connect(url,
		nats.Name(clientName),
		nats.Timeout(10*time.Second), // Increased CLI timeout for connect
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Printf("WARN: NATS disconnected: %v", err) // Use standard log for CLI disconnects
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) { slog.Debug("INFO: NATS reconnected", "url", nc.ConnectedUrl()) }),
		nats.ClosedHandler(func(nc *nats.Conn) { slog.Debug("INFO: NATS connection closed.") }),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to NATS: %w", err)
	}
	slog.Debug("Connected to NATS successfully.")

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("creating JetStream context: %w", err)
	}
	slog.Debug("JetStream context created.")
	return nc, js, nil
}
