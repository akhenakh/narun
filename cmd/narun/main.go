// narun/cmd/narun/main.go

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort" // Added for sorting output
	"strings"
	"syscall"
	"text/tabwriter" // Added for tabular output
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

	// Logger setup
	logger := slog.New(logHandler).With("service", "hello-consumer") // Keep this logical name for logging
	slog.SetDefault(logger)

	command := os.Args[1]
	args := os.Args[2:]

	// Special case for 'deploy -h' or 'deploy --help'
	if command == "deploy" && len(args) > 0 && (args[0] == "-h" || args[0] == "--help") {
		// Create a dummy flagset just to call its Usage()
		deployFlags := flag.NewFlagSet("deploy", flag.ExitOnError)
		// Define the flags again so Usage() knows about them
		deployFlags.String("nats", DefaultNatsURL, "NATS server URL")
		deployFlags.String("config", "", "Path to the application ServiceSpec YAML configuration file (optional). If provided, overrides -name, -tag, -node, -replicas.")
		deployFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
		deployFlags.String("name", "", "Application name (required if -config is not used)")
		deployFlags.String("tag", "", "Binary tag (required if -config is not used)")
		deployFlags.String("node", "local", "Target node name (used if -config is not used)")
		deployFlags.Int("replicas", 1, "Number of replicas on the target node (used if -config is not used)")
		deployFlags.Usage = func() { // Define the specific Usage text again
			fmt.Fprintf(os.Stderr, `Usage: %s deploy [options] <binary_path> [<binary_path>...]

Uploads application binaries and configuration.

Arguments:
  <binary_path>   Path to the application binary file (at least one required).

Options:
`, os.Args[0])
			deployFlags.PrintDefaults()
			fmt.Fprintln(os.Stderr, "\nNote: If -config is provided, the -name, -tag, -node, and -replicas flags are ignored.")
		}
		deployFlags.Usage() // Call the specific usage
		os.Exit(0)          // Exit successfully after showing help
	}

	// Normal Command Handling
	switch command {
	case "deploy":
		handleDeployCmd(args)
	case "logs":
		// Add -h check for logs if desired
		handleLogsCmd(args)
	case "list-images":
		// Add -h check for list-images if desired
		handleListImagesCmd(args)
	case "list-apps":
		// Add -h check for list-apps if desired
		handleListAppsCmd(args)
	case "delete-app":
		handleAppDeleteCmd(args)
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
  deploy        Upload application binaries and configuration.
                Run '%s deploy -h' for specific deploy options.
  logs          Stream logs from node runners.
  list-images   List application binaries stored in NATS Object Store.
  list-apps     List deployed applications and their status on nodes.
  delete-app    Delete an application configuration from NATS KV.
  help          Show this help message.

Common Options (apply to multiple commands where relevant):
  -nats <url>     NATS server URL (default: %s)
  -timeout <dur>  Timeout for NATS operations (default: %s)

Options for deploy:
`, os.Args[0], os.Args[0], DefaultNatsURL, DefaultTimeout)
	// Print deploy flags specifically
	deployFlags := flag.NewFlagSet("deploy", flag.ExitOnError)
	deployFlags.String("nats", DefaultNatsURL, "NATS server URL")
	deployFlags.String("config", "", "Path to the application ServiceSpec YAML configuration file (required)")
	deployFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	deployFlags.PrintDefaults()

	fmt.Fprintln(os.Stderr, "\nOptions for logs:")
	logsFlags := flag.NewFlagSet("logs", flag.ExitOnError)
	logsFlags.String("nats", DefaultNatsURL, "NATS server URL")
	logsFlags.String("app", "", "Filter logs by application name")
	logsFlags.String("node", "", "Filter logs by node ID")
	logsFlags.String("instance", "", "Filter logs by specific instance ID (requires -app)")
	logsFlags.Bool("f", false, "Follow the log stream continuously (alias -follow)")
	logsFlags.Bool("follow", false, "Follow the log stream continuously")
	logsFlags.Bool("raw", false, "Output raw JSON log messages")
	logsFlags.Duration("timeout", 1*time.Minute, "Timeout for NATS connection/initial setup")
	logsFlags.PrintDefaults()

	fmt.Fprintln(os.Stderr, "\nOptions for list-images:")
	listImagesFlags := flag.NewFlagSet("list-images", flag.ExitOnError)
	listImagesFlags.String("nats", DefaultNatsURL, "NATS server URL")
	listImagesFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	// Add filtering options later if needed
	listImagesFlags.PrintDefaults()

	fmt.Fprintln(os.Stderr, "\nOptions for list-apps:")
	listAppsFlags := flag.NewFlagSet("list-apps", flag.ExitOnError)
	listAppsFlags.String("nats", DefaultNatsURL, "NATS server URL")
	listAppsFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	// Add filtering options later if needed
	listAppsFlags.PrintDefaults()

	fmt.Fprintln(os.Stderr, "\nOptions for delete:")
	deleteFlags := flag.NewFlagSet("delete", flag.ExitOnError)
	deleteFlags.String("nats", DefaultNatsURL, "NATS server URL")
	deleteFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	deleteFlags.Bool("y", false, "Skip confirmation prompt")
	deleteFlags.PrintDefaults()
}

// Deploy Command Logic
func handleDeployCmd(args []string) {
	deployFlags := flag.NewFlagSet("deploy", flag.ExitOnError)
	natsURL := deployFlags.String("nats", DefaultNatsURL, "NATS server URL")
	configFile := deployFlags.String("config", "", "Path to the application ServiceSpec YAML configuration file (optional). If provided, overrides -name, -tag, -node, -replicas.")
	timeout := deployFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	// Flags used when -config is NOT provided
	appNameFlag := deployFlags.String("name", "", "Application name (required if -config is not used)")
	tagFlag := deployFlags.String("tag", "", "Binary tag tag (required if -config is not used)")
	nodeFlag := deployFlags.String("node", "local", "Target node name (used if -config is not used)")
	replicasFlag := deployFlags.Int("replicas", 1, "Number of replicas on the target node (used if -config is not used)")

	// Usage function specific to deploy
	deployFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s deploy [options] <binary_path> [<binary_path>...]

Uploads application binaries and configuration.

Arguments:
  <binary_path>   Path to the application binary file (at least one required).

Options:
`, os.Args[0])
		deployFlags.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nNote: If -config is provided, the -name, -tag, -node, and -replicas flags are ignored.")
	}

	if err := deployFlags.Parse(args); err != nil {
		// Usage is already printed by ExitOnError
		return
	}
	binaryFiles := deployFlags.Args()
	if len(binaryFiles) == 0 {
		log.Println("Deploy Error: At least one binary file path must be provided.")
		deployFlags.Usage()
		os.Exit(1)
	}

	var spec noderunner.ServiceSpec
	var configData []byte
	var err error

	if *configFile != "" {
		// Mode 1: Using -config file
		log.Printf("Starting Deployment using Config File: Config='%s', Binaries=%v, NATS='%s'", *configFile, binaryFiles, *natsURL)
		if _, err = os.Stat(*configFile); os.IsNotExist(err) {
			log.Fatalf("Deploy Error: Config file not found at %s", *configFile)
		}
		configData, err = os.ReadFile(*configFile)
		if err != nil {
			log.Fatalf("Deploy Error: reading config file %s: %v", *configFile, err)
		}
		if err = yaml.Unmarshal(configData, &spec); err != nil {
			log.Fatalf("Deploy Error: parsing ServiceSpec YAML from %s: %v", *configFile, err)
		}
		// Validate mandatory fields from file
		if spec.Name == "" {
			log.Fatalf("Deploy Error: 'name' field is missing or empty in %s", *configFile)
		}
		if spec.Tag == "" {
			log.Fatalf("Deploy Error: 'binary_version_tag' field is missing or empty in %s", *configFile)
		}
		// Node validation (keep it simple)
		for i, nodeSpec := range spec.Nodes {
			if strings.TrimSpace(nodeSpec.Name) == "" {
				log.Fatalf("Deploy Error in %s: node selector %d: name empty", *configFile, i)
			}
			if nodeSpec.Replicas <= 0 {
				log.Fatalf("Deploy Error in %s: node selector %s: replicas must be positive", *configFile, nodeSpec.Name)
			}
			// Check for duplicate node names within the file's spec
			// (omitted for brevity, assume initial validation is sufficient here)
		}
		log.Printf("Deploying ServiceSpec from %s: AppName='%s', BinaryVersionTag='%s', TargetNodes=%+v",
			*configFile, spec.Name, spec.Tag, spec.Nodes)

	} else {
		// Mode 2: Using flags
		log.Printf("Starting Deployment using Flags: Binaries=%v, NATS='%s'", binaryFiles, *natsURL)
		// Validate required flags for this mode
		if *appNameFlag == "" {
			log.Println("Deploy Error: -name flag is required when -config is not provided.")
			deployFlags.Usage()
			os.Exit(1)
		}
		if *tagFlag == "" {
			log.Println("Deploy Error: -tag flag is required when -config is not provided.")
			deployFlags.Usage()
			os.Exit(1)
		}
		if *replicasFlag <= 0 {
			log.Println("Deploy Error: -replicas must be positive.")
			deployFlags.Usage()
			os.Exit(1)
		}
		if strings.TrimSpace(*nodeFlag) == "" {
			log.Println("Deploy Error: -node cannot be empty.")
			deployFlags.Usage()
			os.Exit(1)
		}

		// Generate the ServiceSpec in memory
		spec = noderunner.ServiceSpec{
			Name: *appNameFlag,
			Tag:  *tagFlag,
			Nodes: []noderunner.NodeSelectorSpec{
				{
					Name:     *nodeFlag,
					Replicas: *replicasFlag,
				},
			},
			// Args and Env are empty when using flags
		}

		log.Printf("Deploying Generated ServiceSpec: AppName='%s', BinaryVersionTag='%s', TargetNode='%s', Replicas=%d",
			spec.Name, spec.Tag, spec.Nodes[0].Name, spec.Nodes[0].Replicas)

		// Marshal the generated spec to YAML for upload
		configData, err = yaml.Marshal(spec)
		if err != nil {
			log.Fatalf("Deploy Error: failed to marshal generated ServiceSpec: %v", err)
		}
	}

	// Common Deployment Logic
	appName := spec.Name // Use name from loaded or generated spec
	tag := spec.Tag      // Use tag from loaded or generated spec

	// NATS Connection and JetStream Setup (remains the same)
	ctxAll, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()
	connectCtx, connectCancel := context.WithTimeout(ctxAll, *timeout)
	nc, js, err := connectNATS(connectCtx, *natsURL, "narun-cli")
	connectCancel()
	if err != nil {
		log.Fatalf("Deploy Error: %v", err)
	}
	defer nc.Close()

	// Get Object Store handle (remains the same)
	osCtx, osCancel := context.WithTimeout(ctxAll, 5*time.Second)
	objStore, err := js.ObjectStore(osCtx, noderunner.AppBinariesOSBucket)
	osCancel()
	if err != nil {
		createCtx, createCancel := context.WithTimeout(ctxAll, 10*time.Second)
		objStore, err = js.CreateObjectStore(createCtx, jetstream.ObjectStoreConfig{Bucket: noderunner.AppBinariesOSBucket, Description: "Narun binaries"})
		createCancel()
		if err != nil {
			log.Fatalf("Deploy Error: accessing/creating Object Store '%s': %v", noderunner.AppBinariesOSBucket, err)
		}
	}

	// Loop Through Provided Binaries (remains the same, uses binaryVersionTag from spec)
	uploadErrors := false
	for _, binaryPath := range binaryFiles {
		log.Printf("--- Processing binary: %s ---", binaryPath)
		if _, err := os.Stat(binaryPath); os.IsNotExist(err) {
			log.Printf("Error: Binary file not found at %s. Skipping.", binaryPath)
			uploadErrors = true
			continue
		}

		operSys, goarch, err := detectBinaryPlatform(binaryPath)
		if err != nil {
			log.Printf("Error: Could not detect platform for %s: %v. Skipping.", binaryPath, err)
			uploadErrors = true
			continue
		}
		log.Printf("Detected Platform: %s / %s", operSys, goarch)

		// Construct object name using the tag from the spec (loaded or generated)
		objectName := fmt.Sprintf("%s-%s-%s", tag, operSys, goarch)
		log.Printf("--> NATS Object Store Name: %s", objectName)

		binaryBaseName := filepath.Base(binaryPath)
		log.Printf("Uploading binary '%s' to Object Store as '%s'...", binaryBaseName, objectName)

		fileHandle, err := os.Open(binaryPath)
		if err != nil {
			log.Printf("Error opening binary %s: %v. Skipping.", binaryPath, err)
			uploadErrors = true
			continue
		}

		putCtx, putCancel := context.WithTimeout(ctxAll, *timeout*2) // Increased timeout for potentially large binaries
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
		fileHandle.Close() // Close handle regardless of error
		if err != nil {
			log.Printf("Error uploading binary '%s' as '%s': %v. Skipping.", binaryBaseName, objectName, err)
			uploadErrors = true
			continue
		}

		log.Printf("Binary uploaded: Name=%s, Size=%d, Digest=%s", objInfo.Name, objInfo.Size, objInfo.Digest)
		log.Printf(" --> Metadata: GOOS=%s, GOARCH=%s, VersionTag=%s", objInfo.Metadata["narun-goos"], objInfo.Metadata["narun-goarch"], objInfo.Metadata["narun-version-tag"])
		log.Printf("--- Finished processing binary: %s ---", binaryPath)
	}

	if uploadErrors {
		log.Fatal("Deployment failed: One or more binary uploads encountered errors.")
	}

	// Upload/Update Config to KV Store (remains the same, uses appName and configData)
	kvCtx, kvCancel := context.WithTimeout(ctxAll, 10*time.Second)
	kvStore, err := js.KeyValue(kvCtx, noderunner.AppConfigKVBucket)
	kvCancel()
	if err != nil {
		createCtx, createCancel := context.WithTimeout(ctxAll, 10*time.Second)
		kvStore, err = js.CreateKeyValue(createCtx, jetstream.KeyValueConfig{Bucket: noderunner.AppConfigKVBucket, Description: "Narun configs"})
		createCancel()
		if err != nil {
			log.Fatalf("Deploy Error: accessing/creating Key-Value Store '%s': %v", noderunner.AppConfigKVBucket, err)
		}
	}
	log.Printf("Updating configuration key '%s' in KV store...", appName)
	putCtx, putCancel := context.WithTimeout(ctxAll, 10*time.Second)
	revision, err := kvStore.Put(putCtx, appName, configData) // Upload loaded or generated YAML
	putCancel()
	if err != nil {
		log.Fatalf("Deploy Error: updating configuration key '%s': %v", appName, err)
	}
	log.Printf("Configuration updated: Key=%s, Revision=%d", appName, revision)
	log.Printf("Deployment of application '%s' (tag '%s') completed successfully.", appName, tag)
}

func handleLogsCmd(args []string) {
	logsFlags := flag.NewFlagSet("logs", flag.ExitOnError)
	natsURL := logsFlags.String("nats", DefaultNatsURL, "NATS server URL")
	appName := logsFlags.String("app", "", "Filter logs by application name")
	nodeID := logsFlags.String("node", "", "Filter logs by node ID")
	instanceID := logsFlags.String("instance", "", "Filter logs by specific instance ID (requires -app)")
	follow := logsFlags.Bool("f", false, "Follow the log stream continuously")
	logsFlags.BoolVar(follow, "follow", false, "Follow the log stream continuously")
	raw := logsFlags.Bool("raw", false, "Output raw JSON log messages")
	timeout := logsFlags.Duration("timeout", 1*time.Minute, "Timeout for NATS connection/initial setup")

	if err := logsFlags.Parse(args); err != nil {
		log.Fatalf("Error parsing logs flags: %v", err)
	}
	if *instanceID != "" && *appName == "" {
		log.Fatal("Logs Error: -instance flag requires the -app flag to be set.")
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
	nc, _, err := connectNATS(ctx, *natsURL, "narun-cli")
	if err != nil {
		log.Fatalf("Logs Error: %v", err)
	}
	defer nc.Close()

	log.Printf("Subscribing to NATS logs: Subject='%s'", natsSubject)
	msgChan := make(chan *nats.Msg, 64)
	sub, err := nc.ChanSubscribe(natsSubject, msgChan)
	if err != nil {
		log.Fatalf("Logs Error: Failed to subscribe to '%s': %v", natsSubject, err)
	}
	defer func() {
		log.Println("Unsubscribing...")
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("Logs Error: Failed to unsubscribe: %v", err)
		}
		close(msgChan)
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Waiting for logs... Press Ctrl+C to exit.")

	for {
		select {
		case msg := <-msgChan:
			if msg == nil {
				log.Println("Subscription channel closed.")
				return
			}
			if *raw {
				fmt.Println(string(msg.Data))
				continue
			}
			var entry logEntry
			if err := json.Unmarshal(msg.Data, &entry); err != nil {
				log.Printf("WARN: Failed to parse log JSON: %v - Data: %s", err, string(msg.Data))
				continue
			}
			if *instanceID != "" && entry.InstanceID != *instanceID {
				continue
			}
			fmt.Printf("[%s] [%s] [%s] %s\n", entry.Timestamp.Format(time.RFC3339Nano), entry.InstanceID, entry.Stream, entry.Message)
		case <-sigChan:
			log.Println("Received interrupt signal. Exiting...")
			return
		}
	}
}

// List Images Command Logic
func handleListImagesCmd(args []string) {
	listImagesFlags := flag.NewFlagSet("list-images", flag.ExitOnError)
	natsURL := listImagesFlags.String("nats", DefaultNatsURL, "NATS server URL")
	timeout := listImagesFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	// TODO: Add filters like -app, -tag, -goos, -goarch later

	if err := listImagesFlags.Parse(args); err != nil {
		log.Fatalf("Error parsing list-images flags: %v", err)
	}

	slog.Debug(fmt.Sprintf("Listing images from NATS Object Store '%s'...", noderunner.AppBinariesOSBucket))

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	nc, js, err := connectNATS(ctx, *natsURL, "narun-cli")
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()

	objStore, err := js.ObjectStore(ctx, noderunner.AppBinariesOSBucket)
	if err != nil {
		// If the bucket doesn't exist, treat as no images found
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, nats.ErrBucketNotFound) { // Check both errors
			log.Printf("Object Store '%s' not found. No images to list.", noderunner.AppBinariesOSBucket)
			return
		}
		log.Fatalf("Error accessing Object Store '%s': %v", noderunner.AppBinariesOSBucket, err)
	}

	// List objects
	listCtx, listCancel := context.WithTimeout(ctx, *timeout) // Use timeout for list op too
	defer listCancel()
	objects, err := objStore.List(listCtx)
	if err != nil {
		// Check if the error is specifically no objects found
		if errors.Is(err, jetstream.ErrNoObjectsFound) {
			log.Println("No images found in the object store.")
			return
		}
		log.Fatalf("Error listing objects in store '%s': %v", noderunner.AppBinariesOSBucket, err)
	}

	// Prepare Tabular Output
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "OBJECT NAME\tSIZE\tMODIFIED\tTAG\tOS\tARCH\tDIGEST")
	fmt.Fprintln(tw, "-----------\t----\t--------\t---\t--\t----\t------")

	count := 0
	for _, objInfo := range objects {
		if objInfo == nil {
			continue
		} // Skip nil entries if any
		count++

		// Extract metadata safely
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

		modTime := objInfo.ModTime.Local().Format(time.RFC3339) // Format time

		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%s\n",
			objInfo.Name,
			objInfo.Size,
			modTime,
			tag,
			goos,
			goarch,
			objInfo.Digest,
		)
	}

	tw.Flush()
	slog.Debug(fmt.Sprintf("Found %d image(s).", count))
}

// List Apps Command Logic
func handleListAppsCmd(args []string) {
	listAppsFlags := flag.NewFlagSet("list-apps", flag.ExitOnError)
	natsURL := listAppsFlags.String("nats", DefaultNatsURL, "NATS server URL")
	timeout := listAppsFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	// TODO: Add filters like -app, -node later

	if err := listAppsFlags.Parse(args); err != nil {
		log.Fatalf("Error parsing list-apps flags: %v", err)
	}

	slog.Debug("Listing deployed applications and node status...")

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	nc, js, err := connectNATS(ctx, *natsURL, "narun-cli")
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()

	// Get Node States
	nodeStates := make(map[string]noderunner.NodeState)
	kvNodeStates, err := js.KeyValue(ctx, noderunner.NodeStateKVBucket)
	if err != nil {
		slog.Warn(fmt.Sprintf("Could not access Node State KV '%s': %v. Node status will be unavailable.", noderunner.NodeStateKVBucket, err))
	} else {
		nodeKeysWatcher, err := kvNodeStates.ListKeys(ctx)
		if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
			slog.Warn(fmt.Sprintf("Failed to list node keys from '%s': %v", noderunner.NodeStateKVBucket, err))
		} else if err == nil { // Only proceed if ListKeys didn't fail immediately
			nodeKeysChan := nodeKeysWatcher.Keys()
		nodeLoop:
			for {
				select {
				case <-ctx.Done():
					slog.Warn(fmt.Sprintf("Timed out listing node keys: %v", ctx.Err()))
					break nodeLoop
				case nodeKey, ok := <-nodeKeysChan:
					if !ok {
						break nodeLoop
					} // Channel closed
					if nodeKey == "" {
						continue
					}

					entry, err := kvNodeStates.Get(ctx, nodeKey)
					if err != nil {
						slog.Warn(fmt.Sprintf("Failed to get node state for key '%s': %v", nodeKey, err))
						continue
					}
					var state noderunner.NodeState
					if err := json.Unmarshal(entry.Value(), &state); err != nil {
						slog.Warn(fmt.Sprintf("Failed to unmarshal node state for key '%s': %v", nodeKey, err))
						continue
					}
					nodeStates[nodeKey] = state
				}
			}
			nodeKeysWatcher.Stop() // Ensure watcher is stopped
		}
	}
	slog.Debug(fmt.Sprintf("Found %d active node(s).", len(nodeStates)))

	// Get App Configs and Correlate
	appConfigs := make(map[string]noderunner.ServiceSpec)
	appInstanceCounts := make(map[string]map[string]int) // map[appName][nodeID]count

	// Pre-calculate instance counts per node
	for nodeID, state := range nodeStates {
		for _, instanceID := range state.ManagedInstances {
			appName := InstanceIDToAppName(instanceID)
			if _, ok := appInstanceCounts[appName]; !ok {
				appInstanceCounts[appName] = make(map[string]int)
			}
			appInstanceCounts[appName][nodeID]++
		}
	}

	// Get app configs
	kvAppConfigs, err := js.KeyValue(ctx, noderunner.AppConfigKVBucket)
	if err != nil {
		slog.Warn(fmt.Sprintf("Could not access App Config KV '%s': %v. Cannot list app details.", noderunner.AppConfigKVBucket, err))
	} else {
		appKeysWatcher, err := kvAppConfigs.ListKeys(ctx)
		if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
			slog.Warn(fmt.Sprintf("Failed to list app keys from '%s': %v", noderunner.AppConfigKVBucket, err))
		} else if err == nil { // Only proceed if ListKeys didn't fail immediately
			appKeysChan := appKeysWatcher.Keys()
		appLoop:
			for {
				select {
				case <-ctx.Done():
					slog.Warn(fmt.Sprintf("Timed out listing app keys: %v", ctx.Err()))
					break appLoop
				case appKey, ok := <-appKeysChan:
					if !ok {
						break appLoop
					} // Channel closed
					if appKey == "" {
						continue
					}

					entry, err := kvAppConfigs.Get(ctx, appKey)
					if err != nil {
						slog.Warn(fmt.Sprintf("Failed to get app config for key '%s': %v", appKey, err))
						continue
					}
					var spec noderunner.ServiceSpec
					if err := yaml.Unmarshal(entry.Value(), &spec); err != nil {
						slog.Warn(fmt.Sprintf("Failed to unmarshal app config for key '%s': %v", appKey, err))
						continue
					}
					appConfigs[appKey] = spec
				}
			}
			appKeysWatcher.Stop() // Ensure watcher is stopped
		}
	}

	// Display Results
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "APPLICATION\tTAG\tNODE\tNODE STATUS\tNODE PLATFORM\tRUNNING INSTANCES")
	fmt.Fprintln(tw, "-----------\t---\t----\t-----------\t-------------\t-----------------")

	// Sort app names for consistent output
	appNames := make([]string, 0, len(appConfigs))
	for name := range appConfigs {
		appNames = append(appNames, name)
	}
	sort.Strings(appNames)

	if len(appNames) == 0 && len(nodeStates) == 0 {
		fmt.Fprintln(tw, "(No applications or nodes found)")
	} else {
		// Iterate through sorted app names
		for _, appName := range appNames {
			spec := appConfigs[appName]
			nodesRunningApp := appInstanceCounts[appName]
			if len(nodesRunningApp) == 0 {
				// App is configured but not running anywhere currently
				fmt.Fprintf(tw, "%s\t%s\t-\t-\t-\t0\n",
					appName,
					spec.Tag,
				)
			} else {
				// Sort node IDs for consistent output within an app
				runningNodeIDs := make([]string, 0, len(nodesRunningApp))
				for nodeID := range nodesRunningApp {
					runningNodeIDs = append(runningNodeIDs, nodeID)
				}
				sort.Strings(runningNodeIDs)

				for _, nodeID := range runningNodeIDs {
					count := nodesRunningApp[nodeID]
					nodeInfo, nodeFound := nodeStates[nodeID]
					nodeStatus := "-"
					nodePlatform := "-/-"
					if nodeFound {
						nodeStatus = nodeInfo.Status
						nodePlatform = fmt.Sprintf("%s/%s", nodeInfo.OS, nodeInfo.Arch)
					}
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\n",
						appName,
						spec.Tag,
						nodeID,
						nodeStatus,
						nodePlatform,
						count,
					)
				}
			}
		}
		// Optionally, list nodes that are running but have no apps matching current configs? Unlikely scenario.
	}

	tw.Flush()
}

// Delete App Command Logic
func handleAppDeleteCmd(args []string) {
	deleteAppFlags := flag.NewFlagSet("delete-app", flag.ExitOnError)
	natsURL := deleteAppFlags.String("nats", DefaultNatsURL, "NATS server URL")
	timeout := deleteAppFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	skipConfirm := deleteAppFlags.Bool("y", false, "Skip confirmation prompt")

	deleteAppFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: %s delete-app [options] <app_name>

Deletes an application configuration from NATS KV. This will cause node runners
to stop and remove instances of this application.

Arguments:
  <app_name>   Name of the application configuration to delete (required).

Options:
`, os.Args[0])
		deleteAppFlags.PrintDefaults()
	}

	if err := deleteAppFlags.Parse(args); err != nil {
		return // Usage already printed by ExitOnError
	}

	if deleteAppFlags.NArg() != 1 {
		log.Println("Delete App Error: Exactly one application name argument is required.")
		deleteAppFlags.Usage()
		os.Exit(1)
	}
	appName := deleteAppFlags.Arg(0)

	// Confirmation Prompt
	if !*skipConfirm {
		fmt.Printf("WARNING: This will permanently delete the configuration for application '%s'.\n", appName)
		fmt.Printf("Node runners watching this configuration will stop all instances of this application.\n")
		fmt.Print("Are you sure you want to proceed? (yes/no): ")
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(response) != "yes" {
			log.Println("Delete operation cancelled.")
			os.Exit(0)
		}
	}

	slog.Debug(fmt.Sprintf("Deleting application configuration '%s'...", appName))

	// NATS Connection and JetStream Setup
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	nc, js, err := connectNATS(ctx, *natsURL, "narun-cli")
	if err != nil {
		log.Fatalf("Delete Error: %v", err)
	}
	defer nc.Close()

	// Get KV Store handle
	kvCtx, kvCancel := context.WithTimeout(ctx, 5*time.Second)
	kvStore, err := js.KeyValue(kvCtx, noderunner.AppConfigKVBucket)
	kvCancel()
	if err != nil {
		log.Fatalf("Delete Error: accessing Key-Value Store '%s': %v", noderunner.AppConfigKVBucket, err)
	}

	// Check if key exists before deleting
	getCtx, getCancel := context.WithTimeout(ctx, 5*time.Second)
	_, getErr := kvStore.Get(getCtx, appName)
	getCancel()
	if getErr != nil {
		if errors.Is(getErr, jetstream.ErrKeyNotFound) {
			fmt.Printf("Application configuration '%s' not found. Nothing to delete.", appName)
			os.Exit(0)
		}
		log.Fatalf("Delete Error: failed to check if key '%s' exists: %v", appName, getErr)
	}

	// Delete the key
	slog.Debug(fmt.Sprintf("Deleting key '%s' from KV store '%s'...", appName, noderunner.AppConfigKVBucket))
	delCtx, delCancel := context.WithTimeout(ctx, 10*time.Second)
	err = kvStore.Delete(delCtx, appName)
	delCancel()
	if err != nil {
		log.Fatalf("Delete Error: failed to delete key '%s': %v", appName, err)
	}

	fmt.Printf("Successfully deleted application configuration '%s'.", appName)
	slog.Debug(fmt.Sprintf("Node runners will now stop instances for this application."))
}

func connectNATS(ctx context.Context, url string, clientName string) (*nats.Conn, jetstream.JetStream, error) {
	// Add client name suffix if empty
	if clientName == "" {
		clientName = "narun-cli-unknown"
	}
	slog.Debug(fmt.Sprintf("Connecting to NATS server %s as %s...", url, clientName))
	nc, err := nats.Connect(url,
		nats.Name(clientName),
		nats.Timeout(5*time.Second), // Use shorter timeout for CLI
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Printf("WARN: NATS disconnected: %v", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) { slog.Debug(fmt.Sprintf("INFO: NATS reconnected to %s", nc.ConnectedUrl())) }),
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

// InstanceIDToAppName helper (copied from noderunner or shared internal pkg)
func InstanceIDToAppName(instanceID string) string {
	lastDash := strings.LastIndex(instanceID, "-")
	if lastDash == -1 {
		return instanceID // Or handle error? Assume format is correct.
	}
	return instanceID[:lastDash]
}
