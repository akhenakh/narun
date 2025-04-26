package main

import (
	"context"
	"encoding/json" // Import encoding/json
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal" // Import os/signal
	"path/filepath"
	"strings"
	"syscall" // Import syscall
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

// Configuration Constants (Shared)
const (
	AppConfigKVBucket   = "app-configs"
	AppBinariesOSBucket = "app-binaries"
	DefaultNatsURL      = "nats://localhost:4222"
	DefaultTimeout      = 15 * time.Second
	LogSubjectPrefix    = "logs" // NATS subject prefix for logs
)

// Struct Definitions (Copied for use in both commands)

type EnvVar struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type NodeSelectorSpec struct {
	Name     string `yaml:"name"`
	Replicas int    `yaml:"replicas"`
}

type ServiceSpec struct {
	Name         string             `yaml:"name"`
	Command      string             `yaml:"command,omitempty"`
	Args         []string           `yaml:"args,omitempty"`
	Env          []EnvVar           `yaml:"env,omitempty"`
	BinaryObject string             `yaml:"binary_object"`
	Nodes        []NodeSelectorSpec `yaml:"nodes,omitempty"`
}

// LogEntry structure expected from node-runner logs
// Keep this in sync with internal/noderunner/process.go
type logEntry struct {
	InstanceID string    `json:"instance_id"`
	AppName    string    `json:"app_name"` // Expect app name for filtering consistency
	NodeID     string    `json:"node_id"`
	Stream     string    `json:"stream"` // "stdout" or "stderr"
	Message    string    `json:"message"`
	Timestamp  time.Time `json:"timestamp"`
}

// Main Function
func main() {
	if len(os.Args) < 2 || os.Args[1] == "deploy" || !isValidSubcommand(os.Args[1]) {
		// Default to deploy or handle deploy explicitly
		// Note: We treat no command or "deploy" as the deployment action.
		// We could make "deploy" mandatory for clarity.
		args := os.Args[1:]
		if len(os.Args) > 1 && os.Args[1] == "deploy" {
			args = os.Args[2:] // Skip the "deploy" command itself
		}
		handleDeployCmd(args)
	} else if os.Args[1] == "logs" {
		handleLogsCmd(os.Args[2:])
	} else {
		fmt.Fprintf(os.Stderr, "Error: Unknown command '%s'\n", os.Args[1])
		fmt.Fprintf(os.Stderr, "Usage: %s [deploy|logs] [options]\n", os.Args[0])
		os.Exit(1)
	}
}

func isValidSubcommand(cmd string) bool {
	switch cmd {
	case "deploy", "logs":
		return true
	default:
		// Allow implicit deploy if the first arg looks like a flag
		return strings.HasPrefix(cmd, "-")
	}
}

// Deploy Command Logic
func handleDeployCmd(args []string) {
	deployFlags := flag.NewFlagSet("deploy", flag.ExitOnError)
	natsURL := deployFlags.String("nats", DefaultNatsURL, "NATS server URL")
	configFile := deployFlags.String("config", "", "Path to the application ServiceSpec YAML configuration file (required)")
	binaryFile := deployFlags.String("binary", "", "Path to the application binary executable (required)")
	timeout := deployFlags.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")

	// Parse flags for the deploy command
	if err := deployFlags.Parse(args); err != nil {
		log.Fatalf("Error parsing deploy flags: %v", err)
	}

	// Use parsed flags
	if *configFile == "" {
		log.Fatal("Deploy Error: -config flag (path to ServiceSpec YAML) is required.")
	}
	if *binaryFile == "" {
		log.Fatal("Deploy Error: -binary flag (path to application executable) is required.")
	}
	// Check files exist
	if _, err := os.Stat(*configFile); os.IsNotExist(err) {
		log.Fatalf("Deploy Error: Config file not found at %s", *configFile)
	}
	if _, err := os.Stat(*binaryFile); os.IsNotExist(err) {
		log.Fatalf("Deploy Error: Binary file not found at %s", *binaryFile)
	}

	log.Printf("Starting Deployment: Config='%s', Binary='%s', NATS='%s'", *configFile, *binaryFile, *natsURL)

	// Read and Parse Config
	configData, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Deploy Error: reading config file %s: %v", *configFile, err)
	}
	var spec ServiceSpec
	if err := yaml.Unmarshal(configData, &spec); err != nil {
		log.Fatalf("Deploy Error: parsing ServiceSpec YAML from %s: %v", *configFile, err)
	}

	// Validate spec (same validation as before)
	if spec.Name == "" {
		log.Fatalf("Deploy Error: 'name' field is missing or empty in %s", *configFile)
	}
	if spec.BinaryObject == "" {
		log.Fatalf("Deploy Error: 'binary_object' field is missing or empty in %s", *configFile)
	}
	// Basic validation for nodes section
	nodeNames := make(map[string]bool)
	for i, nodeSpec := range spec.Nodes {
		if strings.TrimSpace(nodeSpec.Name) == "" {
			log.Fatalf("Deploy Error in config %s: node selector at index %d: 'name' cannot be empty", *configFile, i)
		}
		if nodeSpec.Replicas <= 0 {
			log.Fatalf("Deploy Error in config %s: node selector for '%s': 'replicas' must be positive, got %d", *configFile, nodeSpec.Name, nodeSpec.Replicas)
		}
		if nodeNames[nodeSpec.Name] {
			log.Fatalf("Deploy Error in config %s: duplicate node selector for name '%s'", *configFile, nodeSpec.Name)
		}
		nodeNames[nodeSpec.Name] = true
	}

	appName := spec.Name
	binaryObjectName := spec.BinaryObject
	log.Printf("Deploying ServiceSpec: AppName='%s', BinaryObject='%s', TargetNodes=%+v", appName, binaryObjectName, spec.Nodes)

	// NATS Connection and JetStream Setup
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	nc, js, err := connectNATS(ctx, *natsURL, "narun-deploy-cli")
	if err != nil {
		log.Fatalf("Deploy Error: %v", err)
	}
	defer nc.Close()

	// Upload Binary
	objStore, err := js.ObjectStore(ctx, AppBinariesOSBucket)
	if err != nil {
		// Attempt to create if not found
		log.Printf("Object store '%s' not found, attempting to create...", AppBinariesOSBucket)
		objStore, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
			Bucket:      AppBinariesOSBucket,
			Description: "Stores application binaries for narun node-runner.",
		})
		if err != nil {
			log.Fatalf("Deploy Error: accessing/creating Object Store '%s': %v", AppBinariesOSBucket, err)
		}
	}

	binaryBaseName := filepath.Base(*binaryFile)
	log.Printf("Uploading binary '%s' to Object Store as '%s'...", binaryBaseName, binaryObjectName)
	fileHandle, err := os.Open(*binaryFile)
	if err != nil {
		log.Fatalf("Deploy Error: opening binary file %s: %v", *binaryFile, err)
	}
	defer fileHandle.Close()
	meta := jetstream.ObjectMeta{
		Name:        binaryObjectName,
		Description: fmt.Sprintf("Binary for %s uploaded via narun-deploy", appName),
	}
	objInfo, err := objStore.Put(ctx, meta, fileHandle)
	if err != nil {
		log.Fatalf("Deploy Error: uploading binary '%s': %v", binaryBaseName, err)
	}
	log.Printf("Binary uploaded: Name=%s, Size=%d, Digest=%s", objInfo.Name, objInfo.Size, objInfo.Digest)

	// Upload/Update Config to KV Store
	kvStore, err := js.KeyValue(ctx, AppConfigKVBucket)
	if err != nil {
		log.Printf("KV store '%s' not found, attempting to create...", AppConfigKVBucket)
		kvStore, err = js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
			Bucket:      AppConfigKVBucket,
			Description: "Stores application service specifications for narun node-runner.",
		})
		if err != nil {
			log.Fatalf("Deploy Error: accessing/creating Key-Value Store '%s': %v", AppConfigKVBucket, err)
		}
	}
	log.Printf("Updating configuration key '%s' in KV store...", appName)
	revision, err := kvStore.Put(ctx, appName, configData)
	if err != nil {
		log.Fatalf("Deploy Error: updating configuration key '%s': %v", appName, err)
	}
	log.Printf("Configuration updated: Key=%s, Revision=%d", appName, revision)

	log.Printf("Deployment of application '%s' initiated successfully.", appName)
}

// Logs Command Logic
func handleLogsCmd(args []string) {
	logsFlags := flag.NewFlagSet("logs", flag.ExitOnError)
	natsURL := logsFlags.String("nats", DefaultNatsURL, "NATS server URL")
	appName := logsFlags.String("app", "", "Filter logs by application name")
	nodeID := logsFlags.String("node", "", "Filter logs by node ID")
	instanceID := logsFlags.String("instance", "", "Filter logs by specific instance ID (requires -app)")
	follow := logsFlags.Bool("f", false, "Follow the log stream continuously") // Alias -f
	logsFlags.BoolVar(follow, "follow", false, "Follow the log stream continuously")
	raw := logsFlags.Bool("raw", false, "Output raw JSON log messages")
	timeout := logsFlags.Duration("timeout", 1*time.Minute, "Timeout for NATS connection/initial setup") // Shorter timeout for logs cmd

	// Parse flags for the logs command
	if err := logsFlags.Parse(args); err != nil {
		log.Fatalf("Error parsing logs flags: %v", err)
	}

	if *instanceID != "" && *appName == "" {
		log.Fatal("Logs Error: -instance flag requires the -app flag to be set.")
	}

	// Build NATS subject based on filters
	subjectParts := []string{LogSubjectPrefix}
	if *appName != "" {
		subjectParts = append(subjectParts, *appName)
	} else {
		subjectParts = append(subjectParts, "*") // Wildcard for app name
	}
	if *nodeID != "" {
		subjectParts = append(subjectParts, *nodeID)
	} else {
		subjectParts = append(subjectParts, "*") // Wildcard for node ID
	}
	natsSubject := strings.Join(subjectParts, ".")

	// NATS Connection
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel() // Cancel context on exit

	nc, _, err := connectNATS(ctx, *natsURL, "narun-logs-cli") // Don't need JetStream for logs
	if err != nil {
		log.Fatalf("Logs Error: %v", err)
	}
	defer nc.Close() // Ensure connection is closed

	log.Printf("Subscribing to NATS logs: Subject='%s'", natsSubject)

	// Channel for receiving messages
	msgChan := make(chan *nats.Msg, 64) // Buffered channel

	// Asynchronous Subscription
	sub, err := nc.ChanSubscribe(natsSubject, msgChan)
	if err != nil {
		log.Fatalf("Logs Error: Failed to subscribe to '%s': %v", natsSubject, err)
	}
	defer func() {
		log.Println("Unsubscribing...")
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("Logs Error: Failed to unsubscribe: %v", err)
		}
		close(msgChan) // Close channel after unsubscribing
	}()

	// Handle OS signals for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Waiting for logs... Press Ctrl+C to exit.")

	// Message Processing Loop
	for {
		select {
		case msg := <-msgChan:
			if msg == nil {
				log.Println("Subscription channel closed.")
				return // Exit if channel is closed
			}

			// Raw output
			if *raw {
				fmt.Println(string(msg.Data))
				continue // Skip further processing
			}

			// Parse JSON
			var entry logEntry
			if err := json.Unmarshal(msg.Data, &entry); err != nil {
				log.Printf("WARN: Failed to parse log message JSON: %v - Data: %s", err, string(msg.Data))
				continue // Print error and skip malformed message
			}

			// Apply client-side instance filter if specified
			if *instanceID != "" && entry.InstanceID != *instanceID {
				continue // Skip messages not matching the instance ID
			}

			// Formatted output
			fmt.Printf("[%s] [%s] [%s] %s\n",
				entry.Timestamp.Format(time.RFC3339Nano), // Use precise timestamp
				entry.InstanceID,
				entry.Stream,
				entry.Message,
			)

		case <-sigChan:
			log.Println("Received interrupt signal. Exiting...")
			return // Exit loop on signal

			// TODO: Implement non-follow behavior (e.g., timeout or retrieve history)
			// For now, it always follows if connected.
			// case <-time.After(someDuration): // Example for non-follow timeout
			// 	if !*follow {
			// 		log.Println("Finished retrieving logs.")
			// 		return
			// 	}
		}
	}
}

// connectNATS establishes a connection and returns NATS conn and JetStream context
func connectNATS(ctx context.Context, url string, clientName string) (*nats.Conn, jetstream.JetStream, error) {
	log.Printf("Connecting to NATS server %s...", url)
	nc, err := nats.Connect(url,
		nats.Name(clientName),
		nats.Timeout(5*time.Second), // Shorter timeout for CLI tool
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Printf("WARN: NATS disconnected: %v", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("INFO: NATS reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Printf("INFO: NATS connection closed.")
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to NATS: %w", err)
	}
	log.Println("Connected to NATS successfully.")

	// Only create JetStream context if needed? For deploy yes, for logs no.
	// Let's create it anyway for deploy, logs command won't use it.
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("creating JetStream context: %w", err)
	}
	log.Println("JetStream context created.")

	return nc, js, nil
}
