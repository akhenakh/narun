// narun/cmd/narun-deploy/main.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	// We need the nats.go library and yaml parsing
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

// Configuration Constants ---
// These should match the values used by the node-runner
const (
	AppConfigKVBucket   = "app-configs"
	AppBinariesOSBucket = "app-binaries"
	DefaultNatsURL      = "nats://localhost:4222"
	DefaultTimeout      = 15 * time.Second
)

// --- ServiceSpec Definition ---
// Copied from internal/noderunner/config.go for simplicity.
// Alternatively, you could import github.com/akhenakh/narun/internal/noderunner
// but that might create unwanted dependencies for a simple CLI tool.

type EnvVar struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type ServiceSpec struct {
	Name         string   `yaml:"name"`              // Name of the service/app, used as KV key
	Command      string   `yaml:"command,omitempty"` // Optional: command to run (defaults to binary name)
	Args         []string `yaml:"args,omitempty"`    // Arguments to pass to the command
	Env          []EnvVar `yaml:"env,omitempty"`     // Environment variables to set
	BinaryObject string   `yaml:"binary_object"`     // Name of the binary object in the Object Store (REQUIRED in spec)
}

func main() {
	natsURL := flag.String("nats", DefaultNatsURL, "NATS server URL")
	configFile := flag.String("config", "", "Path to the application ServiceSpec YAML configuration file (required)")
	binaryFile := flag.String("binary", "", "Path to the application binary executable (required)")
	timeout := flag.Duration("timeout", DefaultTimeout, "Timeout for NATS operations")
	flag.Parse()

	if *configFile == "" {
		log.Fatal("Error: -config flag (path to ServiceSpec YAML) is required.")
	}
	if *binaryFile == "" {
		log.Fatal("Error: -binary flag (path to application executable) is required.")
	}

	// Check if files exist
	if _, err := os.Stat(*configFile); os.IsNotExist(err) {
		log.Fatalf("Error: Config file not found at %s", *configFile)
	}
	if _, err := os.Stat(*binaryFile); os.IsNotExist(err) {
		log.Fatalf("Error: Binary file not found at %s", *binaryFile)
	}

	log.Printf("Deploying application using config '%s' and binary '%s' to NATS server '%s'", *configFile, *binaryFile, *natsURL)

	//  Read and Parse Config
	configData, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Error reading config file %s: %v", *configFile, err)
	}

	var spec ServiceSpec
	if err := yaml.Unmarshal(configData, &spec); err != nil {
		log.Fatalf("Error parsing ServiceSpec YAML from %s: %v", *configFile, err)
	}

	// Validate parsed spec
	if spec.Name == "" {
		log.Fatalf("Error: 'name' field is missing or empty in the configuration file %s", *configFile)
	}
	if spec.BinaryObject == "" {
		// Default BinaryObject to Name if not provided in the spec explicitly
		log.Printf("Warning: 'binary_object' not specified in config, defaulting to service name '%s'", spec.Name)
		spec.BinaryObject = spec.Name
		// log.Fatalf("Error: 'binary_object' field is missing or empty in the configuration file %s", *configFile)
	}

	appName := spec.Name
	binaryObjectName := spec.BinaryObject
	log.Printf("Parsed ServiceSpec: AppName='%s', BinaryObject='%s'", appName, binaryObjectName)

	//  NATS Connection and JetStream Setup
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	log.Printf("Connecting to NATS server %s...", *natsURL)
	nc, err := nats.Connect(*natsURL,
		nats.Name("narun-deploy-cli"),
		nats.Timeout(5*time.Second), // Shorter connect timeout
	)
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	log.Println("Connected to NATS successfully.")

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("Error creating JetStream context: %v", err)
	}
	log.Println("JetStream context created.")

	//  Upload Binary to Object Store
	log.Printf("Accessing Object Store bucket: %s", AppBinariesOSBucket)
	objStore, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      AppBinariesOSBucket,
		Description: "Stores application binaries for narun node-runner.",
	})
	if err != nil {
		log.Fatalf("Error accessing Object Store '%s': %v", AppBinariesOSBucket, err)
	}

	binaryBaseName := filepath.Base(*binaryFile)
	log.Printf("Uploading binary '%s' to Object Store as '%s'...", binaryBaseName, binaryObjectName)

	// Open the binary file for reading
	fileHandle, err := os.Open(*binaryFile)
	if err != nil {
		log.Fatalf("Error opening binary file %s for reading: %v", *binaryFile, err)
	}
	defer fileHandle.Close() // Ensure file handle is closed

	// Use objStore.Put with the file handle as the io.Reader
	meta := jetstream.ObjectMeta{
		Name:        binaryObjectName, // The desired name in the object store
		Description: fmt.Sprintf("Binary for %s uploaded via narun-deploy", appName),
		// Headers: map[string][]string{"Original-Filename": {binaryBaseName}}, // Optional header
	}

	// Put takes context, metadata (containing the name), and a reader
	objInfo, err := objStore.Put(ctx, meta, fileHandle)
	if err != nil {
		log.Fatalf("Error uploading binary '%s' using Put: %v", binaryBaseName, err)
	}
	log.Printf("Binary uploaded successfully: Name=%s, Size=%d, Digest=%s", objInfo.Name, objInfo.Size, objInfo.Digest)

	// Upload/Update Config to KV Store
	log.Printf("Accessing Key-Value Store bucket: %s", AppConfigKVBucket)
	kvStore, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      AppConfigKVBucket,
		Description: "Stores application service specifications for narun node-runner.",
	})
	if err != nil {
		log.Fatalf("Error accessing Key-Value Store '%s': %v", AppConfigKVBucket, err)
	}

	log.Printf("Updating configuration key '%s' in KV store...", appName)
	revision, err := kvStore.Put(ctx, appName, configData)
	if err != nil {
		log.Fatalf("Error updating configuration key '%s' in KV store: %v", appName, err)
	}
	log.Printf("Configuration updated successfully: Key=%s, Revision=%d", appName, revision)

	log.Printf("Deployment of application '%s' initiated successfully.", appName)
}
