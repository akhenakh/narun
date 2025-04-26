package noderunner

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	AppConfigKVBucket     = "app-configs"
	AppBinariesOSBucket   = "app-binaries"
	DefaultKVTTL          = 1 * time.Hour // Example TTL for KV entries if needed
	NodeStateKVBucket     = "node-runner-states"
	NodeStateKVTTL        = 45 * time.Second   // Key expires if not updated within this duration
	NodeHeartbeatInterval = NodeStateKVTTL / 3 // Update frequency (e.g., every 15s)
)

// EnvVar defines an environment variable for the service.
type EnvVar struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

// Mount defines a volume mount (Simplified - Not Implemented in Execution Yet).
type Mount struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}

// ServiceSpec defines the desired configuration for an application managed by the node runner.
// This structure is stored as YAML in the NATS KV store.
type ServiceSpec struct {
	Name    string   `yaml:"name"`              // Name of the service/app, used as KV key and potentially binary name
	Command string   `yaml:"command,omitempty"` // Optional: command to run (defaults to binary name)
	Args    []string `yaml:"args,omitempty"`    // Arguments to pass to the command
	Env     []EnvVar `yaml:"env,omitempty"`     // Environment variables to set
	// Mounts    []Mount  `yaml:"mount,omitempty"` // Mount points (Future Enhancement)
	// Replica int      `yaml:"replica"`           // Target replica count (Not directly used by node runner)
	BinaryObject string `yaml:"binary_object"` // Name of the binary object in the Object Store
}

// NodeState represents the information stored about a node runner in the KV store.
type NodeState struct {
	NodeID      string    `json:"node_id"`
	LastSeen    time.Time `json:"last_seen"`    // Timestamp of the last heartbeat
	Version     string    `json:"version"`      // Version of the node-runner binary
	StartTime   time.Time `json:"start_time"`   // When this runner instance started
	ManagedApps []string  `json:"managed_apps"` // List of app names currently managed
	Status      string    `json:"status"`       // e.g., "running", "shutting_down"
}

// ParseServiceSpec parses the YAML byte slice into a ServiceSpec struct.
func ParseServiceSpec(data []byte) (*ServiceSpec, error) {
	var spec ServiceSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ServiceSpec YAML: %w", err)
	}

	// Validation
	if spec.Name == "" {
		return nil, fmt.Errorf("service name is required")
	}
	if spec.BinaryObject == "" {
		// Default to Name if BinaryObject not specified
		spec.BinaryObject = spec.Name
		// return nil, fmt.Errorf("service binary_object name is required")
	}
	if spec.Command == "" {
		// Default command to the binary name if not specified
		spec.Command = spec.Name // We'll use the local path later
	}

	return &spec, nil
}
