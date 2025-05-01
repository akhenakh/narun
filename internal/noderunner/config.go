package noderunner

import (
	"fmt"
	"runtime"
	"strings"
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
	FileOSBucket          = "narun-files"      // Object store for user files
	LogSubjectPrefix      = "logs"             // prefix for the logs
	SecretKVBucket        = "narun-secrets"    // KV store for encrypted secrets
)

// EnvVar defines an environment variable for the service.
// Now includes ValueFromSecret. Only one of Value or ValueFromSecret should be set.
type EnvVar struct {
	Name            string `yaml:"name"`
	Value           string `yaml:"value"`
	ValueFromSecret string `yaml:"valueFromSecret,omitempty"` // Name of the secret in the SecretKVBucket
}

// SourceSpec defines the source of a mounted file or volume.
// Currently, only ObjectStore is supported.
type SourceSpec struct {
	ObjectStore string `yaml:"objectStore,omitempty"` // Name of the file object in the FileOSBucket
	// Could add Secret, ConfigMap, Volume later
}

// MountSpec defines a file or volume to be mounted into the instance's working directory.
type MountSpec struct {
	Path   string     `yaml:"path"`   // Relative path within the instance's working directory
	Source SourceSpec `yaml:"source"` // Source of the mount content
}

// LandlockPathSpec defines a specific path and its allowed access modes for Landlock.
type LandlockPathSpec struct {
	Path  string `yaml:"path"`  // Filesystem path (absolute or relative to work dir?) - Assume absolute for now, user needs to know.
	Modes string `yaml:"modes"` // Permissions string (e.g., "r", "rw", "rwc", "rx")
}

// LandlockSpec defines the Landlock configuration for a service.
type LandlockSpec struct {
	Shared bool               `yaml:"shared,omitempty"` // Allow access to common shared libraries
	Stdio  bool               `yaml:"stdio,omitempty"`  // Allow standard I/O operations
	TTY    bool               `yaml:"tty,omitempty"`    // Allow terminal operations
	Tmp    bool               `yaml:"tmp,omitempty"`    // Allow access to /tmp
	VMInfo bool               `yaml:"vminfo,omitempty"` // Allow reading system VM info
	DNS    bool               `yaml:"dns,omitempty"`    // Allow necessary files for DNS resolution
	Certs  bool               `yaml:"certs,omitempty"`  // Allow reading system SSL/TLS certificates
	Paths  []LandlockPathSpec `yaml:"paths,omitempty"`  // Custom paths to allow access to
}

// NodeSelectorSpec defines which node should run the service and how many replicas.
type NodeSelectorSpec struct {
	Name     string `yaml:"name"`     // Node ID (matches node-runner's ID)
	Replicas int    `yaml:"replicas"` // Number of instances on this node
}

// ServiceSpec defines the desired configuration for an application managed by the node runner.
// This structure is stored as YAML in the NATS KV store.
type ServiceSpec struct {
	Name     string             `yaml:"name"`               // Name of the service/app, used as KV key
	Command  string             `yaml:"command,omitempty"`  // Optional: command to run (defaults to binary name)
	Args     []string           `yaml:"args,omitempty"`     // Arguments to pass to the command
	Env      []EnvVar           `yaml:"env,omitempty"`      // Environment variables to set
	Tag      string             `yaml:"tag"`                // Tag for the binary
	Nodes    []NodeSelectorSpec `yaml:"nodes,omitempty"`    // List of nodes to deploy on and replica counts
	Mode     string             `yaml:"mode,omitempty"`     // Execution mode: "exec" (default) or "landlock"
	Landlock LandlockSpec       `yaml:"landlock,omitempty"` // Landlock specific configuration
	Mounts   []MountSpec        `yaml:"mounts,omitempty"`   // Files to mount into the instance directory
}

// StoredSecret is the structure stored as JSON in the SecretKVBucket.
type StoredSecret struct {
	Name        string    `json:"name"`       // Name of the secret (for verification, used as AAD)
	Ciphertext  []byte    `json:"ciphertext"` // AES-GCM encrypted value
	Nonce       []byte    `json:"nonce"`      // Nonce used for encryption (must be unique per key+encryption)
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// NodeState represents the information stored about a node runner in the KV store.
type NodeState struct {
	NodeID           string    `json:"node_id"`
	LastSeen         time.Time `json:"last_seen"`         // Timestamp of the last heartbeat
	Version          string    `json:"version"`           // Version of the node-runner binary
	StartTime        time.Time `json:"start_time"`        // When this runner instance started
	ManagedInstances []string  `json:"managed_instances"` // List of instance IDs currently managed (e.g., "hello-0", "hello-1")
	Status           string    `json:"status"`            // e.g., "running", "shutting_down"
	OS               string    `json:"os"`                // OS the runner is on
	Arch             string    `json:"arch"`              // Architecture the runner is on
}

// ParseServiceSpec parses the YAML byte slice into a ServiceSpec struct.
func ParseServiceSpec(data []byte) (*ServiceSpec, error) {
	var spec ServiceSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ServiceSpec YAML: %w", err)
	}

	if spec.Name == "" {
		return nil, fmt.Errorf("service 'name' is required")
	}
	if spec.Tag == "" {
		return nil, fmt.Errorf("service 'tag' is required (e.g., myapp-v1.0)")
	}
	if spec.Command == "" {
		// Default command to the binary name if not specified
		// Note: This default might be less useful now, as the actual binary filename
		// will include OS/Arch. The path retrieved from fetchAndStoreBinary is more reliable.
		spec.Command = spec.Name // Default command (will use local binary path later)
	}

	// Default and Validate Mode
	if spec.Mode == "" {
		spec.Mode = "exec" // Default mode
	}
	if spec.Mode != "exec" && spec.Mode != "landlock" {
		return nil, fmt.Errorf("invalid mode '%s': must be 'exec' or 'landlock'", spec.Mode)
	}

	// Validate Landlock spec only if mode is landlock
	if spec.Mode == "landlock" {
		if runtime.GOOS != "linux" {
			// Log warning during parse, but runtime check in runner is definitive
			fmt.Printf("Warning: Landlock mode specified for app '%s' during parse, but current OS is not Linux (%s).\n", spec.Name, runtime.GOOS)
		}
		// Validate custom paths
		for i, p := range spec.Landlock.Paths {
			if strings.TrimSpace(p.Path) == "" {
				return nil, fmt.Errorf("landlock path at index %d: 'path' cannot be empty", i)
			}
			if strings.TrimSpace(p.Modes) == "" {
				return nil, fmt.Errorf("landlock path '%s': 'modes' cannot be empty", p.Path)
			}
			validModes := "rwxc"
			for _, char := range p.Modes {
				if !strings.ContainsRune(validModes, char) {
					return nil, fmt.Errorf("landlock path '%s': invalid character '%c' in modes '%s'. Only 'r', 'w', 'x', 'c' allowed.", p.Path, char, p.Modes)
				}
			}
		}
	}

	// Validate Env Vars: only one of value or valueFromSecret should be set
	for i, env := range spec.Env {
		if env.Name == "" {
			return nil, fmt.Errorf("env var at index %d: 'name' cannot be empty", i)
		}
		if env.Value != "" && env.ValueFromSecret != "" {
			return nil, fmt.Errorf("env var '%s': cannot specify both 'value' and 'valueFromSecret'", env.Name)
		}
		// Allow both to be empty? Maybe for system-provided vars later. For now, require one or the other if Env is defined.
		// Let's relax this: allow neither to be set if desired (e.g. just inheriting).
		// if env.Value == "" && env.ValueFromSecret == "" {
		// 	return nil, fmt.Errorf("env var '%s': must specify either 'value' or 'valueFromSecret'", env.Name)
		// }
		if strings.ContainsAny(env.Name, " =") {
			return nil, fmt.Errorf("env var name '%s' contains invalid characters", env.Name)
		}
	}

	// Validate Mounts
	for i, mount := range spec.Mounts {
		if strings.TrimSpace(mount.Path) == "" {
			return nil, fmt.Errorf("mount at index %d: 'path' cannot be empty", i)
		}
		// Basic validation: currently only objectStore source is supported
		if strings.TrimSpace(mount.Source.ObjectStore) == "" {
			return nil, fmt.Errorf("mount '%s': source.objectStore name cannot be empty", mount.Path)
		}
	}

	nodeNames := make(map[string]bool)
	for i, nodeSpec := range spec.Nodes {
		if strings.TrimSpace(nodeSpec.Name) == "" {
			return nil, fmt.Errorf("node selector at index %d: 'name' cannot be empty", i)
		}
		if nodeSpec.Replicas <= 0 {
			return nil, fmt.Errorf("node selector for '%s': 'replicas' must be positive, got %d", nodeSpec.Name, nodeSpec.Replicas)
		}
		if nodeNames[nodeSpec.Name] {
			return nil, fmt.Errorf("duplicate node selector for name '%s'", nodeSpec.Name)
		}
		nodeNames[nodeSpec.Name] = true
	}

	return &spec, nil
}

// FindTargetReplicas returns the target replica count for a specific nodeID.
// Returns 0 if the nodeID is not found in the spec's Nodes list.
func (s *ServiceSpec) FindTargetReplicas(nodeID string) int {
	for _, nodeSpec := range s.Nodes {
		if nodeSpec.Name == nodeID {
			return nodeSpec.Replicas
		}
	}
	return 0 // Not targeted for this node
}
