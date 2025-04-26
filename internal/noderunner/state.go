package noderunner

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// AppStatus represents the state of a managed application process.
type AppStatus string

const (
	StatusStarting AppStatus = "starting"
	StatusRunning  AppStatus = "running"
	StatusStopping AppStatus = "stopping"
	StatusStopped  AppStatus = "stopped"
	StatusFailed   AppStatus = "failed"
	StatusCrashed  AppStatus = "crashed" // For unexpected exits
)

// ManagedApp holds the runtime state for an application instance managed by the runner.
type ManagedApp struct {
	InstanceID    string             // Unique ID for this instance (e.g., "appName-0")
	Spec          *ServiceSpec       // The configuration this instance is running with (pointer to shared spec)
	Cmd           *exec.Cmd          // The running process command
	Status        AppStatus          // Current status of the app instance
	Pid           int                // Process ID
	StartTime     time.Time          // When the process was last started
	ConfigHash    string             // Hash of the ServiceSpec YAML used for this generation
	BinaryPath    string             // Local path to the executable being run
	StdoutPipe    io.ReadCloser      // Pipe for stdout
	StderrPipe    io.ReadCloser      // Pipe for stderr
	LogWg         sync.WaitGroup     // WaitGroup for log forwarding goroutines
	StopSignal    chan struct{}      // Signal channel to stop monitoring/restarting
	processCtx    context.Context    // Context for the running process and its monitors
	processCancel context.CancelFunc // Function to cancel the process context
	restartCount  int                // Number of times restarted since last config change/start
	lastExitCode  *int               // Store the last exit code
}

// appInfo holds the state for all instances of a single application on this node.
type appInfo struct {
	mu         sync.RWMutex
	spec       *ServiceSpec  // Current desired spec for this app
	instances  []*ManagedApp // Slice of currently managed instances
	configHash string        // Hash of the spec currently applied
}

// AppStateManager manages the state of all applications on the node.
type AppStateManager struct {
	mu   sync.RWMutex
	apps map[string]*appInfo // map[appName]*appInfo
}

// NewAppStateManager creates a new state manager.
func NewAppStateManager() *AppStateManager {
	return &AppStateManager{
		apps: make(map[string]*appInfo),
	}
}

// GetAppInfo retrieves the appInfo struct for a given app name. Creates if not exists.
// Ensures thread-safe access to the top-level map.
func (m *AppStateManager) GetAppInfo(appName string) *appInfo {
	m.mu.RLock()
	info, exists := m.apps[appName]
	m.mu.RUnlock()

	if !exists {
		m.mu.Lock()
		// Double-check after acquiring write lock
		info, exists = m.apps[appName]
		if !exists {
			info = &appInfo{
				instances: make([]*ManagedApp, 0),
			}
			m.apps[appName] = info
		}
		m.mu.Unlock()
	}
	return info
}

// GetManagedInstance retrieves a specific instance by its ID.
// Returns the instance and true if found, otherwise nil and false.
func (m *AppStateManager) GetManagedInstance(instanceID string) (*ManagedApp, bool) {
	appName := InstanceIDToAppName(instanceID) // Helper needed
	appInfo := m.GetAppInfo(appName)           // Gets or creates appInfo

	appInfo.mu.RLock()
	defer appInfo.mu.RUnlock()

	for _, instance := range appInfo.instances {
		if instance.InstanceID == instanceID {
			return instance, true
		}
	}
	return nil, false
}

// DeleteApp removes all state for a given app name.
func (m *AppStateManager) DeleteApp(appName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.apps, appName)
}

// GetAllAppInfos returns a copy of the current state map.
// Note: This returns pointers to appInfo, callers must handle locking on appInfo.mu.
func (m *AppStateManager) GetAllAppInfos() map[string]*appInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	appsCopy := make(map[string]*appInfo, len(m.apps))
	for k, v := range m.apps {
		appsCopy[k] = v // Copy the pointer
	}
	return appsCopy
}

// Generates a unique instance ID.
func GenerateInstanceID(appName string, replicaIndex int) string {
	return fmt.Sprintf("%s-%d", appName, replicaIndex)
}

// Extracts app name from instance ID (simple implementation).
func InstanceIDToAppName(instanceID string) string {
	lastDash := strings.LastIndex(instanceID, "-")
	if lastDash == -1 {
		return instanceID // Or handle error? Assume format is correct.
	}
	return instanceID[:lastDash]
}
