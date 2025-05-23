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
	InstanceID    string             // Unique ID for this instance (e.g., "appName-0" or "appName-cron-XYZ")
	RunID         string             // Unique ID for this specific run of the instance process
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
	restartCount  int                // Number of times restarted since last config change/start (for persistent services)
	lastExitCode  *int               // Store the last exit code

	// cgroups related
	cgroupPath    string       // Absolute path to the instance's cgroup
	cgroupFd      int          // File descriptor for the cgroup directory (for UseCgroupFD)
	cgroupCleanup func() error // Function to clean up the cgroup

	IsCronJobRun bool // True if this instance is an ephemeral cron job run
}

// appInfo holds the state for all instances of a single application on this node.
type appInfo struct {
	mu            sync.RWMutex
	spec          *ServiceSpec  // Current desired spec for this app
	instances     []*ManagedApp // Slice of currently managed instances (both replicas and cron runs)
	configHash    string        // Hash of the spec currently applied
	cronEntryID   int           // Store cron.EntryID to manage/remove scheduled job if spec changes. Using int as robfig/cron.EntryID is an alias for int.
	cronScheduler *sync.Mutex   // Mutex specifically for operations on cronEntryID for this app
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
				instances:     make([]*ManagedApp, 0),
				cronScheduler: &sync.Mutex{}, // Initialize mutex for cron scheduling
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
	// Additional cleanup related to cron jobs might be needed here if cronEntryID is stored
	// For now, the runner will handle removing from its cron scheduler
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

// Generates a unique instance ID for persistent replicas.
func GenerateInstanceID(appName string, replicaIndex int) string {
	return fmt.Sprintf("%s-%d", appName, replicaIndex)
}

// Generates a unique instance ID for cron job runs.
func GenerateCronInstanceID(appName string) string {
	// Using a short UUID or timestamp to ensure uniqueness for concurrent cron runs or quick successions
	return fmt.Sprintf("%s-cron-%s", appName, time.Now().UnixNano())
}

// Extracts app name from instance ID.
func InstanceIDToAppName(instanceID string) string {
	// Handle cron job instance IDs like "appName-cron-XXXX"
	if strings.Contains(instanceID, "-cron-") {
		parts := strings.SplitN(instanceID, "-cron-", 2)
		if len(parts) > 0 {
			return parts[0]
		}
		// Fallback if "-cron-" is at the beginning (should not happen with GenerateCronInstanceID)
		return instanceID
	}
	// Handle regular replica instance IDs like "appName-0"
	lastDash := strings.LastIndex(instanceID, "-")
	if lastDash == -1 {
		return instanceID // No dash, assume full ID is app name
	}
	// Check if the part after dash is numeric (replica index)
	if _, err := fmt.Sscan(instanceID[lastDash+1:], new(int)); err == nil {
		return instanceID[:lastDash]
	}
	// If not ending in "-<number>", assume full ID is app name (e.g. app name itself contains a dash)
	return instanceID
}
