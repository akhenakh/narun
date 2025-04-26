package noderunner

import (
	"context"
	"io"
	"os/exec"
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
	Spec          *ServiceSpec       // The configuration this instance is running with
	Cmd           *exec.Cmd          // The running process command
	Status        AppStatus          // Current status of the app
	Pid           int                // Process ID
	StartTime     time.Time          // When the process was last started
	ConfigHash    string             // Hash of the ServiceSpec YAML used
	BinaryPath    string             // Local path to the executable being run
	StdoutPipe    io.ReadCloser      // Pipe for stdout
	StderrPipe    io.ReadCloser      // Pipe for stderr
	LogWg         sync.WaitGroup     // WaitGroup for log forwarding goroutines
	StopSignal    chan struct{}      // Signal channel to stop monitoring/restarting
	processCtx    context.Context    // Context for the running process and its monitors
	processCancel context.CancelFunc // Function to cancel the process context
	restartCount  int                // Number of times restarted since last config change
	lastExitCode  *int               // Store the last exit code
}

// AppStateManager manages the state of all applications on the node.
type AppStateManager struct {
	mu   sync.RWMutex
	apps map[string]*ManagedApp // map[appName]*ManagedApp
}

// NewAppStateManager creates a new state manager.
func NewAppStateManager() *AppStateManager {
	return &AppStateManager{
		apps: make(map[string]*ManagedApp),
	}
}

// Get retrieves the state for a given app name.
func (m *AppStateManager) Get(appName string) (*ManagedApp, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	app, exists := m.apps[appName]
	return app, exists
}

// GetAll returns a copy of the current state map.
func (m *AppStateManager) GetAll() map[string]*ManagedApp {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Return a shallow copy to avoid race conditions on the map itself
	appsCopy := make(map[string]*ManagedApp, len(m.apps))
	for k, v := range m.apps {
		appsCopy[k] = v
	}
	return appsCopy
}

// Set stores the state for a given app name.
func (m *AppStateManager) Set(appName string, app *ManagedApp) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.apps[appName] = app
}

// Delete removes the state for a given app name.
func (m *AppStateManager) Delete(appName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.apps, appName)
}

// UpdateStatus updates the status of a specific app.
func (m *AppStateManager) UpdateStatus(appName string, status AppStatus, exitCode *int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if app, exists := m.apps[appName]; exists {
		app.Status = status
		app.lastExitCode = exitCode // Update last exit code
		return true
	}
	return false
}
