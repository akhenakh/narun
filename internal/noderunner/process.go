package noderunner

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors" // Import errors package
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath" // Import strings package
	"strings"

	// Import sync package
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3" // Use v3 consistently if config uses it
)

const (
	RestartDelay    = 5 * time.Second  // Delay before restarting a crashed app
	MaxRestarts     = 5                // Max restarts before giving up (per instance)
	StopTimeout     = 10 * time.Second // Time to wait for graceful shutdown before SIGKILL
	LogBufferSize   = 1024             // Buffer size for log lines
	StatusQueueSize = 10               // Buffer size for status update channel
)

type statusUpdate struct {
	InstanceID string    `json:"instance_id"` // Changed from appName
	NodeID     string    `json:"node_id"`
	Status     AppStatus `json:"status"`
	Pid        int       `json:"pid,omitempty"`
	ExitCode   *int      `json:"exit_code,omitempty"` // Use pointer to distinguish 0 from unset
	Error      string    `json:"error,omitempty"`
	Time       time.Time `json:"time"`
}

type logEntry struct {
	InstanceID string    `json:"instance_id"` // Changed from appName
	AppName    string    `json:"app_name"`    // Keep app name for context
	NodeID     string    `json:"node_id"`
	Stream     string    `json:"stream"` // "stdout" or "stderr"
	Message    string    `json:"message"`
	Timestamp  time.Time `json:"timestamp"`
}

// calculateSpecHash generates a simple hash of the ServiceSpec for change detection.
func calculateSpecHash(spec *ServiceSpec) (string, error) {
	// Use YAML representation for hashing
	data, err := yaml.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("failed to marshal spec for hashing: %w", err)
	}
	h := fnv.New64a()
	_, err = h.Write(data)
	if err != nil {
		// Should not happen with in-memory write
		return "", fmt.Errorf("failed to write spec to hash: %w", err)
	}
	return fmt.Sprintf("%x", h.Sum64()), nil
}

// startAppInstance attempts to download the binary, configure, and start a single instance.
// It assumes the calling code (handleAppConfigUpdate) holds the lock on appInfo.

// startAppInstance attempts to download the binary, configure, and start a single instance.
func (nr *NodeRunner) startAppInstance(ctx context.Context, appInfo *appInfo, replicaIndex int) error {
	spec := appInfo.spec // Use the spec from appInfo
	appName := spec.Name
	instanceID := GenerateInstanceID(appName, replicaIndex)
	logger := nr.logger.With("app", appName, "instance_id", instanceID)
	logger.Info("Attempting to start application instance")

	configHash := appInfo.configHash // Get hash from locked appInfo

	// Check if an instance with this ID exists and is truly RUNNING.
	// Allow overwriting/replacing if status is Starting, Stopped, Crashed, Failed,
	// as this indicates the previous process associated with this ID is gone.
	instanceExists := false
	for _, existingInstance := range appInfo.instances {
		if existingInstance.InstanceID == instanceID {
			instanceExists = true
			if existingInstance.Status == StatusRunning {
				// This is the only status that should prevent a start.
				logger.Warn("Attempted to start instance that is already running.", "status", existingInstance.Status, "pid", existingInstance.Pid)
				// Return error without publishing status, as the instance is already running.
				return fmt.Errorf("instance %s already running with pid %d", instanceID, existingInstance.Pid)
			}
			// If not StatusRunning, log that we are replacing it.
			logger.Debug("Found existing non-running instance state, proceeding to replace.", "status", existingInstance.Status)
			break // Found the relevant instance, proceed.
		}
	}

	// Fetch and Store Binary
	binaryPath, err := nr.fetchAndStoreBinary(ctx, spec.BinaryObject, appName, configHash)
	if err != nil {
		// Publish failure ONLY if the instance didn't previously exist or wasn't running
		// If we are replacing a failed/stopped instance, its state will be updated later.
		if !instanceExists { // Publish failure if this was a fresh start attempt
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("Binary fetch failed: %v", err))
		}
		return fmt.Errorf("failed to fetch binary for %s: %w", instanceID, err)
	}
	logger.Info("Binary downloaded/verified", "path", binaryPath)

	// Prepare Command (Executable Check)
	if err := os.Chmod(binaryPath, 0755); err != nil {
		logger.Error("Failed to make binary executable", "path", binaryPath, "error", err)
		if !instanceExists {
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("Binary not executable: %v", err))
		}
		return fmt.Errorf("failed making binary executable %s: %w", binaryPath, err)
	}

	//  Create context for this specific instance
	processCtx, processCancel := context.WithCancel(nr.globalCtx)

	cmdPath := binaryPath
	cmd := exec.CommandContext(processCtx, cmdPath, spec.Args...)
	cmd.Env = os.Environ()
	// Add BASE env vars
	for _, envVar := range spec.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", envVar.Name, envVar.Value))
	}
	// Add instance-specific env vars
	cmd.Env = append(cmd.Env, fmt.Sprintf("NARUN_APP_NAME=%s", appName))
	cmd.Env = append(cmd.Env, fmt.Sprintf("NARUN_INSTANCE_ID=%s", instanceID))
	cmd.Env = append(cmd.Env, fmt.Sprintf("NARUN_REPLICA_INDEX=%d", replicaIndex))
	cmd.Env = append(cmd.Env, fmt.Sprintf("NARUN_NODE_ID=%s", nr.nodeID))

	//  Directory Setup
	instanceDir := filepath.Join(nr.dataDir, "instances", instanceID)
	workDir := filepath.Join(instanceDir, "work")
	if err := os.MkdirAll(workDir, 0755); err != nil {
		processCancel()
		logger.Error("Failed to create instance working directory", "dir", workDir, "error", err)
		if !instanceExists {
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("Failed work dir: %v", err))
		}
		return fmt.Errorf("failed to create work dir %s for instance %s: %w", workDir, instanceID, err)
	}
	cmd.Dir = workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} // Start process in its own group

	// Pipes
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		processCancel()
		logger.Error("Failed to get stdout pipe", "error", err)
		if !instanceExists {
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("stdout pipe failed: %v", err))
		}
		return fmt.Errorf("stdout pipe failed for %s: %w", instanceID, err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		processCancel()
		stdoutPipe.Close() // Close stdout pipe if stderr fails
		logger.Error("Failed to get stderr pipe", "error", err)
		if !instanceExists {
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("stderr pipe failed: %v", err))
		}
		return fmt.Errorf("stderr pipe failed for %s: %w", instanceID, err)
	}

	// Create/Update ManagedApp State
	// Create the new state object
	appInstance := &ManagedApp{
		InstanceID:    instanceID,
		Spec:          spec,
		Cmd:           cmd,
		Status:        StatusStarting, // Start with Starting status
		ConfigHash:    configHash,
		BinaryPath:    binaryPath,
		StdoutPipe:    stdoutPipe,
		StderrPipe:    stderrPipe,
		StopSignal:    make(chan struct{}),
		processCtx:    processCtx,
		processCancel: processCancel,
		restartCount:  0, // Reset restart count for this new process attempt
	}

	// Replace or Add the instance in appInfo (caller holds lock)
	foundAndReplaced := false
	for i, existing := range appInfo.instances {
		if existing.InstanceID == instanceID {
			// If replacing, inherit the restart count from the one being replaced
			appInstance.restartCount = existing.restartCount
			logger.Debug("Replacing existing state for instance in appInfo", "index", i, "inheriting_restart_count", appInstance.restartCount)
			// Cancel the context of the old instance being replaced, just in case its monitor is stuck
			if existing.processCancel != nil {
				existing.processCancel()
			}
			appInfo.instances[i] = appInstance // Replace pointer in slice
			foundAndReplaced = true
			break
		}
	}
	if !foundAndReplaced {
		appInfo.instances = append(appInfo.instances, appInstance)
		logger.Debug("Appending new instance state to appInfo")
	}

	// Publish starting status
	nr.publishStatusUpdate(instanceID, StatusStarting, nil, nil, "Starting process")

	// Start Process
	logger.Info("Starting process", "command", cmdPath, "args", spec.Args, "env_added", 4)
	if startErr := cmd.Start(); startErr != nil {
		errMsg := fmt.Sprintf("Failed to start process: %v", startErr)
		logger.Error(errMsg, "error", startErr)

		// Update status to Failed immediately IN THE NEW appInstance object
		appInstance.Status = StatusFailed
		appInstance.processCancel() // Cancel context for this failed attempt's resources

		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)

		// Return error, monitorAppInstance for the *previous* instance (if any)
		// will handle the permanent failure state update and potential removal.
		return fmt.Errorf("failed to start %s: %w", instanceID, startErr)
	}

	//  Success Path
	// Update the state after successful start in the new appInstance object
	appInstance.Pid = cmd.Process.Pid
	appInstance.StartTime = time.Now()
	appInstance.Status = StatusRunning // Mark as Running
	logger.Info("Process started successfully", "pid", appInstance.Pid)
	nr.publishStatusUpdate(instanceID, StatusRunning, &appInstance.Pid, nil, "")

	// Start Goroutines for Logs and Monitoring using the NEW app state
	appInstance.LogWg.Add(2)
	go nr.forwardLogs(appInstance, appInstance.StdoutPipe, "stdout", logger)
	go nr.forwardLogs(appInstance, appInstance.StderrPipe, "stderr", logger)
	go nr.monitorAppInstance(appInstance, logger) // Monitor this specific new instance

	return nil // Signal success
}

// stopAppInstance signals a specific application instance to terminate gracefully.
// Assumes caller holds the lock on appInfo.mu if modifications to appInfo.instances are needed after stop.
func (nr *NodeRunner) stopAppInstance(appInstance *ManagedApp, intentional bool) error {
	instanceID := appInstance.InstanceID
	logger := nr.logger.With("app", InstanceIDToAppName(instanceID), "instance_id", instanceID)

	if appInstance.Status == StatusStopped || appInstance.Status == StatusStopping || appInstance.Status == StatusFailed {
		logger.Info("Stop request: Instance not running or already stopping/stopped/failed.", "status", appInstance.Status)
		return nil // Not running or already handled
	}

	if appInstance.Cmd == nil || appInstance.Cmd.Process == nil {
		logger.Warn("Stop request: Instance state inconsistent (no command/process). Cleaning up state.")
		appInstance.Status = StatusFailed // Mark as failed due to inconsistency
		if appInstance.processCancel != nil {
			appInstance.processCancel() // Cancel context if possible
		}
		// Caller should remove this inconsistent instance from appInfo.instances
		return fmt.Errorf("inconsistent state for instance %s", instanceID)
	}

	pid := appInstance.Pid // Get PID before potentially changing status
	logger.Info("Stopping application instance process", "pid", pid)
	appInstance.Status = StatusStopping // Update status IN THE PASSED OBJECT
	nr.publishStatusUpdate(instanceID, StatusStopping, &pid, nil, "")

	// Signal monitorAppInstance goroutine to stop restarting (if applicable)
	// Ensure StopSignal is closed only once
	select {
	case <-appInstance.StopSignal: // Already closed
	default:
		close(appInstance.StopSignal)
	}

	// Attempt graceful shutdown first (SIGTERM to the process group)
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
		// ESRCH means process already exited
		if errors.Is(err, syscall.ESRCH) {
			logger.Warn("Stop request: Process already exited before SIGTERM", "pid", pid)
			// Process already exited, let monitorAppInstance handle cleanup.
			// Cancel context manually to ensure monitorAppInstance wakes up if needed.
			appInstance.processCancel()
			return nil
		}
		logger.Error("Failed to send SIGTERM", "pid", pid, "error", err)
		// Cancel context to ensure monitorAppInstance runs and sees the error/state
		appInstance.processCancel()
		return fmt.Errorf("failed to send SIGTERM to %s (PID %d): %w", instanceID, pid, err)
	}

	// Wait for process to exit or timeout (monitorAppInstance handles the actual wait)
	// We just need to ensure the context gets cancelled eventually if SIGTERM doesn't work.
	termTimer := time.NewTimer(StopTimeout)
	defer termTimer.Stop()

	select {
	case <-termTimer.C:
		// Timeout expired, force kill (SIGKILL)
		logger.Warn("Graceful shutdown timed out. Sending SIGKILL.", "pid", pid)
		if killErr := syscall.Kill(-pid, syscall.SIGKILL); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
			logger.Error("Failed to send SIGKILL", "pid", pid, "error", killErr)
		}
		// Cancel context if not already cancelled to ensure monitorAppInstance cleans up
		appInstance.processCancel()

	case <-appInstance.processCtx.Done():
		// Process exited gracefully (context cancelled by monitorAppInstance) or context was cancelled externally.
		logger.Info("Process exited or context canceled during stop wait.")
	}

	// Final cleanup of state is handled by monitorAppInstance when cmd.Wait() returns.
	// The caller (handleAppConfigUpdate) is responsible for removing the instance from appInfo.instances if needed.
	return nil
}

// forwardLogs reads from a pipe (stdout/stderr) and publishes lines to NATS.
func (nr *NodeRunner) forwardLogs(appInstance *ManagedApp, pipe io.ReadCloser, streamName string, logger *slog.Logger) {
	defer appInstance.LogWg.Done()
	defer pipe.Close() // Ensure pipe is closed when done

	scanner := bufio.NewScanner(pipe)
	scanner.Buffer(make([]byte, LogBufferSize), bufio.MaxScanTokenSize) // Use a buffer

	logger = logger.With("stream", streamName) // Logger already includes instanceID
	logger.Debug("Starting log forwarding")
	appName := InstanceIDToAppName(appInstance.InstanceID) // Extract appName

	for scanner.Scan() {
		line := scanner.Text()
		// Publish to NATS
		logMsg := logEntry{
			InstanceID: appInstance.InstanceID, // Use instanceID
			AppName:    appName,                // Include appName
			NodeID:     nr.nodeID,
			Stream:     streamName,
			Message:    line,
			Timestamp:  time.Now(),
		}
		logData, err := json.Marshal(logMsg)
		if err != nil {
			logger.Warn("Failed to marshal log entry", "error", err)
			continue // Skip this line
		}

		// Publish logs under a subject including instance ID for finer filtering?
		// Or just app name? Let's use app name for simplicity, instance ID is in payload.
		subject := fmt.Sprintf("logs.%s.%s", appName, nr.nodeID)
		// Use Publish for fire-and-forget logs
		if err := nr.nc.Publish(subject, logData); err != nil {
			// Log error but continue, maybe NATS is temporarily down
			logger.Warn("Failed to publish log entry to NATS", "subject", subject, "error", err)
		}
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrClosed) && !errors.Is(err, context.Canceled) {
		// Don't log error if context was canceled (means process exited)
		// or if the pipe was closed intentionally.
		select {
		case <-appInstance.processCtx.Done(): // Check if context was canceled
			logger.Debug("Log pipe closed due to process exit")
		default:
			logger.Error("Error reading log pipe", "error", err)
		}
	} else {
		logger.Debug("Log forwarding finished")
	}
}

// monitorAppInstance waits for a single application instance process to exit and handles restarts or cleanup.
func (nr *NodeRunner) monitorAppInstance(appInstance *ManagedApp, logger *slog.Logger) {
	instanceID := appInstance.InstanceID
	appName := InstanceIDToAppName(instanceID)

	defer func() {
		// Final cleanup of context and wait for logs
		appInstance.processCancel() // Ensure context is always cancelled on exit
		appInstance.LogWg.Wait()    // Wait for log forwarders to finish
		logger.Debug("Monitor finished and resources cleaned up")

		// Remove instance from state manager ONLY if it failed permanently or stopped cleanly/intentionally.
		// Do not remove if it crashed and might be restarted.
		if appInstance.Status == StatusFailed || appInstance.Status == StatusStopped {
			nr.removeInstanceState(appName, instanceID, logger)
		}
	}()

	logger.Info("Monitoring process instance", "pid", appInstance.Pid)
	waitErr := appInstance.Cmd.Wait() // Blocks until process exits

	exitCode := -1
	intentionalStop := false
	errMsg := ""

	// Determine if stop was intentional
	select {
	case <-appInstance.StopSignal:
		intentionalStop = true
		logger.Info("Process exit detected after intentional stop signal")
	default:
	}
	if appInstance.processCtx.Err() == context.Canceled && !intentionalStop {
		intentionalStop = true // Treat external context cancel as intentional stop
		logger.Info("Process exit detected after context cancellation")
	}

	//  Determine Exit Status
	finalStatus := StatusStopped // Assume clean stop initially
	if waitErr != nil {
		errMsg = waitErr.Error()
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
			// Check signal
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				sig := status.Signal()
				logger.Warn("Process killed by signal", "pid", appInstance.Pid, "signal", sig)
				if (sig == syscall.SIGTERM || sig == syscall.SIGINT) && intentionalStop {
					finalStatus = StatusStopped // Intentional termination
				} else {
					finalStatus = StatusCrashed // Unexpected signal or not stopped intentionally
				}
			} else {
				// Exited with non-zero code
				logger.Warn("Process exited non-zero", "pid", appInstance.Pid, "exit_code", exitCode)
				finalStatus = StatusCrashed
			}
		} else {
			// Other wait error (e.g., start failed)
			logger.Error("Process wait failed", "error", waitErr)
			finalStatus = StatusFailed
			intentionalStop = true // Don't restart on non-ExitError failures
		}
	} else {
		// Exited cleanly (exit code 0)
		exitCode = 0
		logger.Info("Process exited cleanly", "pid", appInstance.Pid)
		finalStatus = StatusStopped
	}

	// Update the instance state immediately
	appInstance.Status = finalStatus
	appInstance.lastExitCode = &exitCode
	nr.publishStatusUpdate(instanceID, finalStatus, &appInstance.Pid, &exitCode, errMsg)

	// Don't restart only if stop was intentional or it failed permanently.
	if intentionalStop || finalStatus == StatusFailed {
		logger.Info("Instance stopped intentionally or failed permanently. No restart.", "status", finalStatus)
		// State updated, cleanup via defer. Caller (handleAppConfigUpdate) might remove instance from slice.
	} else if finalStatus == StatusCrashed || finalStatus == StatusStopped { //  Treat Stopped like Crashed for restart
		//  Restart Logic
		appInfo := nr.state.GetAppInfo(appName)                           // Get current info for the app
		appInfo.mu.RLock()                                                // Lock for reading spec and current instances
		configHashMatches := appInfo.configHash == appInstance.ConfigHash // Check if spec changed since this instance started
		appInfo.mu.RUnlock()                                              // Unlock after reading

		// Find the current index of this instance (needed for restart call)
		// Note: This relies on the instance *not* being removed yet.
		replicaIndex := -1
		appInfo.mu.RLock()
		for idx, inst := range appInfo.instances {
			if inst.InstanceID == instanceID {
				replicaIndex = idx
				break
			}
		}
		appInfo.mu.RUnlock()

		if replicaIndex == -1 {
			logger.Warn("Instance disappeared from state before restart check could complete. Aborting restart.")
			return // Instance was removed, don't restart
		}

		shouldRestart := false
		if !configHashMatches {
			logger.Info("Configuration changed since instance started. No restart.", "old_hash", appInstance.ConfigHash, "new_hash", appInfo.configHash)
		} else if appInstance.restartCount >= MaxRestarts {
			logger.Error("Instance crashed too many times. Giving up.", "max_restarts", MaxRestarts)
			appInstance.Status = StatusFailed // Mark as permanently failed
			nr.publishStatusUpdate(instanceID, StatusFailed, &appInstance.Pid, &exitCode, "Exceeded max restarts")
		} else {
			shouldRestart = true
		}

		if shouldRestart {
			appInstance.restartCount++
			logger.Warn("Instance crashed. Attempting restart.", "restart_count", appInstance.restartCount, "max_restarts", MaxRestarts, "delay", RestartDelay)

			// Update status to starting *before* sleep+restart
			appInstance.Status = StatusStarting // Update local status first
			nr.publishStatusUpdate(instanceID, StatusStarting, nil, nil, fmt.Sprintf("Restarting (%d/%d)", appInstance.restartCount, MaxRestarts))

			// Delay before restarting
			restartTimer := time.NewTimer(RestartDelay)
			select {
			case <-restartTimer.C:
				// Timer expired, proceed
			case <-nr.globalCtx.Done(): // Check runner's global context
				logger.Info("Restart canceled due to runner shutdown.")
				restartTimer.Stop()
				return
			case <-appInstance.processCtx.Done(): // Should not happen here, but check
				logger.Warn("Instance context cancelled during restart delay?")
				restartTimer.Stop()
				return
			}

			//  Re-attempt start under appInfo lock
			appInfo.mu.Lock() // Lock for the start attempt
			// Double-check conditions under lock before starting
			currentConfigHash := appInfo.configHash

			instanceStillExists := false
			for _, inst := range appInfo.instances {
				if inst.InstanceID == instanceID {
					instanceStillExists = true
					break
				}
			}

			if !instanceStillExists {
				logger.Warn("Instance was removed during restart delay. Aborting restart.")
			} else if currentConfigHash != appInstance.ConfigHash {
				logger.Info("Config changed during restart delay. Aborting restart.")
			} else {
				// Conditions still met, attempt restart
				err := nr.startAppInstance(context.Background(), appInfo, replicaIndex) // Use background context for the attempt
				if err != nil {
					logger.Error("Failed to restart application instance", "error", err)
					// Update final status to Failed if restart fails
					appInstance.Status = StatusFailed // Update status again
					nr.publishStatusUpdate(instanceID, StatusFailed, nil, &exitCode, fmt.Sprintf("Restart failed: %v", err))
					// Defer will remove failed instance state later
				} else {
					logger.Info("Instance restarted successfully")
					// The new instance's monitor will take over
					// Need to exit this monitor goroutine for the OLD instance.
					appInfo.mu.Unlock() // Unlock before returning
					return
				}
			}
			appInfo.mu.Unlock() // Unlock if restart was aborted or failed
		} else {
			// Should not restart, mark as Failed if it crashed MaxRestarts times
			if appInstance.restartCount >= MaxRestarts {
				appInstance.Status = StatusFailed
				// Defer will handle removal
			}
		}
	}
}

// publishStatusUpdate sends a status update message to NATS.
func (nr *NodeRunner) publishStatusUpdate(instanceID string, status AppStatus, pid *int, exitCode *int, errMsg string) {
	currentPid := 0
	if pid != nil && (status == StatusRunning || status == StatusStopping) {
		currentPid = *pid
	}
	appName := InstanceIDToAppName(instanceID) // Extract app name

	update := statusUpdate{
		InstanceID: instanceID, // Use instanceID
		NodeID:     nr.nodeID,
		Status:     status,
		Pid:        currentPid,
		ExitCode:   exitCode,
		Error:      errMsg,
		Time:       time.Now(),
	}
	updateData, err := json.Marshal(update)
	if err != nil {
		nr.logger.Warn("Failed to marshal status update", "instance_id", instanceID, "error", err)
		return
	}

	// Publish status under a subject including instance ID? Or app name?
	// Let's use app name for general app status, instance ID is in payload.
	subject := fmt.Sprintf("status.%s.%s", appName, nr.nodeID)
	if err := nr.nc.Publish(subject, updateData); err != nil {
		nr.logger.Warn("Failed to publish status update to NATS", "instance_id", instanceID, "subject", subject, "error", err)
	}
}

// fetchAndStoreBinary downloads the binary from NATS Object Store if the specific version doesn't exist locally.
// It uses the object's digest to version the local storage path.
func (nr *NodeRunner) fetchAndStoreBinary(ctx context.Context, objectName, appName, configHash string) (string, error) {
	// configHash might be less relevant now, but keep it for context if needed later.
	logger := nr.logger.With("app", appName, "object", objectName)

	// 1. Get Object Info to determine the version (Digest)
	objInfo, getInfoErr := nr.appBinaries.GetInfo(ctx, objectName)
	if getInfoErr != nil {
		if errors.Is(getInfoErr, jetstream.ErrObjectNotFound) {
			return "", fmt.Errorf("binary object '%s' not found in object store: %w", objectName, getInfoErr)
		}
		return "", fmt.Errorf("failed to get info for binary object '%s': %w", objectName, getInfoErr)
	}

	// 2. Determine Version Identifier (Prefer Digest)
	versionID := objInfo.Digest // Format is usually "SHA-256=xxxxx"
	if versionID == "" {
		// Fallback or error? Digest is crucial for this strategy.
		logger.Error("Object store info is missing digest, cannot reliably version binary", "object", objectName)
		return "", fmt.Errorf("binary object '%s' info is missing digest", objectName)
	}
	// Extract the actual hash part if prefixed (e.g., "SHA-256=")
	if parts := strings.SplitN(versionID, "=", 2); len(parts) == 2 {
		versionID = parts[1] // Use the hash value
	}
	if versionID == "" { // Check again after potential split
		logger.Error("Object store digest value is empty after parsing", "object", objectName, "original_digest", objInfo.Digest)
		return "", fmt.Errorf("binary object '%s' digest value is empty", objectName)
	}

	safeAppName := sanitizePathElement(appName)
	safeVersionID := sanitizePathElement(versionID)
	safeObjectName := sanitizePathElement(objectName)
	versionedDir := filepath.Join(nr.dataDir, "binaries", safeAppName, safeVersionID)
	localPath := filepath.Join(versionedDir, safeObjectName)

	// 4. Check if this specific version exists locally
	_, statErr := os.Stat(localPath)
	if statErr == nil {
		// ... (verify hash of existing file - same logic as before) ...
		hashMatches, err := verifyLocalFileHash(localPath, objInfo.Digest)
		if err == nil && hashMatches {
			logger.Info("Local binary version exists and hash verified.", "path", localPath)
			return localPath, nil
		}
		logger.Warn("Local binary version exists but hash verification failed or errored. Re-downloading.", "path", localPath, "verify_err", err)
	} else if !os.IsNotExist(statErr) {
		return "", fmt.Errorf("failed to stat local binary path %s: %w", localPath, statErr)
	} else {
		logger.Info("Local binary version not found.", "path", localPath)
	}

	// 5. Download using GetObject and manual copy
	logger.Info("Downloading specific binary version using GetObject reader", "version_id", versionID, "local_path", localPath)

	if err := os.MkdirAll(versionedDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create versioned binary directory %s: %w", versionedDir, err)
	}

	// Download to temp file using io.Reader
	tempLocalPath := localPath + ".tmp" + fmt.Sprintf(".%d", time.Now().UnixNano())
	outFile, err := os.Create(tempLocalPath) // Create the temp file for writing
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file %s: %w", tempLocalPath, err)
	}
	// Ensure outfile is closed if errors occur before rename
	defer func() {
		if outFile != nil { // Check if outFile was successfully created
			outFile.Close()
			// If an error occurred before rename, remove the temp file
			if err != nil {
				logger.Warn("Removing temporary file due to error before rename", "path", tempLocalPath, "error", err)
				os.Remove(tempLocalPath)
			}
		}
	}()

	// Get the object reader
	objResult, err := nr.appBinaries.Get(ctx, objectName)
	if err != nil {
		// No need to remove temp file here, it wasn't written to yet if Get failed
		outFile.Close()          // Close the handle
		outFile = nil            // Prevent double close in defer
		os.Remove(tempLocalPath) // Remove the empty temp file
		return "", fmt.Errorf("failed to get object reader for %s: %w", objectName, err)
	}
	defer objResult.Close() // Ensure the object reader resources are closed

	// Copy data from reader to temp file
	bytesCopied, err := io.Copy(outFile, objResult)
	if err != nil {
		// Error during copy, outFile deferred close/remove will handle cleanup
		return "", fmt.Errorf("failed to copy object data to %s: %w", tempLocalPath, err)
	}
	logger.Debug("Binary data copied to temporary path", "temp_path", tempLocalPath, "bytes_copied", bytesCopied)

	if syncErr := outFile.Sync(); syncErr != nil {
		logger.Error("Failed to sync temporary file before closing", "path", tempLocalPath, "error", syncErr)
		// Proceed to close and verify anyway, but log the sync error
	}

	// Explicitly close the output file *before* verification
	if err = outFile.Close(); err != nil {
		outFile = nil // Prevent double close in defer
		// Error closing file, likely disk issue? Remove temp file.
		logger.Error("Failed to close temporary file after writing", "path", tempLocalPath, "error", err)
		os.Remove(tempLocalPath)
		return "", fmt.Errorf("failed to close temporary file %s: %w", tempLocalPath, err)
	}
	outFile = nil // Prevent double close in defer

	//  Verify checksum after download
	hashMatches, verifyErr := verifyLocalFileHash(tempLocalPath, objInfo.Digest)
	if verifyErr != nil {
		logger.Error("Failed to verify hash of downloaded binary", "path", tempLocalPath, "error", verifyErr)
		logger.Warn("Keeping temporary file due to verification error", "path", tempLocalPath)
		err = fmt.Errorf("failed to verify downloaded binary %s: %w", tempLocalPath, verifyErr) // Set error for defer cleanup
		return "", err
	}
	if !hashMatches {
		logger.Error("Hash mismatch for downloaded binary", "path", tempLocalPath, "expected_digest", objInfo.Digest)
		logger.Warn("Keeping temporary file due to hash mismatch", "path", tempLocalPath)
		err = fmt.Errorf("hash mismatch for downloaded binary %s", tempLocalPath) // Set error for defer cleanup
		return "", err
	}

	// Atomic Rename to final destination
	logger.Debug("Verification successful, attempting rename", "from", tempLocalPath, "to", localPath)
	if err = os.Rename(tempLocalPath, localPath); err != nil {
		logger.Error("Failed to rename temporary binary file to final destination", "temp", tempLocalPath, "final", localPath, "error", err)
		// Don't set global err here, defer will try to remove tempLocalPath anyway
		_ = os.Remove(tempLocalPath) // Attempt cleanup
		return "", fmt.Errorf("failed to finalize binary download for %s: %w", localPath, err)
	}

	logger.Info("Binary version downloaded and verified successfully", "path", localPath)
	return localPath, nil // Success
}

// verifyLocalFileHash calculates the SHA256 hash of the local file, encodes it
// using URL-safe Base64, and compares it with the expected digest string
// from NATS Object Store (e.g., "SHA-256=base64string").
func verifyLocalFileHash(filePath, expectedDigest string) (bool, error) {
	// Extract the expected Base64 hash value
	if !strings.HasPrefix(expectedDigest, "SHA-256=") {
		// We only support SHA-256 digests for now
		return false, fmt.Errorf("unsupported digest format (expected 'SHA-256=' prefix): %s", expectedDigest)
	}
	expectedBase64Hash := strings.TrimPrefix(expectedDigest, "SHA-256=")
	if expectedBase64Hash == "" {
		return false, fmt.Errorf("expected digest value is empty after removing prefix: %s", expectedDigest)
	}

	// Open the local file
	f, err := os.Open(filePath)
	if err != nil {
		return false, fmt.Errorf("failed to open file for hashing %s: %w", filePath, err)
	}
	defer f.Close() // Ensure file is closed

	// Calculate the SHA256 hash
	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return false, fmt.Errorf("failed to read file for hashing %s: %w", filePath, err)
	}
	hashBytes := hasher.Sum(nil) // Get the raw hash bytes

	// Encode the calculated hash bytes using URL-safe Base64 (no padding)
	actualBase64Hash := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(hashBytes)
	actualHexHash := fmt.Sprintf("%x", hashBytes) // Calculate hex too for logging

	slog.Debug("Verifying file hash",
		"file", filePath,
		"expected_base64", expectedBase64Hash,
		"calculated_base64", actualBase64Hash,
		"calculated_hex", actualHexHash)

	// Compare the calculated Base64 hash with the expected one
	match := actualBase64Hash == expectedBase64Hash

	if !match {
		slog.Warn("Hash verification failed (inside function)", // Differentiate log message
			"file", filePath,
			"expected_base64", expectedBase64Hash,
			"calculated_base64", actualBase64Hash,
			"calculated_hex", actualHexHash)
	}

	return match, nil
}

// Helper function to sanitize strings for use as path components
func sanitizePathElement(element string) string {
	// Replace potentially problematic characters like ':', '/', '\' etc.
	// Keep it simple for common cases. More robust sanitization might be needed
	// depending on expected object names and digests.
	r := strings.NewReplacer(":", "_", "/", "_", "\\", "_", "*", "_", "?", "_", "<", "_", ">", "_", "|", "_")
	sanitized := r.Replace(element)
	// Avoid overly long names (adjust limit as needed)
	if len(sanitized) > 100 {
		sanitized = sanitized[:100]
	}
	return sanitized
}

// removeInstanceState safely removes an instance from the appInfo slice.
// Assumes the monitorAppInstance defer function calls this.
func (nr *NodeRunner) removeInstanceState(appName, instanceID string, logger *slog.Logger) {
	appInfo := nr.state.GetAppInfo(appName)
	appInfo.mu.Lock()
	defer appInfo.mu.Unlock()

	newInstances := make([]*ManagedApp, 0, len(appInfo.instances))
	found := false
	for _, instance := range appInfo.instances {
		if instance.InstanceID == instanceID {
			found = true
		} else {
			newInstances = append(newInstances, instance)
		}
	}

	if found {
		appInfo.instances = newInstances
		logger.Info("Removed instance state.", "count_after_remove", len(newInstances))
	} else {
		logger.Warn("Attempted to remove instance state, but it was not found.")
	}
}
