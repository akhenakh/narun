package noderunner

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	RestartDelay    = 5 * time.Second  // Delay before restarting a crashed app
	MaxRestarts     = 5                // Max restarts before giving up (per config update)
	StopTimeout     = 10 * time.Second // Time to wait for graceful shutdown before SIGKILL
	LogBufferSize   = 1024             // Buffer size for log lines
	StatusQueueSize = 10               // Buffer size for status update channel
)

type statusUpdate struct {
	AppName  string    `json:"app_name"`
	NodeID   string    `json:"node_id"`
	Status   AppStatus `json:"status"`
	Pid      int       `json:"pid,omitempty"`
	ExitCode *int      `json:"exit_code,omitempty"` // Use pointer to distinguish 0 from unset
	Error    string    `json:"error,omitempty"`
	Time     time.Time `json:"time"`
}

type logEntry struct {
	AppName   string    `json:"app_name"`
	NodeID    string    `json:"node_id"`
	Stream    string    `json:"stream"` // "stdout" or "stderr"
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
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

// startApp attempts to download the binary, configure, and start the application process.
func (nr *NodeRunner) startApp(ctx context.Context, spec *ServiceSpec, existingApp *ManagedApp) error {
	appName := spec.Name
	logger := nr.logger.With("app", appName)
	logger.Info("Attempting to start application")

	// Calculate Config Hash
	newConfigHash, err := calculateSpecHash(spec)
	if err != nil {
		logger.Error("Failed to calculate config hash", "error", err)
		return fmt.Errorf("failed to hash config for %s: %w", appName, err)
	}

	// Check if restart/update is needed AND handle state correctly
	proceedWithStart := true // Assume we need to start by default

	if existingApp != nil {
		isTerminalState := existingApp.Status == StatusStopped || existingApp.Status == StatusFailed || existingApp.Status == StatusCrashed

		// Check if the app is *not* in a final/terminal state
		if !isTerminalState { // <--- Use isTerminalState
			// App is in a non-terminal state (Running, Stopping, Starting)

			// Check if config hash is the same
			if existingApp.ConfigHash == newConfigHash {
				// Config is the same. Only bail out if the status is NOT StatusStarting.
				// If it IS StatusStarting, it means monitorApp called us for a restart, so we MUST proceed.
				if existingApp.Status != StatusStarting {
					logger.Info("Application already running/stopping with the same configuration. No action needed.", "status", existingApp.Status, "hash", newConfigHash)
					proceedWithStart = false // Don't proceed
				} else {
					// Status IS Starting - This is the restart call from monitorApp. Proceed.
					logger.Debug("Proceeding with startApp during restart (status was Starting)", "hash", newConfigHash)
				}
			} else {
				// Config changed. Need to stop the old one first.
				logger.Info("Configuration changed. Stopping existing instance.", "old_hash", existingApp.ConfigHash, "new_hash", newConfigHash, "status", existingApp.Status)
				if err := nr.stopApp(appName, true); err != nil { // Signal intentional stop
					logger.Error("Failed to stop existing instance before update", "error", err)
					// Proceed with caution, maybe return error? For now, log and continue.
				}
				// Reset restart count since config changed
				existingApp.restartCount = 0 // Note: This modification might be lost as we create a new 'app' later, restart count logic relies on copy below
			}
		} else { // <--- Use isTerminalState
			// App is in a terminal state (Stopped, Failed, Crashed).
			// Check if config hash changed while it was stopped/crashed.
			if existingApp.ConfigHash != newConfigHash {
				logger.Info("Config changed while app was stopped/failed/crashed. Resetting restart count.", "old_hash", existingApp.ConfigHash, "new_hash", newConfigHash)
				// No need to modify existingApp.restartCount here, the new 'app' below will start at 0
			}
			// Always proceed to start if it was in a terminal state.
		}
	}

	// Exit early if check determined no action needed
	if !proceedWithStart {
		return nil
	}

	// If we proceed, create a NEW ManagedApp state for this run
	// Fetch and Store Binary
	binaryPath, err := nr.fetchAndStoreBinary(ctx, spec.BinaryObject, appName, newConfigHash)
	if err != nil {
		return err
	}
	logger.Info("Binary downloaded and stored", "path", binaryPath)

	// Prepare Command
	if err := os.Chmod(binaryPath, 0755); err != nil {
		logger.Error("Failed to make binary executable", "path", binaryPath, "error", err)
		nr.publishStatusUpdate(appName, StatusFailed, nil, nil, fmt.Sprintf("Binary not executable: %v", err))
		return fmt.Errorf("failed making binary executable %s: %w", binaryPath, err)
	}

	processCtx, processCancel := context.WithCancel(ctx) // Use the passed context
	cmdPath := binaryPath
	cmd := exec.CommandContext(processCtx, cmdPath, spec.Args...)
	cmd.Env = os.Environ()
	for _, envVar := range spec.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", envVar.Name, envVar.Value))
	}

	// Explicitly create app base directory FIRST
	appBaseDir := filepath.Join(nr.dataDir, appName)
	if err := os.MkdirAll(appBaseDir, 0755); err != nil {
		processCancel() // Cancel context before returning
		logger.Error("Failed to create application base directory", "dir", appBaseDir, "error", err)
		nr.publishStatusUpdate(appName, StatusFailed, nil, nil, fmt.Sprintf("Failed base dir: %v", err))
		return fmt.Errorf("failed to create base dir %s for app %s: %w", appBaseDir, appName, err)
	}
	logger.Debug("Ensured application base directory exists", "dir", appBaseDir)

	// Now create the working directory INSIDE the app base directory
	workDir := filepath.Join(appBaseDir, "work")
	if err := os.MkdirAll(workDir, 0755); err != nil {
		processCancel()
		logger.Error("Failed to create working directory", "dir", workDir, "error", err)
		nr.publishStatusUpdate(appName, StatusFailed, nil, nil, fmt.Sprintf("Failed work dir: %v", err))
		return fmt.Errorf("failed to create work dir %s for app %s: %w", workDir, appName, err)
	}
	cmd.Dir = workDir // Set the working directory for the command
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		processCancel()
		logger.Error("Failed to get stdout pipe", "error", err)
		return fmt.Errorf("stdout pipe failed for %s: %w", appName, err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		processCancel()
		stdoutPipe.Close()
		logger.Error("Failed to get stderr pipe", "error", err)
		return fmt.Errorf("stderr pipe failed for %s: %w", appName, err)
	}

	// Create NEW ManagedApp State
	app := &ManagedApp{
		Spec:          spec,
		Cmd:           cmd,
		Status:        StatusStarting,
		ConfigHash:    newConfigHash,
		BinaryPath:    binaryPath,
		StdoutPipe:    stdoutPipe,
		StderrPipe:    stderrPipe,
		StopSignal:    make(chan struct{}),
		processCtx:    processCtx,
		processCancel: processCancel,
		restartCount:  0, // Default to 0 for new state
	}

	if existingApp != nil && existingApp.ConfigHash == newConfigHash {
		app.restartCount = existingApp.restartCount
		logger.Debug("Carrying over restart count", "count", app.restartCount)
	}

	// Update the central state with the *new* ManagedApp object
	nr.state.Set(appName, app)
	// Publish starting status immediately for the new attempt
	nr.publishStatusUpdate(appName, StatusStarting, nil, nil, "Starting process")

	// Start Process
	logger.Info("Starting process", "command", cmdPath, "args", spec.Args, "restart_count", app.restartCount)
	if startErr := cmd.Start(); startErr != nil {
		errMsg := fmt.Sprintf("Failed to start process: %v", startErr)
		logger.Error(errMsg, "error", startErr) // Log the specific error

		// Update the newly created app state to Failed
		app.Status = StatusFailed
		app.processCancel()                               // Cancel context for this failed attempt's resources
		nr.state.UpdateStatus(appName, StatusFailed, nil) // Update central status map

		// Publish the failure status
		nr.publishStatusUpdate(appName, StatusFailed, nil, nil, errMsg)

		// Return a wrapped error to the caller (handleAppConfigUpdate)
		return fmt.Errorf("failed to start %s: %w", appName, startErr)
	}

	// Update the new app state after successful start
	app.Pid = cmd.Process.Pid
	app.StartTime = time.Now()
	app.Status = StatusRunning
	nr.state.UpdateStatus(appName, StatusRunning, &app.Pid) // Update map status
	logger.Info("Process started successfully", "pid", app.Pid)
	nr.publishStatusUpdate(appName, StatusRunning, &app.Pid, nil, "")

	// Start Goroutines for Logs and Monitoring using the *new* app state
	app.LogWg.Add(2)
	go nr.forwardLogs(app, app.StdoutPipe, "stdout", logger)
	go nr.forwardLogs(app, app.StderrPipe, "stderr", logger)
	go nr.monitorApp(app, logger) // Monitor the new app instance

	return nil
}

// stopApp signals the application process to terminate gracefully, then forcefully if necessary.
func (nr *NodeRunner) stopApp(appName string, intentional bool) error {
	logger := nr.logger.With("app", appName)
	app, exists := nr.state.Get(appName)
	if !exists || app.Status == StatusStopped || app.Status == StatusStopping {
		logger.Info("Stop request: App not running or already stopping.")
		return nil // Not running or already stopped/stopping
	}

	if app.Cmd == nil || app.Cmd.Process == nil {
		logger.Warn("Stop request: App state inconsistent (no command/process). Cleaning up state.")
		nr.state.Delete(appName)
		// Cancel context if it exists
		if app.processCancel != nil {
			app.processCancel()
		}
		return nil
	}

	logger.Info("Stopping application process", "pid", app.Pid)
	nr.state.UpdateStatus(appName, StatusStopping, nil)
	nr.publishStatusUpdate(appName, StatusStopping, &app.Pid, nil, "")

	// Signal monitorApp goroutine to stop restarting (if applicable)
	if !intentional {
		// If not intentional, signal that we are stopping it now
		close(app.StopSignal)
	} else if app.StopSignal != nil {
		// If intentional (e.g. config update/delete), ensure StopSignal is closed
		// Check for nil first in case it was already closed by monitorApp on exit
		select {
		case <-app.StopSignal: // Already closed
		default:
			close(app.StopSignal)
		}
	}

	// Attempt graceful shutdown first (SIGTERM)
	// Use negative PID to signal the entire process group
	if err := syscall.Kill(-app.Pid, syscall.SIGTERM); err != nil {
		// ESRCH means process already exited
		if err == syscall.ESRCH {
			logger.Warn("Stop request: Process already exited before SIGTERM", "pid", app.Pid)
			// Process already exited, let monitorApp handle cleanup.
			// Cancel context manually to ensure monitorApp wakes up if needed.
			app.processCancel()
			return nil
		}
		logger.Error("Failed to send SIGTERM", "pid", app.Pid, "error", err)
		// Proceed to SIGKILL? Maybe not, let monitorApp see the state.
		// Cancel context to ensure monitorApp runs
		app.processCancel()
		return fmt.Errorf("failed to send SIGTERM to %s (PID %d): %w", appName, app.Pid, err)
	}

	// Wait for process to exit or timeout
	termTimer := time.NewTimer(StopTimeout)
	defer termTimer.Stop()
	waitChan := make(chan error, 1)
	go func() {
		// We don't call cmd.Wait() here because monitorApp is already doing that.
		// Instead, we rely on monitorApp detecting the exit or the context cancellation.
		// If monitorApp hasn't detected exit after timeout, we force kill.
		<-app.processCtx.Done() // Wait for context cancellation (by monitorApp or us)
		waitChan <- app.processCtx.Err()
	}()

	select {
	case <-termTimer.C:
		// Timeout expired, force kill (SIGKILL)
		logger.Warn("Graceful shutdown timed out. Sending SIGKILL.", "pid", app.Pid)
		if killErr := syscall.Kill(-app.Pid, syscall.SIGKILL); killErr != nil && killErr != syscall.ESRCH {
			logger.Error("Failed to send SIGKILL", "pid", app.Pid, "error", killErr)
			// Even if SIGKILL fails, let monitorApp clean up state
		}
		// Cancel context if not already cancelled to ensure monitorApp cleans up
		app.processCancel()

	case err := <-waitChan:
		// Process exited gracefully or context was canceled by monitorApp
		if err != nil && err != context.Canceled {
			logger.Debug("Process wait ended during stop", "error", err) // e.g. context.DeadlineExceeded shouldn't happen here
		} else {
			logger.Info("Process exited gracefully after SIGTERM or was already exiting.")
		}

	}

	// Final cleanup of state is handled by monitorApp when cmd.Wait() returns.
	return nil
}

// forwardLogs reads from a pipe (stdout/stderr) and publishes lines to NATS.
func (nr *NodeRunner) forwardLogs(app *ManagedApp, pipe io.ReadCloser, streamName string, logger *slog.Logger) {
	defer app.LogWg.Done()
	defer pipe.Close() // Ensure pipe is closed when done

	scanner := bufio.NewScanner(pipe)
	scanner.Buffer(make([]byte, LogBufferSize), bufio.MaxScanTokenSize) // Use a buffer

	logger = logger.With("stream", streamName)
	logger.Debug("Starting log forwarding")

	for scanner.Scan() {
		line := scanner.Text()
		// Publish to NATS
		logMsg := logEntry{
			AppName:   app.Spec.Name,
			NodeID:    nr.nodeID,
			Stream:    streamName,
			Message:   line,
			Timestamp: time.Now(),
		}
		logData, err := json.Marshal(logMsg)
		if err != nil {
			logger.Warn("Failed to marshal log entry", "error", err)
			continue // Skip this line
		}

		subject := fmt.Sprintf("logs.%s.%s", app.Spec.Name, nr.nodeID)
		// Use Publish for fire-and-forget logs
		if err := nr.nc.Publish(subject, logData); err != nil {
			// Log error but continue, maybe NATS is temporarily down
			logger.Warn("Failed to publish log entry to NATS", "subject", subject, "error", err)
		}

		// Optional: Log locally as well? Might be too noisy.
		// logger.Debug("Forwarded log", "line", line)
	}

	if err := scanner.Err(); err != nil && err != io.EOF && err != os.ErrClosed && err != context.Canceled {
		// Don't log error if context was canceled (means process exited)
		// or if the pipe was closed intentionally.
		select {
		case <-app.processCtx.Done(): // Check if context was canceled
			logger.Debug("Log pipe closed due to process exit")
		default:
			logger.Error("Error reading log pipe", "error", err)
		}
	} else {
		logger.Debug("Log forwarding finished")
	}
}

// monitorApp waits for the application process to exit and handles restarts or cleanup.
func (nr *NodeRunner) monitorApp(app *ManagedApp, logger *slog.Logger) {
	defer func() {
		// Final cleanup of pipes and context regardless of exit path
		// Pipes are closed by forwardLogs, but cancel context here.
		app.processCancel()
		// Wait for log forwarders to finish
		app.LogWg.Wait()
		logger.Debug("Monitor finished and resources cleaned up")
	}()

	logger.Info("Monitoring process", "pid", app.Pid)
	waitErr := app.Cmd.Wait() // Blocks until process exits

	exitCode := -1
	intentionalStop := false
	errMsg := "" // Initialize error message string

	// Check if the process was intentionally stopped
	select {
	case <-app.StopSignal:
		intentionalStop = true
		logger.Info("Process exit detected after intentional stop signal")
	default:
		// Not intentionally stopped (or signal not yet processed)
	}

	// Check if the context was canceled (could be from stopApp or external)
	if app.processCtx.Err() == context.Canceled && !intentionalStop {
		// If context cancelled but not via our StopSignal, treat as intentional external stop
		intentionalStop = true
		logger.Info("Process exit detected after context cancellation")
	}

	if waitErr != nil {
		errMsg = waitErr.Error() // <<< Get error message only if waitErr is not nil
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
			logger.Warn("Process exited with error", "pid", app.Pid, "exit_code", exitCode, "error", waitErr)
			// Check if killed by signal
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				if status.Signaled() {
					logger.Warn("Process killed by signal", "signal", status.Signal())
					// If killed by SIGTERM or SIGINT, might be graceful shutdown
					if status.Signal() == syscall.SIGTERM || status.Signal() == syscall.SIGINT {
						// Treat SIGTERM/SIGINT as intentional if StopSignal was closed
						if intentionalStop {
							logger.Info("Process terminated by SIGTERM/SIGINT after stop signal.")
							// Keep status Stopping if that was set, otherwise Stopped
							if app.Status != StatusStopping {
								app.Status = StatusStopped
							}
						} else {
							// Treat as crash if not explicitly stopped
							logger.Warn("Process terminated by SIGTERM/SIGINT unexpectedly.")
							app.Status = StatusCrashed
						}
					} else {
						// Other signals usually mean a crash
						logger.Warn("Process killed by unexpected signal", "signal", status.Signal())
						app.Status = StatusCrashed
					}
				} else {
					// Exited with non-zero code
					logger.Warn("Process exited with non-zero status code", "exit_code", exitCode)
					app.Status = StatusCrashed
				}
			} else {
				// Unknown exit error type, likely crashed
				logger.Warn("Process exited with unknown error type", "error", waitErr)
				app.Status = StatusCrashed
			}
		} else {
			// Error other than ExitError (e.g., command not found, permission error during Start)
			logger.Error("Process wait failed with non-ExitError", "error", waitErr)
			app.Status = StatusFailed
			intentionalStop = true // Treat Start-related errors as non-restartable
		}
	} else {
		// Exited cleanly (exit code 0)
		exitCode = 0
		logger.Info("Process exited cleanly", "pid", app.Pid, "exit_code", exitCode)
		app.Status = StatusStopped // Mark as stopped cleanly
		errMsg = ""                // Ensure error message is empty for clean exit
	}

	// Update state map immediately
	exitCodePtr := &exitCode // Take address for pointer

	nr.publishStatusUpdate(app.Spec.Name, app.Status, &app.Pid, exitCodePtr, errMsg)

	// Handle Cleanup / Restart Logic
	if intentionalStop || app.Status == StatusFailed || app.Status == StatusStopped {
		logger.Info("Process stopped intentionally, failed permanently, or exited cleanly. Cleaning up.", "status", app.Status)
		// State already updated, pipes/context cleaned up by defer
	} else if app.Status == StatusCrashed {
		// Restart logic for crashed apps
		if app.restartCount < MaxRestarts {
			app.restartCount++
			logger.Warn("Process crashed. Attempting restart.", "restart_count", app.restartCount, "max_restarts", MaxRestarts, "delay", RestartDelay)
			// Update status to starting before sleep+restart
			nr.state.UpdateStatus(app.Spec.Name, StatusStarting, nil)
			nr.publishStatusUpdate(app.Spec.Name, StatusStarting, nil, nil, fmt.Sprintf("Restarting (%d/%d)", app.restartCount, MaxRestarts))

			// Use a timer bound by the global context for the delay
			restartTimer := time.NewTimer(RestartDelay)
			select {
			case <-restartTimer.C:
				// Timer expired, proceed with restart
			case <-nr.globalCtx.Done():
				logger.Info("Restart canceled due to runner shutdown.", "app", app.Spec.Name)
				restartTimer.Stop() // Clean up timer
				return              // Don't attempt restart if runner is shutting down
			}

			// Re-fetch the spec from state in case it changed while sleeping
			currentApp, exists := nr.state.Get(app.Spec.Name)
			if !exists || currentApp.ConfigHash != app.ConfigHash {
				logger.Info("Config changed or app deleted while waiting to restart. Aborting restart.", "app", app.Spec.Name)
				return // Don't restart if config changed or app was deleted
			}
			// Pass the existing app state (with updated restart count) to startApp
			err := nr.startApp(context.Background(), app.Spec, currentApp) // Use background context for restart attempt
			if err != nil {
				logger.Error("Failed to restart application", "app", app.Spec.Name, "error", err)
				// Status should have been updated to Failed by the startApp attempt
			}
		} else {
			logger.Error("Process crashed too many times. Giving up.", "app", app.Spec.Name, "max_restarts", MaxRestarts)
			nr.state.UpdateStatus(app.Spec.Name, StatusFailed, exitCodePtr) // Mark as permanently failed
			nr.publishStatusUpdate(app.Spec.Name, StatusFailed, &app.Pid, exitCodePtr, "Exceeded max restarts")
		}
	}
}

// publishStatusUpdate sends a status update message to NATS.
func (nr *NodeRunner) publishStatusUpdate(appName string, status AppStatus, pid *int, exitCode *int, errMsg string) {
	// Make sure pid is non-nil only if status implies a running process
	currentPid := 0
	if pid != nil && (status == StatusRunning || status == StatusStopping) {
		currentPid = *pid
	}

	update := statusUpdate{
		AppName:  appName,
		NodeID:   nr.nodeID,
		Status:   status,
		Pid:      currentPid,
		ExitCode: exitCode, // Pass pointer directly
		Error:    errMsg,
		Time:     time.Now(),
	}
	updateData, err := json.Marshal(update)
	if err != nil {
		nr.logger.Warn("Failed to marshal status update", "app", appName, "error", err)
		return
	}

	subject := fmt.Sprintf("status.%s.%s", appName, nr.nodeID)
	if err := nr.nc.Publish(subject, updateData); err != nil {
		nr.logger.Warn("Failed to publish status update to NATS", "app", appName, "subject", subject, "error", err)
	}
}

// fetchAndStoreBinary downloads the binary from NATS Object Store if it doesn't exist locally or hash differs.
func (nr *NodeRunner) fetchAndStoreBinary(ctx context.Context, objectName, appName, configHash string) (string, error) {
	// Store binaries in <dataDir>/<appName>/<configHash>/<binaryName>
	// Using configHash ensures we fetch a new binary if the config *pointing* to it changes,
	// even if the binary object itself hasn't changed. This might be too aggressive.
	// Alternative: Use binary object's digest/version if available.
	// Let's use a simpler <dataDir>/binaries/<objectName> for now. Hash check can be on metadata later.
	// localDir := filepath.Join(nr.dataDir, "binaries")
	// We need the *config* hash to decide if we *need* to re-evaluate the binary,
	// but the binary *itself* might not change. Storing by object name is simpler.
	localDir := filepath.Join(nr.dataDir, "binaries")
	localPath := filepath.Join(localDir, objectName) // Use object name for the local file name

	if err := os.MkdirAll(localDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create binary directory %s: %w", localDir, err)
	}

	// Check if file exists locally
	localInfo, err := os.Stat(localPath)
	if err == nil {
		// File exists, potentially check metadata against object store?
		// For simplicity, assume local copy is fine if it exists. Add hash/metadata check later.
		nr.logger.Debug("Binary already exists locally", "path", localPath)

		// Get object store info to compare (optional enhancement)
		objInfo, getInfoErr := nr.appBinaries.GetInfo(ctx, objectName)
		if getInfoErr == nil {
			// Compare modification time or hash if available
			// For example, compare os.FileInfo ModTime with objInfo.ModTime
			// Or better, compare objInfo.Digest if available and hash the local file
			if localInfo.ModTime().Before(objInfo.ModTime) {
				nr.logger.Info("Local binary is older than object store version. Re-downloading.", "local_mtime", localInfo.ModTime(), "remote_mtime", objInfo.ModTime)
				// Continue to download below
			} else {
				nr.logger.Debug("Local binary seems up-to-date.", "path", localPath)
				return localPath, nil // Assume local is good
			}
		} else {
			nr.logger.Warn("Could not get object store info to verify local binary", "object", objectName, "error", getInfoErr)
			// Proceed assuming local is okay if GetInfo failed
			return localPath, nil
		}
	} else if !os.IsNotExist(err) {
		// Error other than not existing
		return "", fmt.Errorf("failed to stat local binary path %s: %w", localPath, err)
	}

	// File doesn't exist locally or needs update, download it
	nr.logger.Info("Downloading binary from object store", "object", objectName, "local_path", localPath)
	err = nr.appBinaries.GetFile(ctx, objectName, localPath) // GetFile overwrites if exists
	if err != nil {
		// Attempt to remove potentially partial file on error
		_ = os.Remove(localPath)
		return "", fmt.Errorf("failed to download binary %s: %w", objectName, err)
	}

	return localPath, nil
}
