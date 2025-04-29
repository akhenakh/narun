package noderunner

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3" // Use v3 consistently if config uses it
)

const (
	RestartDelay    = 2 * time.Second  // Delay before restarting a crashed app
	MaxRestarts     = 5                // Max restarts before giving up (per instance)
	StopTimeout     = 10 * time.Second // Time to wait for graceful shutdown before SIGKILL
	LogBufferSize   = 1024             // Buffer size for log lines
	StatusQueueSize = 10               // Buffer size for status update channel
)

type statusUpdate struct {
	InstanceID string    `json:"instance_id"`
	NodeID     string    `json:"node_id"`
	Status     AppStatus `json:"status"`
	Pid        int       `json:"pid,omitempty"`
	ExitCode   *int      `json:"exit_code,omitempty"` // Use pointer to distinguish 0 from unset
	Error      string    `json:"error,omitempty"`
	Time       time.Time `json:"time"`
}

type logEntry struct {
	InstanceID string    `json:"instance_id"` // Instance ID as it will appear in logs
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
func (nr *NodeRunner) startAppInstance(ctx context.Context, appInfo *appInfo, replicaIndex int) error {
	spec := appInfo.spec // Use the spec from appInfo
	if spec == nil {
		return fmt.Errorf("cannot start instance, app spec is nil for %s", appInfo.configHash /* Use hash or appName */)
	}
	appName := spec.Name
	tag := spec.Tag // Get the tag
	instanceID := GenerateInstanceID(appName, replicaIndex)
	logger := nr.logger.With("app", appName, "instance_id", instanceID, "tag", tag)
	logger.Info("Attempting to start application instance")

	configHash := appInfo.configHash // Get hash from locked appInfo

	// Check if an instance with this ID exists and is truly RUNNING.
	instanceExists := false
	for _, existingInstance := range appInfo.instances {
		if existingInstance.InstanceID == instanceID {
			instanceExists = true
			if existingInstance.Status == StatusRunning {
				logger.Warn("Attempted to start instance that is already running.", "status", existingInstance.Status, "pid", existingInstance.Pid)
				return fmt.Errorf("instance %s already running with pid %d", instanceID, existingInstance.Pid)
			}
			logger.Debug("Found existing non-running instance state, proceeding to replace.", "status", existingInstance.Status)
			break
		}
	}

	// Fetch and Store Binary ** using tag and local platform **
	binaryPath, err := nr.fetchAndStoreBinary(ctx, tag, appName, configHash)
	if err != nil {
		errMsg := fmt.Sprintf("Binary fetch failed for %s/%s: %v", nr.localOS, nr.localArch, err)
		logger.Error(errMsg, "error", err) // Log error with platform info
		if !instanceExists {
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
		}
		return fmt.Errorf("failed to fetch binary for %s (%s/%s): %w", instanceID, nr.localOS, nr.localArch, err)
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

	cmdPath := binaryPath // Use the fetched binary path
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
	cmd.Env = append(cmd.Env, fmt.Sprintf("NARUN_NODE_OS=%s", nr.localOS))
	cmd.Env = append(cmd.Env, fmt.Sprintf("NARUN_NODE_ARCH=%s", nr.localArch))

	// Directory Setup
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
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

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
		stdoutPipe.Close()
		logger.Error("Failed to get stderr pipe", "error", err)
		if !instanceExists {
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("stderr pipe failed: %v", err))
		}
		return fmt.Errorf("stderr pipe failed for %s: %w", instanceID, err)
	}

	appInstance := &ManagedApp{
		InstanceID: instanceID, Spec: spec, Cmd: cmd, Status: StatusStarting, ConfigHash: configHash,
		BinaryPath: binaryPath, StdoutPipe: stdoutPipe, StderrPipe: stderrPipe,
		StopSignal: make(chan struct{}), processCtx: processCtx, processCancel: processCancel, restartCount: 0,
	}

	foundAndReplaced := false
	for i, existing := range appInfo.instances {
		if existing.InstanceID == instanceID {
			appInstance.restartCount = existing.restartCount
			logger.Debug("Replacing existing state for instance in appInfo", "index", i, "inheriting_restart_count", appInstance.restartCount)
			if existing.processCancel != nil {
				existing.processCancel()
			}
			appInfo.instances[i] = appInstance
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
	logger.Info("Starting process", "command", cmdPath, "args", spec.Args, "env_added", 6) // Env count updated
	if startErr := cmd.Start(); startErr != nil {
		errMsg := fmt.Sprintf("Failed to start process: %v", startErr)
		logger.Error(errMsg, "error", startErr)
		appInstance.Status = StatusFailed
		appInstance.processCancel()
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
		return fmt.Errorf("failed to start %s: %w", instanceID, startErr)
	}

	// Success Path
	appInstance.Pid = cmd.Process.Pid
	appInstance.StartTime = time.Now()
	appInstance.Status = StatusRunning
	logger.Info("Process started successfully", "pid", appInstance.Pid)
	nr.publishStatusUpdate(instanceID, StatusRunning, &appInstance.Pid, nil, "")
	appInstance.LogWg.Add(2)
	go nr.forwardLogs(appInstance, appInstance.StdoutPipe, "stdout", logger)
	go nr.forwardLogs(appInstance, appInstance.StderrPipe, "stderr", logger)
	go nr.monitorAppInstance(appInstance, logger)

	return nil // Signal success
}

// stopAppInstance gracefully stops an application instance process.
func (nr *NodeRunner) stopAppInstance(appInstance *ManagedApp, intentional bool) error {
	instanceID := appInstance.InstanceID
	logger := nr.logger.With("app", InstanceIDToAppName(instanceID), "instance_id", instanceID)

	if appInstance.Status == StatusStopped || appInstance.Status == StatusStopping || appInstance.Status == StatusFailed {
		logger.Info("Stop request: Instance not running or already stopping/stopped/failed.", "status", appInstance.Status)
		return nil
	}
	if appInstance.Cmd == nil || appInstance.Cmd.Process == nil {
		logger.Warn("Stop request: Instance state inconsistent (no command/process). Cleaning up state.")
		appInstance.Status = StatusFailed // Mark as failed if state is broken
		if appInstance.processCancel != nil {
			appInstance.processCancel() // Ensure context is cancelled
		}
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, "Inconsistent state during stop") // Publish failure
		return fmt.Errorf("inconsistent state for instance %s", instanceID)
	}

	pid := appInstance.Pid
	logger.Info("Stopping application instance process", "pid", pid)
	appInstance.Status = StatusStopping
	nr.publishStatusUpdate(instanceID, StatusStopping, &pid, nil, "")

	// Close stop signal channel if not already closed
	select {
	case <-appInstance.StopSignal:
	default:
		close(appInstance.StopSignal)
	}

	// Send SIGTERM to the process group
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			logger.Warn("Stop request: Process already exited before SIGTERM", "pid", pid)
			appInstance.processCancel() // Ensure monitor wakes up
			return nil                  // Not an error if already gone
		}
		// Unexpected error sending signal
		logger.Error("Failed to send SIGTERM", "pid", pid, "error", err)
		appInstance.processCancel() // Cancel context anyway
		// Don't necessarily mark as failed yet, monitor loop will handle final state
		return fmt.Errorf("failed to send SIGTERM to %s (PID %d): %w", instanceID, pid, err)
	}

	// Wait for graceful shutdown or timeout
	termTimer := time.NewTimer(StopTimeout)
	defer termTimer.Stop()
	select {
	case <-termTimer.C:
		logger.Warn("Graceful shutdown timed out. Sending SIGKILL.", "pid", pid)
		// Send SIGKILL to the process group
		if killErr := syscall.Kill(-pid, syscall.SIGKILL); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
			logger.Error("Failed to send SIGKILL", "pid", pid, "error", killErr)
			// Process might still be running, but we tried our best. Monitor will handle exit.
		}
		appInstance.processCancel() // Ensure context is cancelled if timeout occurs
	case <-appInstance.processCtx.Done():
		logger.Info("Process exited or context canceled during stop wait.")
		// Process exited, maybe cleanly or due to SIGTERM
	}
	return nil // Signal that stop attempt was made
}

// forwardLogs reads logs from a pipe and publishes them to NATS.
func (nr *NodeRunner) forwardLogs(appInstance *ManagedApp, pipe io.ReadCloser, streamName string, logger *slog.Logger) {
	defer appInstance.LogWg.Done()
	defer pipe.Close()
	scanner := bufio.NewScanner(pipe)
	// Use a reasonable buffer size, potentially adjustable via config later
	const initialBufSize = 4 * 1024
	const maxBufSize = 64 * 1024 // Max line size to handle
	buf := make([]byte, initialBufSize)
	scanner.Buffer(buf, maxBufSize)

	logger = logger.With("stream", streamName)
	logger.Debug("Starting log forwarding")
	appName := InstanceIDToAppName(appInstance.InstanceID)
	subject := fmt.Sprintf("%s.%s.%s", LogSubjectPrefix, appName, nr.nodeID)

	for scanner.Scan() {
		line := scanner.Text()
		logMsg := logEntry{InstanceID: appInstance.InstanceID, AppName: appName, NodeID: nr.nodeID, Stream: streamName, Message: line, Timestamp: time.Now()}
		logData, err := json.Marshal(logMsg)
		if err != nil {
			logger.Warn("Failed to marshal log entry", "error", err)
			continue
		}
		// Use Publish for fire-and-forget logging
		if err := nr.nc.Publish(subject, logData); err != nil {
			// Log warning but continue trying to forward logs
			logger.Warn("Failed to publish log entry to NATS", "subject", subject, "error", err)
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrClosed) && !errors.Is(err, context.Canceled) {
		// Check if context was cancelled (process exited) vs. a real read error
		select {
		case <-appInstance.processCtx.Done():
			logger.Debug("Log pipe closed due to process exit")
		default:
			logger.Error("Error reading log pipe", "error", err)
		}
	} else {
		logger.Debug("Log forwarding finished")
	}
}

// monitorAppInstance waits for an app instance to exit and handles restarts or cleanup.
func (nr *NodeRunner) monitorAppInstance(appInstance *ManagedApp, logger *slog.Logger) {
	instanceID := appInstance.InstanceID
	appName := InstanceIDToAppName(instanceID)

	// Ensure resources are cleaned up when monitor exits
	defer func() {
		// Ensure context is always cancelled on exit
		appInstance.processCancel()
		// Wait for log forwarders to finish
		appInstance.LogWg.Wait()
		logger.Debug("Monitor finished and log forwarders completed")
		// Remove instance state ONLY if it's terminally failed or intentionally stopped AND no restart is pending
		if appInstance.Status == StatusFailed || appInstance.Status == StatusStopped {
			// Check if a restart was suppressed due to external factors
			select {
			case <-appInstance.StopSignal: // Intentional stop signal received
			case <-nr.globalCtx.Done(): // Runner is shutting down
			default:
				// If neither stop signal nor global shutdown is active, and status is stopped/failed, remove state.
				// This condition is a bit complex, maybe simplify? Let's remove if failed or stopped *unintentionally*.
				if appInstance.Status == StatusFailed || (appInstance.Status == StatusStopped && appInstance.lastExitCode != nil && *appInstance.lastExitCode != 0) {
					nr.removeInstanceState(appName, instanceID, logger)
				}
			}
		}
	}()

	logger.Info("Monitoring process instance", "pid", appInstance.Pid)
	waitErr := appInstance.Cmd.Wait()

	// Determine exit details
	exitCode := -1
	intentionalStop := false
	errMsg := ""

	// Check if an intentional stop was signalled BEFORE Wait() returned
	select {
	case <-appInstance.StopSignal:
		intentionalStop = true
		logger.Info("Process exit detected after intentional stop signal")
	default:
		// No intentional stop signal before Wait() returned
	}

	// If context was cancelled, assume intentional stop unless already signalled otherwise
	if appInstance.processCtx.Err() == context.Canceled && !intentionalStop {
		intentionalStop = true
		logger.Info("Process exit detected after context cancellation")
	}

	// Determine final status based on Wait() error and signals
	finalStatus := StatusStopped
	if waitErr != nil {
		errMsg = waitErr.Error()
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				sig := status.Signal()
				logger.Warn("Process killed by signal", "pid", appInstance.Pid, "signal", sig)
				// Treat SIGTERM/SIGINT during intentional stop as clean stop
				if (sig == syscall.SIGTERM || sig == syscall.SIGINT) && intentionalStop {
					finalStatus = StatusStopped
				} else {
					finalStatus = StatusCrashed // Killed by other signal or during non-intentional stop
				}
			} else {
				// Exited non-zero, but not signalled
				logger.Warn("Process exited non-zero", "pid", appInstance.Pid, "exit_code", exitCode)
				finalStatus = StatusCrashed
			}
		} else {
			// Wait failed for other reason (e.g., I/O error, start failed initially?)
			logger.Error("Process wait failed", "error", waitErr)
			finalStatus = StatusFailed
			intentionalStop = true // Treat non-exit errors as fatal, no restart
		}
	} else {
		// Exited cleanly (code 0)
		exitCode = 0
		logger.Info("Process exited cleanly", "pid", appInstance.Pid)
		finalStatus = StatusStopped
	}

	// Update internal state and publish
	appInstance.Status = finalStatus
	appInstance.lastExitCode = &exitCode
	nr.publishStatusUpdate(instanceID, finalStatus, &appInstance.Pid, &exitCode, errMsg)

	// --- Restart Logic ---
	// Conditions for NO restart:
	// 1. Intentional stop (signal or context cancel)
	// 2. Final status is Failed (unrecoverable error)
	// 3. Runner is shutting down (global context cancelled)
	if intentionalStop || finalStatus == StatusFailed || nr.globalCtx.Err() != nil {
		logger.Info("Instance stopped intentionally or failed permanently or runner shutting down. No restart.", "status", finalStatus, "intentional", intentionalStop, "runner_shutdown", nr.globalCtx.Err() != nil)
		return // Exit monitor
	}

	// Conditions met for potential restart (Crashed or Stopped unexpectedly)
	appInfo := nr.state.GetAppInfo(appName) // Read-only access needed here

	// Check if config changed or instance was removed while process was running
	appInfo.mu.RLock()
	configHashMatches := appInfo.configHash == appInstance.ConfigHash
	instanceStillManaged := false
	var currentReplicaIndex = -1 // Need index for restart call
	for idx, inst := range appInfo.instances {
		if inst.InstanceID == instanceID {
			instanceStillManaged = true
			currentReplicaIndex = idx
			break
		}
	}
	appInfo.mu.RUnlock()

	if !instanceStillManaged {
		logger.Warn("Instance disappeared from state before restart check. Aborting restart.")
		return
	}
	if !configHashMatches {
		logger.Info("Configuration changed since instance started. No restart.", "old_hash", appInstance.ConfigHash, "new_hash", appInfo.configHash)
		return
	}
	if currentReplicaIndex == -1 {
		logger.Error("Could not determine replica index for restarting instance. Aborting restart.", "instance_id", instanceID)
		return
	}

	// Check restart count
	if appInstance.restartCount >= MaxRestarts {
		logger.Error("Instance crashed too many times. Giving up.", "max_restarts", MaxRestarts)
		appInstance.Status = StatusFailed // Mark as permanently failed
		nr.publishStatusUpdate(instanceID, StatusFailed, &appInstance.Pid, &exitCode, "Exceeded max restarts")
		return // Exit monitor
	}

	// Proceed with restart
	appInstance.restartCount++
	logger.Warn("Instance crashed/stopped unexpectedly. Attempting restart.", "restart_count", appInstance.restartCount, "max_restarts", MaxRestarts, "delay", RestartDelay)
	appInstance.Status = StatusStarting // Set status before publishing
	nr.publishStatusUpdate(instanceID, StatusStarting, nil, nil, fmt.Sprintf("Restarting (%d/%d)", appInstance.restartCount, MaxRestarts))

	// Wait for restart delay, cancellable by global shutdown or specific stop signal
	restartTimer := time.NewTimer(RestartDelay)
	select {
	case <-restartTimer.C:
		// Delay finished
	case <-nr.globalCtx.Done():
		logger.Info("Restart canceled due to runner shutdown.")
		restartTimer.Stop()
		return // Exit monitor
	case <-appInstance.processCtx.Done():
		// This case should be less likely now but handles if context gets cancelled externally during delay
		logger.Warn("Instance context cancelled during restart delay?")
		restartTimer.Stop()
		return // Exit monitor
	case <-appInstance.StopSignal:
		logger.Info("Restart canceled due to explicit stop signal for instance.")
		restartTimer.Stop()
		return // Exit monitor
	}

	// Re-acquire lock to perform the restart safely
	appInfo.mu.Lock()
	defer appInfo.mu.Unlock()

	// Double-check conditions after acquiring lock and waiting
	currentConfigHashAfterDelay := appInfo.configHash
	instanceStillExistsAfterDelay := false
	for _, inst := range appInfo.instances {
		if inst.InstanceID == instanceID {
			instanceStillExistsAfterDelay = true
			break
		}
	}

	if !instanceStillExistsAfterDelay {
		logger.Warn("Instance was removed during restart delay. Aborting restart.")
	} else if currentConfigHashAfterDelay != appInstance.ConfigHash {
		logger.Info("Config changed during restart delay. Aborting restart.")
	} else {
		// Attempt the actual restart
		err := nr.startAppInstance(context.Background(), appInfo, currentReplicaIndex) // Pass necessary context and index
		if err != nil {
			logger.Error("Failed to restart application instance", "error", err)
			// If restart fails, mark as failed permanently
			appInstance.Status = StatusFailed
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, &exitCode, fmt.Sprintf("Restart failed: %v", err))
			// No return here, let defer handle cleanup
		} else {
			logger.Info("Instance restarted successfully")
			// Restart succeeded, this monitor goroutine's job is done.
			// The *new* instance started by startAppInstance will have its *own* monitor goroutine.
			return // Exit *this* monitor goroutine successfully.
		}
	}
}

// publishStatusUpdate sends the current status of an instance to NATS.
func (nr *NodeRunner) publishStatusUpdate(instanceID string, status AppStatus, pid *int, exitCode *int, errMsg string) {
	currentPid := 0
	if pid != nil && (status == StatusRunning || status == StatusStopping) {
		currentPid = *pid
	}
	appName := InstanceIDToAppName(instanceID)
	update := statusUpdate{InstanceID: instanceID, NodeID: nr.nodeID, Status: status, Pid: currentPid, ExitCode: exitCode, Error: errMsg, Time: time.Now()}
	updateData, err := json.Marshal(update)
	if err != nil {
		nr.logger.Warn("Failed to marshal status update", "instance_id", instanceID, "error", err)
		return
	}
	subject := fmt.Sprintf("status.%s.%s", appName, nr.nodeID)
	// Fire-and-forget publish
	if err := nr.nc.Publish(subject, updateData); err != nil {
		// Log error but don't block runner operation
		nr.logger.Warn("Failed to publish status update to NATS", "instance_id", instanceID, "subject", subject, "error", err)
	}
}

// fetchAndStoreBinary downloads the binary from NATS Object Store for the node's platform.
// It uses the object's digest to version the local storage path.
func (nr *NodeRunner) fetchAndStoreBinary(ctx context.Context, tag, appName, configHash string) (string, error) {
	// Construct the target object name using local platform
	targetObjectName := fmt.Sprintf("%s-%s-%s", tag, nr.localOS, nr.localArch)
	logger := nr.logger.With("app", appName, "tag", tag, "object", targetObjectName)

	// Get Object Info to determine the version (Digest)
	objInfo, getInfoErr := nr.appBinaries.GetInfo(ctx, targetObjectName) // ** Use target name **
	if getInfoErr != nil {
		if errors.Is(getInfoErr, jetstream.ErrObjectNotFound) {
			return "", fmt.Errorf("binary object '%s' for platform %s/%s not found in object store: %w", targetObjectName, nr.localOS, nr.localArch, getInfoErr)
		}
		return "", fmt.Errorf("failed to get info for binary object '%s': %w", targetObjectName, getInfoErr)
	}

	// Determine Version Identifier (Digest)
	versionID := objInfo.Digest
	if versionID == "" {
		logger.Error("Object store info is missing digest", "object", targetObjectName)
		return "", fmt.Errorf("binary object '%s' info is missing digest", targetObjectName)
	}
	// Ensure digest format is correct (SHA-256=...) before extracting value
	if !strings.HasPrefix(versionID, "SHA-256=") {
		return "", fmt.Errorf("unsupported digest format (expected 'SHA-256=' prefix): %s", versionID)
	}
	versionID = strings.TrimPrefix(versionID, "SHA-256=") // Extract the Base64 part
	if versionID == "" {
		logger.Error("Object store digest value is empty after parsing", "object", targetObjectName, "original_digest", objInfo.Digest)
		return "", fmt.Errorf("binary object '%s' digest value is empty", targetObjectName)
	}

	// Construct Local Path (Using digest and target object name)
	safeAppName := sanitizePathElement(appName)
	safeVersionID := sanitizePathElement(versionID)         // Digest hash (Base64 value)
	safeObjectName := sanitizePathElement(targetObjectName) // Includes OS/Arch
	versionedDir := filepath.Join(nr.dataDir, "binaries", safeAppName, safeVersionID)
	localPath := filepath.Join(versionedDir, safeObjectName)

	// Check if this specific version exists locally
	_, statErr := os.Stat(localPath)
	if statErr == nil {
		// File exists, verify hash
		hashMatches, err := verifyLocalFileHash(localPath, objInfo.Digest) // Pass original digest with prefix
		if err == nil && hashMatches {
			logger.Info("Local binary version exists and hash verified.", "path", localPath)
			return localPath, nil // Success, use local copy
		}
		// Hash mismatch or error reading local file, proceed to re-download
		logger.Warn("Local binary version exists but hash verification failed or errored. Re-downloading.", "path", localPath, "verify_err", err)
	} else if !os.IsNotExist(statErr) {
		// Error stating the file other than "not found"
		return "", fmt.Errorf("failed to stat local binary path %s: %w", localPath, statErr)
	} else {
		// File does not exist locally
		logger.Info("Local binary version not found.", "path", localPath)
	}

	//  Download
	logger.Info("Downloading specific binary version", "version_id", versionID, "local_path", localPath)
	if err := os.MkdirAll(versionedDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create versioned binary directory %s: %w", versionedDir, err)
	}
	// Use unique temp file name
	tempLocalPath := localPath + ".tmp." + fmt.Sprintf("%d", time.Now().UnixNano())
	outFile, err := os.Create(tempLocalPath)
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file %s: %w", tempLocalPath, err)
	}
	var finalErr error // To capture error for defer cleanup
	defer func() {
		if outFile != nil {
			outFile.Close() // Ensure closed before potential remove
		}
		// Remove temp file if an error occurred during download/verify/rename
		if finalErr != nil {
			logger.Warn("Removing temporary file due to error", "path", tempLocalPath, "error", finalErr)
			if remErr := os.Remove(tempLocalPath); remErr != nil && !errors.Is(remErr, os.ErrNotExist) {
				logger.Error("Failed to remove temporary download file", "path", tempLocalPath, "error", remErr)
			}
		}
	}()

	// Get object reader
	objResult, err := nr.appBinaries.Get(ctx, targetObjectName) // ** Use target name **
	if err != nil {
		finalErr = fmt.Errorf("failed to get object reader for %s: %w", targetObjectName, err)
		return "", finalErr
	}
	defer objResult.Close() // Close object reader when done

	// Copy data
	bytesCopied, err := io.Copy(outFile, objResult)
	if err != nil {
		finalErr = fmt.Errorf("failed to copy object data to %s: %w", tempLocalPath, err)
		return "", finalErr
	}
	logger.Debug("Binary data copied", "temp_path", tempLocalPath, "bytes_copied", bytesCopied)

	// Sync and close file before verification/rename
	if syncErr := outFile.Sync(); syncErr != nil {
		logger.Error("Failed to sync temporary file", "path", tempLocalPath, "error", syncErr)
		// Potentially continue, but log the error
	}
	if closeErr := outFile.Close(); closeErr != nil {
		outFile = nil // Prevent double close in defer
		finalErr = fmt.Errorf("failed to close temporary file %s: %w", tempLocalPath, closeErr)
		return "", finalErr
	}
	outFile = nil // Mark as closed for defer

	// Verify checksum of downloaded file
	hashMatches, verifyErr := verifyLocalFileHash(tempLocalPath, objInfo.Digest) // Pass original digest
	if verifyErr != nil || !hashMatches {
		errMsg := fmt.Sprintf("hash verification failed (err: %v, match: %v)", verifyErr, hashMatches)
		logger.Error(errMsg, "path", tempLocalPath, "expected_digest", objInfo.Digest)
		finalErr = fmt.Errorf("hash verification failed for %s: %s", tempLocalPath, errMsg)
		return "", finalErr
	}

	// Atomic Rename
	logger.Debug("Verification successful, renaming", "from", tempLocalPath, "to", localPath)
	if err = os.Rename(tempLocalPath, localPath); err != nil {
		logger.Error("Failed to rename temporary binary file", "temp", tempLocalPath, "final", localPath, "error", err)
		// Set finalErr so defer removes the temp file if rename fails
		finalErr = fmt.Errorf("failed to finalize binary download for %s: %w", localPath, err)
		// Try removing temp file here too, just in case defer has issues
		_ = os.Remove(tempLocalPath)
		return "", finalErr
	}

	logger.Info("Binary version downloaded and verified successfully", "path", localPath)
	return localPath, nil // Success
}

// verifyLocalFileHash calculates the SHA256 hash of a file and compares it
// to the expected NATS Object Store digest string (e.g., "SHA-256=Base64Hash").
func verifyLocalFileHash(filePath, expectedDigest string) (bool, error) {
	if !strings.HasPrefix(expectedDigest, "SHA-256=") {
		return false, fmt.Errorf("unsupported digest format (expected 'SHA-256=' prefix): %s", expectedDigest)
	}
	expectedBase64Hash := strings.TrimPrefix(expectedDigest, "SHA-256=")
	expectedBase64Hash = strings.TrimRight(expectedBase64Hash, "=") // trailing b64
	if expectedBase64Hash == "" {
		return false, fmt.Errorf("expected digest value is empty after removing prefix: %s", expectedDigest)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return false, fmt.Errorf("failed to open file for hashing %s: %w", filePath, err)
	}
	defer f.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return false, fmt.Errorf("failed to read file for hashing %s: %w", filePath, err)
	}
	hashBytes := hasher.Sum(nil)

	// NATS uses URL encoding without padding for the digest value
	actualBase64Hash := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(hashBytes)
	actualHexHash := fmt.Sprintf("%x", hashBytes) // For logging/debugging

	// Compare the Base64 encoded values (padding doesn't matter for comparison here)
	match := actualBase64Hash == expectedBase64Hash

	if !match {
		slog.Warn("Hash verification failed", "file", filePath,
			"expected_digest", expectedDigest,
			"expected_base64", expectedBase64Hash,
			"calculated_base64", actualBase64Hash,
			"calculated_hex", actualHexHash)
	} else {
		slog.Debug("Hash verification successful", "file", filePath,
			"expected_digest", expectedDigest,
			"calculated_hex", actualHexHash)
	}
	return match, nil
}

// sanitizePathElement removes potentially problematic characters for filesystem paths.
func sanitizePathElement(element string) string {
	// Allow alphanumeric, hyphen, underscore, dot. Replace others.
	// Using strings.Builder for potentially better performance
	var sb strings.Builder
	sb.Grow(len(element)) // Preallocate approximate size

	for _, r := range element {
		if ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || ('0' <= r && r <= '9') || r == '-' || r == '_' || r == '.' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_') // Replace disallowed characters with underscore
		}
	}

	sanitized := sb.String()
	// Limit length to prevent excessively long paths
	const maxLen = 100
	if len(sanitized) > maxLen {
		sanitized = sanitized[:maxLen]
	}
	// Avoid names that are just "." or ".."
	if sanitized == "." || sanitized == ".." {
		return "_" + sanitized + "_"
	}
	return sanitized
}

// removeInstanceState removes the state for a specific instance from the manager.
func (nr *NodeRunner) removeInstanceState(appName, instanceID string, logger *slog.Logger) {
	appInfo := nr.state.GetAppInfo(appName) // Gets or creates (though should exist)
	appInfo.mu.Lock()
	defer appInfo.mu.Unlock()

	newInstances := make([]*ManagedApp, 0, len(appInfo.instances))
	found := false
	for _, instance := range appInfo.instances {
		if instance.InstanceID == instanceID {
			found = true
			// Optionally, explicitly cancel context if not already done
			if instance.processCancel != nil {
				instance.processCancel()
			}
		} else {
			newInstances = append(newInstances, instance)
		}
	}

	if found {
		appInfo.instances = newInstances
		logger.Info("Removed instance state.", "remaining_instances", len(newInstances))
		// If this was the last instance, maybe remove the appInfo entirely?
		// Need careful consideration of locking if we do that here.
		// For now, leave the appInfo entry even if instances list is empty.
	} else {
		logger.Warn("Attempted to remove instance state, but it was not found.")
	}
}
