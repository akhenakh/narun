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
	RestartDelay    = 5 * time.Second  // Delay before restarting a crashed app
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
func (nr *NodeRunner) startAppInstance(ctx context.Context, appInfo *appInfo, replicaIndex int) error {
	spec := appInfo.spec // Use the spec from appInfo
	if spec == nil {
		return fmt.Errorf("cannot start instance, app spec is nil for %s", appInfo.configHash /* Use hash or appName */)
	}
	appName := spec.Name
	binaryVersionTag := spec.BinaryVersionTag // ** Get the tag **
	instanceID := GenerateInstanceID(appName, replicaIndex)
	logger := nr.logger.With("app", appName, "instance_id", instanceID, "version_tag", binaryVersionTag)
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

	// Fetch and Store Binary ** using version tag and local platform **
	binaryPath, err := nr.fetchAndStoreBinary(ctx, binaryVersionTag, appName, configHash)
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

	// Directory Setup (Unchanged)
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

	// Pipes (Unchanged)
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

	// Create/Update ManagedApp State (Unchanged logic, fields same)
	appInstance := &ManagedApp{
		InstanceID: instanceID, Spec: spec, Cmd: cmd, Status: StatusStarting, ConfigHash: configHash,
		BinaryPath: binaryPath, StdoutPipe: stdoutPipe, StderrPipe: stderrPipe,
		StopSignal: make(chan struct{}), processCtx: processCtx, processCancel: processCancel, restartCount: 0,
	}

	// Replace or Add the instance in appInfo (Unchanged)
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

	// Publish starting status (Unchanged)
	nr.publishStatusUpdate(instanceID, StatusStarting, nil, nil, "Starting process")

	// Start Process (Unchanged)
	logger.Info("Starting process", "command", cmdPath, "args", spec.Args, "env_added", 6) // Env count updated
	if startErr := cmd.Start(); startErr != nil {
		errMsg := fmt.Sprintf("Failed to start process: %v", startErr)
		logger.Error(errMsg, "error", startErr)
		appInstance.Status = StatusFailed
		appInstance.processCancel()
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
		return fmt.Errorf("failed to start %s: %w", instanceID, startErr)
	}

	// Success Path (Unchanged)
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

// stopAppInstance (Unchanged)
func (nr *NodeRunner) stopAppInstance(appInstance *ManagedApp, intentional bool) error {
	instanceID := appInstance.InstanceID
	logger := nr.logger.With("app", InstanceIDToAppName(instanceID), "instance_id", instanceID)

	if appInstance.Status == StatusStopped || appInstance.Status == StatusStopping || appInstance.Status == StatusFailed {
		logger.Info("Stop request: Instance not running or already stopping/stopped/failed.", "status", appInstance.Status)
		return nil
	}
	if appInstance.Cmd == nil || appInstance.Cmd.Process == nil {
		logger.Warn("Stop request: Instance state inconsistent (no command/process). Cleaning up state.")
		appInstance.Status = StatusFailed
		if appInstance.processCancel != nil {
			appInstance.processCancel()
		}
		return fmt.Errorf("inconsistent state for instance %s", instanceID)
	}

	pid := appInstance.Pid
	logger.Info("Stopping application instance process", "pid", pid)
	appInstance.Status = StatusStopping
	nr.publishStatusUpdate(instanceID, StatusStopping, &pid, nil, "")

	select {
	case <-appInstance.StopSignal:
	default:
		close(appInstance.StopSignal)
	}

	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			logger.Warn("Stop request: Process already exited before SIGTERM", "pid", pid)
			appInstance.processCancel() // Ensure monitor wakes up
			return nil
		}
		logger.Error("Failed to send SIGTERM", "pid", pid, "error", err)
		appInstance.processCancel()
		return fmt.Errorf("failed to send SIGTERM to %s (PID %d): %w", instanceID, pid, err)
	}

	termTimer := time.NewTimer(StopTimeout)
	defer termTimer.Stop()
	select {
	case <-termTimer.C:
		logger.Warn("Graceful shutdown timed out. Sending SIGKILL.", "pid", pid)
		if killErr := syscall.Kill(-pid, syscall.SIGKILL); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
			logger.Error("Failed to send SIGKILL", "pid", pid, "error", killErr)
		}
		appInstance.processCancel()
	case <-appInstance.processCtx.Done():
		logger.Info("Process exited or context canceled during stop wait.")
	}
	return nil
}

// forwardLogs (Unchanged)
func (nr *NodeRunner) forwardLogs(appInstance *ManagedApp, pipe io.ReadCloser, streamName string, logger *slog.Logger) {
	defer appInstance.LogWg.Done()
	defer pipe.Close()
	scanner := bufio.NewScanner(pipe)
	scanner.Buffer(make([]byte, LogBufferSize), bufio.MaxScanTokenSize)
	logger = logger.With("stream", streamName)
	logger.Debug("Starting log forwarding")
	appName := InstanceIDToAppName(appInstance.InstanceID)

	for scanner.Scan() {
		line := scanner.Text()
		logMsg := logEntry{InstanceID: appInstance.InstanceID, AppName: appName, NodeID: nr.nodeID, Stream: streamName, Message: line, Timestamp: time.Now()}
		logData, err := json.Marshal(logMsg)
		if err != nil {
			logger.Warn("Failed to marshal log entry", "error", err)
			continue
		}
		subject := fmt.Sprintf("logs.%s.%s", appName, nr.nodeID)
		if err := nr.nc.Publish(subject, logData); err != nil {
			logger.Warn("Failed to publish log entry to NATS", "subject", subject, "error", err)
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, os.ErrClosed) && !errors.Is(err, context.Canceled) {
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

// monitorAppInstance (Unchanged logic, just passes info to startAppInstance)
func (nr *NodeRunner) monitorAppInstance(appInstance *ManagedApp, logger *slog.Logger) {
	instanceID := appInstance.InstanceID
	appName := InstanceIDToAppName(instanceID)

	defer func() {
		appInstance.processCancel()
		appInstance.LogWg.Wait()
		logger.Debug("Monitor finished and resources cleaned up")
		if appInstance.Status == StatusFailed || appInstance.Status == StatusStopped {
			nr.removeInstanceState(appName, instanceID, logger)
		}
	}()

	logger.Info("Monitoring process instance", "pid", appInstance.Pid)
	waitErr := appInstance.Cmd.Wait()

	exitCode := -1
	intentionalStop := false
	errMsg := ""
	select {
	case <-appInstance.StopSignal:
		intentionalStop = true
		logger.Info("Process exit detected after intentional stop signal")
	default:
	}
	if appInstance.processCtx.Err() == context.Canceled && !intentionalStop {
		intentionalStop = true
		logger.Info("Process exit detected after context cancellation")
	}

	finalStatus := StatusStopped
	if waitErr != nil {
		errMsg = waitErr.Error()
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				sig := status.Signal()
				logger.Warn("Process killed by signal", "pid", appInstance.Pid, "signal", sig)
				if (sig == syscall.SIGTERM || sig == syscall.SIGINT) && intentionalStop {
					finalStatus = StatusStopped
				} else {
					finalStatus = StatusCrashed
				}
			} else {
				logger.Warn("Process exited non-zero", "pid", appInstance.Pid, "exit_code", exitCode)
				finalStatus = StatusCrashed
			}
		} else {
			logger.Error("Process wait failed", "error", waitErr)
			finalStatus = StatusFailed
			intentionalStop = true
		}
	} else {
		exitCode = 0
		logger.Info("Process exited cleanly", "pid", appInstance.Pid)
		finalStatus = StatusStopped
	}

	appInstance.Status = finalStatus
	appInstance.lastExitCode = &exitCode
	nr.publishStatusUpdate(instanceID, finalStatus, &appInstance.Pid, &exitCode, errMsg)

	if intentionalStop || finalStatus == StatusFailed {
		logger.Info("Instance stopped intentionally or failed permanently. No restart.", "status", finalStatus)
	} else if finalStatus == StatusCrashed || finalStatus == StatusStopped {
		appInfo := nr.state.GetAppInfo(appName)
		appInfo.mu.RLock()
		configHashMatches := appInfo.configHash == appInstance.ConfigHash
		appInfo.mu.RUnlock()

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
			logger.Warn("Instance disappeared from state before restart check. Aborting restart.")
			return
		}

		shouldRestart := false
		if !configHashMatches {
			logger.Info("Configuration changed since instance started. No restart.", "old_hash", appInstance.ConfigHash, "new_hash", appInfo.configHash)
		} else if appInstance.restartCount >= MaxRestarts {
			logger.Error("Instance crashed too many times. Giving up.", "max_restarts", MaxRestarts)
			appInstance.Status = StatusFailed
			nr.publishStatusUpdate(instanceID, StatusFailed, &appInstance.Pid, &exitCode, "Exceeded max restarts")
		} else {
			shouldRestart = true
		}

		if shouldRestart {
			appInstance.restartCount++
			logger.Warn("Instance crashed. Attempting restart.", "restart_count", appInstance.restartCount, "max_restarts", MaxRestarts, "delay", RestartDelay)
			appInstance.Status = StatusStarting
			nr.publishStatusUpdate(instanceID, StatusStarting, nil, nil, fmt.Sprintf("Restarting (%d/%d)", appInstance.restartCount, MaxRestarts))

			restartTimer := time.NewTimer(RestartDelay)
			select {
			case <-restartTimer.C:
			case <-nr.globalCtx.Done():
				logger.Info("Restart canceled due to runner shutdown.")
				restartTimer.Stop()
				return
			case <-appInstance.processCtx.Done():
				logger.Warn("Instance context cancelled during restart delay?")
				restartTimer.Stop()
				return
			}

			appInfo.mu.Lock() // Lock for the start attempt
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
				// ** Pass binaryVersionTag (implicitly via appInfo.spec) to startAppInstance **
				err := nr.startAppInstance(context.Background(), appInfo, replicaIndex)
				if err != nil {
					logger.Error("Failed to restart application instance", "error", err)
					appInstance.Status = StatusFailed
					nr.publishStatusUpdate(instanceID, StatusFailed, nil, &exitCode, fmt.Sprintf("Restart failed: %v", err))
				} else {
					logger.Info("Instance restarted successfully")
					appInfo.mu.Unlock()
					return // Exit *this* monitor goroutine
				}
			}
			appInfo.mu.Unlock()
		} else {
			if appInstance.restartCount >= MaxRestarts {
				appInstance.Status = StatusFailed
			}
		}
	}
}

// publishStatusUpdate (Unchanged)
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
	if err := nr.nc.Publish(subject, updateData); err != nil {
		nr.logger.Warn("Failed to publish status update to NATS", "instance_id", instanceID, "subject", subject, "error", err)
	}
}

// fetchAndStoreBinary downloads the binary from NATS Object Store for the node's platform.
// It uses the object's digest to version the local storage path.
func (nr *NodeRunner) fetchAndStoreBinary(ctx context.Context, binaryVersionTag, appName, configHash string) (string, error) {
	// ** Construct the target object name using local platform **
	targetObjectName := fmt.Sprintf("%s-%s-%s", binaryVersionTag, nr.localOS, nr.localArch)
	logger := nr.logger.With("app", appName, "version_tag", binaryVersionTag, "object", targetObjectName)

	// 1. Get Object Info to determine the version (Digest)
	objInfo, getInfoErr := nr.appBinaries.GetInfo(ctx, targetObjectName) // ** Use target name **
	if getInfoErr != nil {
		if errors.Is(getInfoErr, jetstream.ErrObjectNotFound) {
			return "", fmt.Errorf("binary object '%s' for platform %s/%s not found in object store: %w", targetObjectName, nr.localOS, nr.localArch, getInfoErr)
		}
		return "", fmt.Errorf("failed to get info for binary object '%s': %w", targetObjectName, getInfoErr)
	}

	// 2. Determine Version Identifier (Digest) - Unchanged logic
	versionID := objInfo.Digest
	if versionID == "" {
		logger.Error("Object store info is missing digest", "object", targetObjectName)
		return "", fmt.Errorf("binary object '%s' info is missing digest", targetObjectName)
	}
	if parts := strings.SplitN(versionID, "=", 2); len(parts) == 2 {
		versionID = parts[1]
	}
	if versionID == "" {
		logger.Error("Object store digest value is empty after parsing", "object", targetObjectName, "original_digest", objInfo.Digest)
		return "", fmt.Errorf("binary object '%s' digest value is empty", targetObjectName)
	}

	// 3. Construct Local Path (Using digest and target object name)
	safeAppName := sanitizePathElement(appName)
	safeVersionID := sanitizePathElement(versionID)         // Digest hash
	safeObjectName := sanitizePathElement(targetObjectName) // Includes OS/Arch
	// Store the OS/Arch specific binary under its digest, using its full name
	versionedDir := filepath.Join(nr.dataDir, "binaries", safeAppName, safeVersionID)
	localPath := filepath.Join(versionedDir, safeObjectName)

	// 4. Check if this specific version exists locally (Unchanged logic)
	_, statErr := os.Stat(localPath)
	if statErr == nil {
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

	// 5. Download (Unchanged logic, uses targetObjectName implicitly via objResult)
	logger.Info("Downloading specific binary version", "version_id", versionID, "local_path", localPath)
	if err := os.MkdirAll(versionedDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create versioned binary directory %s: %w", versionedDir, err)
	}
	tempLocalPath := localPath + ".tmp" + fmt.Sprintf(".%d", time.Now().UnixNano())
	outFile, err := os.Create(tempLocalPath)
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file %s: %w", tempLocalPath, err)
	}
	defer func() {
		if outFile != nil {
			outFile.Close()
			if err != nil {
				logger.Warn("Removing temporary file", "path", tempLocalPath, "error", err)
				os.Remove(tempLocalPath)
			}
		}
	}()

	objResult, err := nr.appBinaries.Get(ctx, targetObjectName) // ** Use target name **
	if err != nil {
		outFile.Close()
		outFile = nil
		os.Remove(tempLocalPath)
		return "", fmt.Errorf("failed to get object reader for %s: %w", targetObjectName, err)
	}
	defer objResult.Close()

	bytesCopied, err := io.Copy(outFile, objResult)
	if err != nil {
		return "", fmt.Errorf("failed to copy object data to %s: %w", tempLocalPath, err)
	}
	logger.Debug("Binary data copied", "temp_path", tempLocalPath, "bytes_copied", bytesCopied)
	if syncErr := outFile.Sync(); syncErr != nil {
		logger.Error("Failed to sync temporary file", "path", tempLocalPath, "error", syncErr)
	}
	if err = outFile.Close(); err != nil {
		outFile = nil
		os.Remove(tempLocalPath)
		return "", fmt.Errorf("failed to close temporary file %s: %w", tempLocalPath, err)
	}
	outFile = nil

	// Verify checksum (Unchanged logic)
	hashMatches, verifyErr := verifyLocalFileHash(tempLocalPath, objInfo.Digest)
	if verifyErr != nil || !hashMatches {
		errMsg := fmt.Sprintf("hash verification failed (err: %v, match: %v)", verifyErr, hashMatches)
		logger.Error(errMsg, "path", tempLocalPath, "expected_digest", objInfo.Digest)
		err = fmt.Errorf("hash verification failed for %s: %s", tempLocalPath, errMsg) // Set error for defer cleanup
		return "", err
	}

	// Atomic Rename (Unchanged logic)
	logger.Debug("Verification successful, renaming", "from", tempLocalPath, "to", localPath)
	if err = os.Rename(tempLocalPath, localPath); err != nil {
		logger.Error("Failed to rename temporary binary file", "temp", tempLocalPath, "final", localPath, "error", err)
		_ = os.Remove(tempLocalPath) // Attempt cleanup
		return "", fmt.Errorf("failed to finalize binary download for %s: %w", localPath, err)
	}

	logger.Info("Binary version downloaded and verified successfully", "path", localPath)
	return localPath, nil // Success
}

// verifyLocalFileHash (Unchanged - already handles padding correctly)
func verifyLocalFileHash(filePath, expectedDigest string) (bool, error) {
	if !strings.HasPrefix(expectedDigest, "SHA-256=") {
		return false, fmt.Errorf("unsupported digest format (expected 'SHA-256=' prefix): %s", expectedDigest)
	}
	expectedBase64Hash := strings.TrimPrefix(expectedDigest, "SHA-256=")
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

	actualBase64Hash := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(hashBytes)
	actualHexHash := fmt.Sprintf("%x", hashBytes)

	trimmedExpected := strings.TrimRight(expectedBase64Hash, "=")
	trimmedActual := strings.TrimRight(actualBase64Hash, "=")

	slog.Debug("Verifying file hash", "file", filePath, "expected_base64_raw", expectedBase64Hash, "calculated_base64_raw", actualBase64Hash, "expected_base64_trimmed", trimmedExpected, "calculated_base64_trimmed", trimmedActual, "calculated_hex", actualHexHash)

	match := trimmedActual == trimmedExpected
	if !match {
		slog.Warn("Hash verification failed", "file", filePath, "expected_digest", expectedDigest, "expected_base64_trimmed", trimmedExpected, "calculated_base64_trimmed", trimmedActual, "calculated_hex", actualHexHash)
	}
	return match, nil
}

// sanitizePathElement (Unchanged)
func sanitizePathElement(element string) string {
	r := strings.NewReplacer(":", "_", "/", "_", "\\", "_", "*", "_", "?", "_", "<", "_", ">", "_", "|", "_")
	sanitized := r.Replace(element)
	if len(sanitized) > 100 {
		sanitized = sanitized[:100]
	}
	return sanitized
}

// removeInstanceState (Unchanged)
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
