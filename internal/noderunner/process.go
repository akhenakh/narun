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
	"runtime"
	"strings"
	"syscall"
	"time"

	"slices"

	"github.com/akhenakh/narun/internal/crypto"
	"github.com/akhenakh/narun/internal/metrics" // Added for metrics
	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

const (
	RestartDelay    = 2 * time.Second  // Delay before restarting a crashed app
	MaxRestarts     = 5                // Max restarts before giving up (per instance)
	StopTimeout     = 10 * time.Second // Time to wait for graceful shutdown before SIGKILL
	LogBufferSize   = 1024             // Buffer size for log lines
	StatusQueueSize = 10               // Buffer size for status update channel
)

// Constants for landlock
const (
	internalLaunchFlag           = "--internal-landlock-launch"
	envLandlockConfigJSON        = "NARUN_INTERNAL_LANDLOCK_CONFIG_JSON"
	envLandlockTargetCmd         = "NARUN_INTERNAL_LANDLOCK_TARGET_CMD"
	envLandlockTargetArgsJSON    = "NARUN_INTERNAL_LANDLOCK_TARGET_ARGS_JSON"
	landlockLauncherErrCode      = 120 // Generic launcher setup error
	landlockUnsupportedErrCode   = 121 // Landlock not supported on OS/Kernel
	landlockLockFailedErrCode    = 122 // l.Lock() failed
	landlockTargetExecFailedCode = 123 // syscall.Exec() failed
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

// Placeholder for landlock integration refinement - passing mount info
type MountInfoForEnv struct {
	Path        string `json:"path"`         // Relative path
	Source      string `json:"source"`       // Object store name
	ResolvedAbs string `json:"resolved_abs"` // Absolute path calculated *by parent*
}

// startAppInstance attempts to download the binary, configure, and start a single instance.
// It assumes the calling code (handleAppConfigUpdate) holds the lock on appInfo.
func (nr *NodeRunner) startAppInstance(ctx context.Context, appInfo *appInfo, replicaIndex int) error {
	spec := appInfo.spec
	if spec == nil {
		return fmt.Errorf("cannot start instance, app spec is nil for %s", appInfo.configHash)
	}
	appName := spec.Name
	tag := spec.Tag
	instanceID := GenerateInstanceID(appName, replicaIndex)
	logger := nr.logger.With("app", appName, "instance_id", instanceID, "tag", tag, "mode", spec.Mode)
	logger.Info("Attempting to start application instance")

	// Fetch Binary First
	binaryPath, fetchErr := nr.fetchAndStoreBinary(ctx, spec.Tag, appName, appInfo.configHash)
	if fetchErr != nil {
		errMsg := fmt.Sprintf("Binary fetch failed: %v", fetchErr)
		logger.Error(errMsg, "error", fetchErr)
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
		return fmt.Errorf("failed to fetch binary for %s: %w", instanceID, fetchErr)
	}
	logger.Info("Binary downloaded/verified", "path", binaryPath)
	if err := os.Chmod(binaryPath, 0755); err != nil {
		logger.Error("Failed to make binary executable", "path", binaryPath, "error", err)
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("Binary not executable: %v", err))
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
		return fmt.Errorf("failed making binary executable %s: %w", binaryPath, err)
	}

	// Prepare Instance Directory and Working Directory
	instanceDir := filepath.Join(nr.dataDir, "instances", instanceID)
	workDir := filepath.Join(instanceDir, "work")
	if err := os.MkdirAll(workDir, 0755); err != nil {
		logger.Error("Failed to create instance working directory", "dir", workDir, "error", err)
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("Failed work dir: %v", err))
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
		return fmt.Errorf("failed to create work dir %s for instance %s: %w", workDir, instanceID, err)
	}
	logger.Debug("Instance directory prepared", "path", workDir)

	// ** Handle Mounts ** (Fetch files *before* preparing command env/args)
	mountInfosForEnv := make([]MountInfoForEnv, 0, len(spec.Mounts)) // For landlock launcher
	for _, mount := range spec.Mounts {
		if mount.Source.ObjectStore != "" {
			err := nr.fetchAndPlaceFile(ctx, mount.Source.ObjectStore, workDir, mount.Path, logger) // Pass workDir
			if err != nil {
				errMsg := fmt.Sprintf("Failed to process mount %s: %v", mount.Path, err)
				logger.Error(errMsg, "source_object", mount.Source.ObjectStore)
				nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
				metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
				return fmt.Errorf("%s", errMsg)                                                          // Treat mount failure as fatal for now
			}
			// Calculate absolute path for landlock launcher env
			absMountPath := filepath.Join(workDir, filepath.Clean(mount.Path))
			mountInfosForEnv = append(mountInfosForEnv, MountInfoForEnv{
				Path:        mount.Path,
				Source:      mount.Source.ObjectStore,
				ResolvedAbs: absMountPath,
			})
		} // else: handle other source types later
	}
	logger.Info("File mounts processed successfully", "count", len(spec.Mounts))

	// Prepare Common Env Vars (including secrets)
	processEnv := os.Environ()
	// Add Base env vars from spec, resolving secrets
	for _, envVar := range spec.Env {
		var resolvedValue string
		var resErr error

		if envVar.ValueFromSecret != "" {
			secretName := envVar.ValueFromSecret
			resolvedValue, resErr = nr.resolveSecret(ctx, secretName, logger)
			if resErr != nil {
				logger.Error("Failed to resolve secret for env var", "secret_name", secretName, "env_var", envVar.Name, "error", resErr)
				errMsg := fmt.Sprintf("Failed to resolve secret '%s': %v", secretName, resErr)
				nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
				metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
				return fmt.Errorf("failed to resolve secret '%s' for env var '%s': %w", secretName, envVar.Name, resErr)
			}
			logger.Debug("Successfully resolved secret", "secret_name", secretName, "env_var", envVar.Name)
		} else {
			resolvedValue = envVar.Value
		}
		processEnv = append(processEnv, fmt.Sprintf("%s=%s", envVar.Name, resolvedValue))
	}
	// Add instance-specific env vars
	processEnv = append(processEnv, fmt.Sprintf("NARUN_APP_NAME=%s", appName))
	processEnv = append(processEnv, fmt.Sprintf("NARUN_INSTANCE_ID=%s", instanceID))
	processEnv = append(processEnv, fmt.Sprintf("NARUN_REPLICA_INDEX=%d", replicaIndex))
	processEnv = append(processEnv, fmt.Sprintf("NARUN_NODE_ID=%s", nr.nodeID))
	processEnv = append(processEnv, fmt.Sprintf("NARUN_INSTANCE_ROOT=%s", workDir))
	processEnv = append(processEnv, fmt.Sprintf("NARUN_NODE_OS=%s", nr.localOS))
	processEnv = append(processEnv, fmt.Sprintf("NARUN_NODE_ARCH=%s", nr.localArch))

	// Create context for the child process (launcher or direct exec)
	processCtx, processCancel := context.WithCancel(nr.globalCtx)

	// Prepare Command based on Mode
	var cmd *exec.Cmd
	var cmdPath string
	var cmdArgs []string

	if spec.Mode == "landlock" {
		logger.Info("Preparing Landlock execution mode")
		if runtime.GOOS != "linux" {
			processCancel()
			errMsg := "landlock mode is only supported on Linux"
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
			return fmt.Errorf("%s", errMsg)
		}

		selfPath, err := os.Executable() // Path to the node-runner binary itself
		if err != nil {
			processCancel()
			errMsg := fmt.Sprintf("Failed to get node-runner executable path: %v", err)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
			return fmt.Errorf("%s", errMsg)
		}

		// Serialize config needed by the launcher child process
		landlockConfigJSON, err := json.Marshal(spec.Landlock)
		if err != nil {
			processCancel()
			errMsg := fmt.Sprintf("Failed to marshal landlock config to JSON: %v", err)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
			return fmt.Errorf("%s", errMsg)
		}
		targetArgsJSON, err := json.Marshal(spec.Args)
		if err != nil {
			processCancel()
			errMsg := fmt.Sprintf("Failed to marshal target args to JSON: %v", err)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
			return fmt.Errorf("%s", errMsg)
		}

		// Serialize mount info for the launcher
		mountInfosJSON, err := json.Marshal(mountInfosForEnv)
		if err != nil {
			processCancel()
			errMsg := fmt.Sprintf("Failed to marshal mount info to JSON: %v", err)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
			return fmt.Errorf("%s", errMsg)
		}

		// Prepare the environment specifically for the LAUNCHER process
		launcherEnv := slices.Clone(processEnv) // Start with app's resolved env
		launcherEnv = append(launcherEnv, fmt.Sprintf("%s=%s", envLandlockConfigJSON, string(landlockConfigJSON)))
		launcherEnv = append(launcherEnv, fmt.Sprintf("%s=%s", envLandlockTargetCmd, binaryPath)) // Pass the fetched application binary path
		launcherEnv = append(launcherEnv, fmt.Sprintf("%s=%s", envLandlockTargetArgsJSON, string(targetArgsJSON)))
		launcherEnv = append(launcherEnv, fmt.Sprintf("NARUN_INTERNAL_MOUNT_INFOS_JSON=%s", string(mountInfosJSON))) // Pass mount info

		// Command to run the launcher (node-runner binary with the internal flag)
		cmdPath = selfPath
		cmdArgs = []string{internalLaunchFlag} // Launcher only needs the flag
		cmd = exec.CommandContext(processCtx, cmdPath, cmdArgs...)
		cmd.Env = launcherEnv // Use the extended environment for the launcher

		logger.Debug("Launcher command prepared", "cmd", cmdPath, "args", cmdArgs)

	} else { // Default "exec" mode
		logger.Info("Preparing standard execution mode")
		cmdPath = binaryPath // Execute the application binary directly
		cmdArgs = spec.Args
		cmd = exec.CommandContext(processCtx, cmdPath, cmdArgs...)
		cmd.Env = processEnv // Use the standard application environment
	}

	// Common Setup (Directory, Pipes, State)
	cmd.Dir = workDir
	// Ensure child process can be killed cleanly via process group
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Pipes for log forwarding
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		processCancel()
		logger.Error("Failed to get stdout pipe", "error", err)
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("stdout pipe failed: %v", err))
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
		return fmt.Errorf("stdout pipe failed for %s: %w", instanceID, err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		processCancel()
		stdoutPipe.Close() // Close previous pipe on error
		logger.Error("Failed to get stderr pipe", "error", err)
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("stderr pipe failed: %v", err))
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
		return fmt.Errorf("stderr pipe failed for %s: %w", instanceID, err)
	}

	// Create/Update ManagedApp State in the manager
	appInstance := &ManagedApp{
		InstanceID:    instanceID,
		Spec:          spec, // Store reference to the spec
		Cmd:           cmd,  // Store the prepared command (launcher or direct exec)
		Status:        StatusStarting,
		ConfigHash:    appInfo.configHash,
		BinaryPath:    binaryPath, // Store path to the actual app binary
		StdoutPipe:    stdoutPipe,
		StderrPipe:    stderrPipe,
		StopSignal:    make(chan struct{}),
		processCtx:    processCtx,
		processCancel: processCancel,
		restartCount:  0, // Initial start
	}
	// Find and replace or append logic (remains the same)
	foundAndReplaced := false
	for i, existing := range appInfo.instances {
		if existing.InstanceID == instanceID {
			appInstance.restartCount = existing.restartCount // Inherit restart count on update
			logger.Debug("Replacing existing state for instance in appInfo", "index", i, "inheriting_restart_count", appInstance.restartCount)
			if existing.processCancel != nil {
				existing.processCancel() // Cancel old context if replacing
			}
			// Explicitly clean up metrics for the old instance being replaced
			nr.cleanupInstanceMetrics(existing)
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

	// Start the Process (either the launcher or the direct application)
	logger.Info("Starting process execution", "command", cmdPath, "args", cmdArgs, "env_count", len(cmd.Env))
	if startErr := cmd.Start(); startErr != nil {
		errMsg := fmt.Sprintf("Failed to start process: %v", startErr)
		logger.Error(errMsg, "error", startErr)
		appInstance.Status = StatusFailed
		appInstance.processCancel() // Ensure context is cancelled
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down
		nr.removeInstanceState(appName, instanceID, logger)                                      // Clean up state if start fails
		nr.cleanupInstanceMetrics(appInstance)                                                   // Cleanup metrics on start fail
		return fmt.Errorf("failed to start %s: %w", instanceID, startErr)
	}

	// Success Path
	appInstance.Pid = cmd.Process.Pid // Get the PID of the started process
	appInstance.StartTime = time.Now()
	appInstance.Status = StatusRunning
	logger.Info("Process started successfully", "pid", appInstance.Pid)
	nr.publishStatusUpdate(instanceID, StatusRunning, &appInstance.Pid, nil, "")

	// Update metrics for successful start
	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(1)
	metrics.NarunNodeRunnerInstanceInfo.WithLabelValues(appName, instanceID, nr.nodeID, spec.Tag, spec.Mode, binaryPath).Set(1)

	// Start log forwarding and monitoring goroutines
	appInstance.LogWg.Add(2)
	go nr.forwardLogs(appInstance, appInstance.StdoutPipe, "stdout", logger)
	go nr.forwardLogs(appInstance, appInstance.StderrPipe, "stderr", logger)
	go nr.monitorAppInstance(appInstance, logger) // Monitor handles cmd.Wait()

	return nil
}

// stopAppInstance gracefully stops an application instance process.
func (nr *NodeRunner) stopAppInstance(appInstance *ManagedApp, intentional bool) error {
	instanceID := appInstance.InstanceID
	appName := InstanceIDToAppName(instanceID)
	logger := nr.logger.With("app", appName, "instance_id", instanceID)

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
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0)     // Mark as down
		return fmt.Errorf("inconsistent state for instance %s", instanceID)
	}

	pid := appInstance.Pid
	logger.Info("Stopping application instance process", "pid", pid)
	appInstance.Status = StatusStopping
	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0) // Mark as down now
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

// resolveSecret fetches and decrypts a secret from the KV store.
func (nr *NodeRunner) resolveSecret(ctx context.Context, secretName string, logger *slog.Logger) (string, error) {
	if nr.masterKey == nil {
		return "", fmt.Errorf("cannot resolve secret '%s': node runner has no master key configured", secretName)
	}

	getCtx, cancel := context.WithTimeout(ctx, 5*time.Second) // Short timeout for KV get
	defer cancel()

	entry, err := nr.kvSecrets.Get(getCtx, secretName)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return "", fmt.Errorf("secret '%s' not found in KV store '%s'", secretName, SecretKVBucket)
		}
		return "", fmt.Errorf("failed to get secret '%s' from KV: %w", secretName, err)
	}

	var storedSecret StoredSecret
	if err := json.Unmarshal(entry.Value(), &storedSecret); err != nil {
		return "", fmt.Errorf("failed to unmarshal stored secret data for '%s': %w", secretName, err)
	}

	// Use secret name as Additional Authenticated Data (AAD)
	aad := []byte(secretName)

	plaintextBytes, err := crypto.Decrypt(nr.masterKey, storedSecret.Ciphertext, storedSecret.Nonce, aad)
	if err != nil {
		logger.Error("Decryption failed", "secret_name", secretName, "error", err)
		// Don't return the specific crypto error message to the user/logs ideally,
		// just a generic "decryption failed" to avoid leaking info.
		return "", fmt.Errorf("decryption failed for secret '%s'", secretName)
	}

	return string(plaintextBytes), nil
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
	spec := appInstance.Spec // Get spec reference (might be nil if config deleted during run)

	// Defer cleanup and state removal
	defer func() {
		appInstance.processCancel()
		appInstance.LogWg.Wait()
		logger.Debug("Monitor finished and log forwarders completed")

		// Determine if this stop requires *physical* disk cleanup
		shouldCleanupDisk := false
		finalStatus := appInstance.Status // Get the final status set before defer runs

		if finalStatus == StatusFailed { // Max restarts or unrecoverable error
			shouldCleanupDisk = true
			logger.Info("Instance failed permanently, scheduling disk cleanup.")
		} else if nr.globalCtx.Err() != nil { // Runner is shutting down
			shouldCleanupDisk = true
			logger.Info("Runner is shutting down, scheduling disk cleanup.")
		} else {
			// Check if the app config itself was deleted *after* the process exited
			appInfo := nr.state.GetAppInfo(appName)
			appInfo.mu.RLock()
			if appInfo.spec == nil { // Spec is nil only if config was deleted
				shouldCleanupDisk = true
				logger.Info("App config deleted, scheduling disk cleanup.")
			}
			appInfo.mu.RUnlock()
		}

		if shouldCleanupDisk {
			// Remove the instance's data directory
			instanceDir := filepath.Dir(appInstance.Cmd.Dir) // Get parent of workDir (e.g., .../instances/app-0)
			logger.Info("Performing permanent cleanup of instance data directory", "dir", instanceDir)
			if err := os.RemoveAll(instanceDir); err != nil {
				logger.Error("Failed to remove instance data directory", "dir", instanceDir, "error", err)
			}
			// Also remove the in-memory state since it's a permanent stop
			nr.removeInstanceState(appName, instanceID, logger)
			nr.cleanupInstanceMetrics(appInstance) // Cleanup metrics for permanently removed instance
		}
		// If not cleaning up disk (e.g., crash with restart pending, or intentional scale-down),
		// the restart logic or scale-down logic handles the in-memory state removal and metrics.
	}()

	logger.Info("Monitoring process instance", "pid", appInstance.Pid)
	waitErr := appInstance.Cmd.Wait() // Wait for the process (launcher or direct exec)

	// Determine exit details
	exitCode := -1
	intentionalStop := false
	errMsg := ""
	finalStatus := StatusStopped // Default to stopped

	// Update metrics for termination
	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID).Set(0)

	// Get Rusage for CPU/Memory metrics
	if appInstance.Cmd.ProcessState != nil {
		if rusage, ok := appInstance.Cmd.ProcessState.SysUsage().(*syscall.Rusage); ok && rusage != nil {
			var rssBytes float64
			if nr.localOS == "linux" {
				rssBytes = float64(rusage.Maxrss * 1024) // Kilobytes to Bytes
			} else if nr.localOS == "darwin" {
				rssBytes = float64(rusage.Maxrss) // Already in Bytes
			} else {
				logger.Warn("MaxRSS reporting for OS not explicitly handled, may be inaccurate or in unknown units.", "os", nr.localOS, "raw_maxrss", rusage.Maxrss)
				rssBytes = float64(rusage.Maxrss) // Best guess, could be 0 or NaN if value is meaningless
			}
			metrics.NarunNodeRunnerInstanceMemoryMaxRSSBytes.WithLabelValues(appName, instanceID, nr.nodeID).Set(rssBytes)
			metrics.NarunNodeRunnerInstanceCPUUserSecondsTotal.WithLabelValues(appName, instanceID, nr.nodeID).Add(metrics.TimevalToSeconds(rusage.Utime))
			metrics.NarunNodeRunnerInstanceCPUSystemSecondsTotal.WithLabelValues(appName, instanceID, nr.nodeID).Add(metrics.TimevalToSeconds(rusage.Stime))
		} else {
			logger.Warn("Could not get Rusage for process, CPU/Memory metrics will not be updated.", "instance_id", instanceID)
		}
	} else {
		logger.Warn("ProcessState is nil, cannot get Rusage.", "instance_id", instanceID)
	}

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

	if waitErr != nil {
		errMsg = waitErr.Error()
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
			metrics.NarunNodeRunnerInstanceLastExitCode.WithLabelValues(appName, instanceID, nr.nodeID).Set(float64(exitCode))
			// Check if it was the launcher that failed
			isLauncherFailure := false
			if spec.Mode == "landlock" && strings.HasSuffix(appInstance.Cmd.Path, "node-runner") { // Check if we ran the launcher
				switch exitCode {
				case landlockLauncherErrCode, landlockUnsupportedErrCode,
					landlockLockFailedErrCode, landlockTargetExecFailedCode:
					isLauncherFailure = true
					finalStatus = StatusFailed // Launcher errors are fatal for the instance
					logger.Error("Landlock launcher failed", "exit_code", exitCode, "error", errMsg)
					// Make sure intentionalStop is true so we don't try to restart
					intentionalStop = true
				}
			}

			if !isLauncherFailure { // Handle non-launcher exit errors
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
					finalStatus = StatusCrashed // Crashed if exited non-zero unexpectedly
				}
			}
		} else { // Wait failed for other reasons (e.g., start failed, IO error)
			logger.Error("Process wait failed", "error", waitErr)
			finalStatus = StatusFailed
			intentionalStop = true                                                                              // Treat non-exit errors as fatal
			metrics.NarunNodeRunnerInstanceLastExitCode.WithLabelValues(appName, instanceID, nr.nodeID).Set(-1) // Indicate error not from exit code
		}
	} else { // Exited cleanly (code 0)
		exitCode = 0
		logger.Info("Process exited cleanly", "pid", appInstance.Pid)
		finalStatus = StatusStopped
		metrics.NarunNodeRunnerInstanceLastExitCode.WithLabelValues(appName, instanceID, nr.nodeID).Set(0)
	}

	// Update internal state and publish status
	appInstance.Status = finalStatus
	appInstance.lastExitCode = &exitCode
	nr.publishStatusUpdate(instanceID, finalStatus, &appInstance.Pid, &exitCode, errMsg)

	// --- Restart Logic ---
	// No restart if: intentional stop, Failed status (unrecoverable), runner shutting down
	if intentionalStop || finalStatus == StatusFailed || nr.globalCtx.Err() != nil {
		logger.Info("Instance stopped/failed or runner shutting down. No restart.",
			"status", finalStatus, "intentional", intentionalStop, "runner_shutdown", nr.globalCtx.Err() != nil)
		// The defer block will handle necessary cleanup if shouldCleanupDisk becomes true.
		// If it's an intentional stop like scale-down (intentionalStop=true, but not a permanent failure),
		// the calling function (e.g., handleAppConfigUpdate) is responsible for nr.cleanupInstanceMetrics.
		return // Exit monitor
	}

	// Potential restart needed (Crashed or Stopped unexpectedly)
	appInfo := nr.state.GetAppInfo(appName)

	appInfo.mu.RLock()
	configHashMatches := appInfo.configHash == appInstance.ConfigHash
	instanceStillManaged := false
	for _, inst := range appInfo.instances {
		if inst.InstanceID == instanceID {
			instanceStillManaged = true
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

	numericalReplicaIndex := extractReplicaIndex(instanceID)
	if numericalReplicaIndex == -1 {
		logger.Error("Could not determine numerical replica index for restarting instance. Aborting restart.", "instance_id", instanceID)
		return
	}

	// Check restart count
	if appInstance.restartCount >= MaxRestarts {
		logger.Error("Instance crashed too many times. Giving up.", "max_restarts", MaxRestarts)
		appInstance.Status = StatusFailed // Mark as permanently failed
		nr.publishStatusUpdate(instanceID, StatusFailed, &appInstance.Pid, &exitCode, "Exceeded max restarts")
		// Defer will handle cleanup as finalStatus is Failed.
		return // Exit monitor
	}

	// Proceed with restart
	appInstance.restartCount++
	metrics.NarunNodeRunnerInstanceRestartsTotal.WithLabelValues(appName, instanceID, nr.nodeID).Inc()
	logger.Warn("Instance crashed/stopped unexpectedly. Attempting restart.", "restart_count", appInstance.restartCount, "max_restarts", MaxRestarts, "delay", RestartDelay)
	appInstance.Status = StatusStarting // Set status before publishing
	nr.publishStatusUpdate(instanceID, StatusStarting, nil, nil, fmt.Sprintf("Restarting (%d/%d)", appInstance.restartCount, MaxRestarts))

	// Wait for restart delay, cancellable by global shutdown or specific stop signal
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
	case <-appInstance.StopSignal:
		logger.Info("Restart canceled due to explicit stop signal for instance.")
		restartTimer.Stop()
		return
	}

	// Re-acquire lock to perform the restart safely
	appInfo.mu.Lock()
	defer appInfo.mu.Unlock()

	// Double-check conditions after lock and delay
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
		err := nr.startAppInstance(context.Background(), appInfo, numericalReplicaIndex)
		if err != nil {
			logger.Error("Failed to restart application instance", "error", err)
			appInstance.Status = StatusFailed
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, &exitCode, fmt.Sprintf("Restart failed: %v", err))
			// Let defer handle final cleanup as StatusFailed
		} else {
			logger.Info("Instance restarted successfully")
			// Restart succeeded, THIS monitor goroutine's job is done.
			// The NEW instance has its own monitor.
			return
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
	appInfo := nr.state.GetAppInfo(appName)
	appInfo.mu.Lock()
	defer appInfo.mu.Unlock()

	newInstances := make([]*ManagedApp, 0, len(appInfo.instances))
	found := false
	for _, instance := range appInfo.instances {
		if instance.InstanceID == instanceID {
			found = true
			if instance.processCancel != nil {
				instance.processCancel() // Ensure context is cancelled
			}
		} else {
			newInstances = append(newInstances, instance)
		}
	}

	if found {
		appInfo.instances = newInstances
		logger.Info("Removed instance state.", "remaining_instances", len(newInstances))
	} else {
		logger.Warn("Attempted to remove instance state, but it was not found.")
	}
}
