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
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/akhenakh/narun/internal/crypto"
	"github.com/akhenakh/narun/internal/metrics" // Added for metrics
	"github.com/google/uuid"                     // For unique Run IDs
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

func generateRunID() string {
	return uuid.NewString()
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
	currentRunID := generateRunID() // Generate RunID for this execution
	logger := nr.logger.With("app", appName, "instance_id", instanceID, "run_id", currentRunID, "tag", tag, "mode", spec.Mode)
	logger.Info("Attempting to start application instance")

	// Fetch Binary First
	binaryPath, fetchErr := nr.fetchAndStoreBinary(ctx, spec.Tag, appName, appInfo.configHash)
	if fetchErr != nil {
		errMsg := fmt.Sprintf("Binary fetch failed: %v", fetchErr)
		logger.Error(errMsg, "error", fetchErr)
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
		return fmt.Errorf("failed to fetch binary for %s: %w", instanceID, fetchErr)
	}
	logger.Info("Binary downloaded/verified", "path", binaryPath)
	if err := os.Chmod(binaryPath, 0755); err != nil {
		logger.Error("Failed to make binary executable", "path", binaryPath, "error", err)
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("Binary not executable: %v", err))
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
		return fmt.Errorf("failed making binary executable %s: %w", binaryPath, err)
	}

	// Prepare Instance Directory and Working Directory
	instanceDir := filepath.Join(nr.dataDir, "instances", instanceID)
	workDir := filepath.Join(instanceDir, "work")
	if err := os.MkdirAll(workDir, 0755); err != nil {
		logger.Error("Failed to create instance working directory", "dir", workDir, "error", err)
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("Failed work dir: %v", err))
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
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
				metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
				return fmt.Errorf("%s", errMsg)
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
				metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
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
	processEnv = append(processEnv, fmt.Sprintf("NARUN_RUN_ID=%s", currentRunID)) // Add NARUN_RUN_ID to env
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
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
			return fmt.Errorf("%s", errMsg)
		}

		selfPath, err := os.Executable() // Path to the node-runner binary itself
		if err != nil {
			processCancel()
			errMsg := fmt.Sprintf("Failed to get node-runner executable path: %v", err)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
			return fmt.Errorf("%s", errMsg)
		}

		// Serialize config needed by the launcher child process
		landlockConfigJSON, err := json.Marshal(spec.Landlock)
		if err != nil {
			processCancel()
			errMsg := fmt.Sprintf("Failed to marshal landlock config to JSON: %v", err)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
			return fmt.Errorf("%s", errMsg)
		}
		targetArgsJSON, err := json.Marshal(spec.Args)
		if err != nil {
			processCancel()
			errMsg := fmt.Sprintf("Failed to marshal target args to JSON: %v", err)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
			return fmt.Errorf("%s", errMsg)
		}

		// Serialize mount info for the launcher
		mountInfosJSON, err := json.Marshal(mountInfosForEnv)
		if err != nil {
			processCancel()
			errMsg := fmt.Sprintf("Failed to marshal mount info to JSON: %v", err)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
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
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
		return fmt.Errorf("stdout pipe failed for %s: %w", instanceID, err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		processCancel()
		stdoutPipe.Close() // Close previous pipe on error
		logger.Error("Failed to get stderr pipe", "error", err)
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, fmt.Sprintf("stderr pipe failed: %v", err))
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
		return fmt.Errorf("stderr pipe failed for %s: %w", instanceID, err)
	}

	// Create/Update ManagedApp State in the manager
	appInstance := &ManagedApp{
		InstanceID:    instanceID,
		RunID:         currentRunID, // Store the generated RunID
		Spec:          spec,         // Store reference to the spec
		Cmd:           cmd,          // Store the prepared command (launcher or direct exec)
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
	// Find and replace or append logic
	foundAndReplaced := false
	for i, existing := range appInfo.instances {
		if existing.InstanceID == instanceID {
			// If replacing, existing instance is from a previous run or config.
			// Its metrics (with its old RunID) should have been finalized by its monitor.
			logger.Debug("Replacing existing state for instance in appInfo", "index", i, "old_run_id", existing.RunID, "new_run_id", appInstance.RunID)
			if existing.processCancel != nil {
				existing.processCancel() // Cancel old context if replacing
			}
			// No explicit metrics cleanup for 'existing' here, its monitor handled it.
			// We are creating a *new* run.
			appInstance.restartCount = existing.restartCount // Inherit logical restart count for the instanceID
			appInfo.instances[i] = appInstance
			foundAndReplaced = true
			break
		}
	}
	if !foundAndReplaced {
		appInfo.instances = append(appInfo.instances, appInstance)
		logger.Debug("Appending new instance state to appInfo")
	}

	// Publish starting status (for the logical instanceID)
	nr.publishStatusUpdate(instanceID, StatusStarting, nil, nil, "Starting process")

	// Start the Process (either the launcher or the direct application)
	logger.Info("Starting process execution", "command", cmdPath, "args", cmdArgs, "env_count", len(cmd.Env))
	if startErr := cmd.Start(); startErr != nil {
		errMsg := fmt.Sprintf("Failed to start process: %v", startErr)
		logger.Error(errMsg, "error", startErr)
		appInstance.Status = StatusFailed
		appInstance.processCancel() // Ensure context is cancelled
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
		nr.removeInstanceState(appName, instanceID, logger)                                                    // Clean up state if start fails
		nr.cleanupInstanceMetrics(appInstance)                                                                 // Cleanup metrics on start fail for this run
		return fmt.Errorf("failed to start %s: %w", instanceID, startErr)
	}

	// Success Path
	appInstance.Pid = cmd.Process.Pid // Get the PID of the started process
	appInstance.StartTime = time.Now()
	appInstance.Status = StatusRunning
	logger.Info("Process started successfully", "pid", appInstance.Pid)
	nr.publishStatusUpdate(instanceID, StatusRunning, &appInstance.Pid, nil, "")

	// Initialize metrics for successful start of this run
	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(1)
	metrics.NarunNodeRunnerInstanceInfo.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID, spec.Tag, spec.Mode, binaryPath).Set(1)
	metrics.NarunNodeRunnerInstanceCPUUserSecondsTotal.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Add(0)
	metrics.NarunNodeRunnerInstanceCPUSystemSecondsTotal.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Add(0)
	metrics.NarunNodeRunnerInstanceMemoryMaxRSSBytes.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0)
	metrics.NarunNodeRunnerInstanceLastExitCode.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Placeholder

	// Start log forwarding and monitoring goroutines
	appInstance.LogWg.Add(2) // 2 for stdout/stderr
	go nr.forwardLogs(appInstance, appInstance.StdoutPipe, "stdout", logger)
	go nr.forwardLogs(appInstance, appInstance.StderrPipe, "stderr", logger)
	go nr.monitorAppInstance(appInstance, logger) // Monitor handles cmd.Wait()

	return nil
}

// stopAppInstance gracefully stops an application instance process.
func (nr *NodeRunner) stopAppInstance(appInstance *ManagedApp, intentional bool) error {
	instanceID := appInstance.InstanceID
	runID := appInstance.RunID // Use the RunID of the instance being stopped
	appName := InstanceIDToAppName(instanceID)
	logger := nr.logger.With("app", appName, "instance_id", instanceID, "run_id", runID)

	if appInstance.Status == StatusStopped || appInstance.Status == StatusStopping || appInstance.Status == StatusFailed {
		logger.Info("Stop request: Instance not running or already stopping/stopped/failed.", "status", appInstance.Status)
		if appInstance.Status != StatusStopping { // Only set if not already in stopping phase
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(0)
		}
		return nil
	}
	if appInstance.Cmd == nil || appInstance.Cmd.Process == nil {
		logger.Warn("Stop request: Instance state inconsistent (no command/process). Cleaning up state.")
		appInstance.Status = StatusFailed // Mark as failed if state is broken
		if appInstance.processCancel != nil {
			appInstance.processCancel() // Ensure context is cancelled
		}
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, "Inconsistent state during stop")    // Publish failure
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(0) // Mark as down for this run
		return fmt.Errorf("inconsistent state for instance %s (run %s)", instanceID, runID)
	}

	pid := appInstance.Pid
	logger.Info("Stopping application instance process", "pid", pid)
	appInstance.Status = StatusStopping
	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(0) // Mark as down now for this run
	nr.publishStatusUpdate(instanceID, StatusStopping, &pid, nil, "")                               // Logical instance status

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
		return fmt.Errorf("failed to send SIGTERM to %s (PID %d, RunID %s): %w", instanceID, pid, runID, err)
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
		}
		appInstance.processCancel() // Ensure context is cancelled if timeout occurs
	case <-appInstance.processCtx.Done():
		logger.Info("Process exited or context canceled during stop wait.")
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
		return "", fmt.Errorf("decryption failed for secret '%s'", secretName)
	}

	return string(plaintextBytes), nil
}

// forwardLogs reads logs from a pipe and publishes them to NATS.
func (nr *NodeRunner) forwardLogs(appInstance *ManagedApp, pipe io.ReadCloser, streamName string, logger *slog.Logger) {
	defer appInstance.LogWg.Done()
	defer pipe.Close()
	scanner := bufio.NewScanner(pipe)
	const initialBufSize = 4 * 1024
	const maxBufSize = 64 * 1024
	buf := make([]byte, initialBufSize)
	scanner.Buffer(buf, maxBufSize)

	logger = logger.With("stream", streamName, "run_id", appInstance.RunID) // Add run_id to log context
	logger.Debug("Starting log forwarding")
	appName := InstanceIDToAppName(appInstance.InstanceID)
	// NATS subject for logs does not need run_id, it's for the logical instance
	subject := fmt.Sprintf("%s.%s.%s", LogSubjectPrefix, appName, nr.nodeID)

	for scanner.Scan() {
		line := scanner.Text()
		// Log entry also does not need run_id as it is part of the NATS message,
		// and consumers of these logs might correlate by instance_id.
		logMsg := logEntry{InstanceID: appInstance.InstanceID, AppName: appName, NodeID: nr.nodeID, Stream: streamName, Message: line, Timestamp: time.Now()}
		logData, err := json.Marshal(logMsg)
		if err != nil {
			logger.Warn("Failed to marshal log entry", "error", err)
			continue
		}
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

// monitorAppInstance waits for an app instance to exit and handles restarts or cleanup.
func (nr *NodeRunner) monitorAppInstance(appInstance *ManagedApp, logger *slog.Logger) {
	instanceID := appInstance.InstanceID
	runID := appInstance.RunID // Capture RunID for this specific monitored run
	appName := InstanceIDToAppName(instanceID)
	spec := appInstance.Spec

	logger = logger.With("run_id", runID) // Add RunID to monitor's logger context

	// Start periodic memory polling goroutine if on Linux
	if runtime.GOOS == "linux" {
		// This goroutine will periodically update the memory metric.
		// It's managed by the appInstance.processCtx.
		appInstance.LogWg.Add(1) // Add to a wait group to ensure it exits cleanly
		go func() {
			defer appInstance.LogWg.Done()
			pollTicker := time.NewTicker(5 * time.Second)
			defer pollTicker.Stop()
			logger.Debug("Starting periodic memory metric poller for process")
			for {
				select {
				case <-pollTicker.C:
					// Check if process is still considered running before polling
					if appInstance.Status == StatusRunning && appInstance.Pid > 0 {
						nr.doPollMemoryAndUpdateMetric(appInstance, logger)
					}
				case <-appInstance.processCtx.Done():
					logger.Debug("Stopping periodic memory metric poller due to process context done")
					return
				}
			}
		}()
	}

	defer func() {
		appInstance.processCancel() // This will also stop the memory poller goroutine
		appInstance.LogWg.Wait()    // Wait for log forwarders AND memory poller
		logger.Debug("Monitor finished and auxiliary goroutines (logs, memory poller) completed")

		shouldCleanupDisk := false
		finalStatus := appInstance.Status

		if finalStatus == StatusFailed {
			shouldCleanupDisk = true
			logger.Info("Instance run failed permanently, scheduling disk cleanup.")
		} else if nr.globalCtx.Err() != nil {
			shouldCleanupDisk = true
			logger.Info("Runner is shutting down, scheduling disk cleanup.")
		} else {
			appInfo := nr.state.GetAppInfo(appName)
			appInfo.mu.RLock()
			if appInfo.spec == nil {
				shouldCleanupDisk = true
				logger.Info("App config deleted, scheduling disk cleanup.")
			}
			appInfo.mu.RUnlock()
		}

		if shouldCleanupDisk {
			instanceDir := filepath.Dir(appInstance.Cmd.Dir)
			logger.Info("Performing permanent cleanup of instance data directory", "dir", instanceDir)
			if err := os.RemoveAll(instanceDir); err != nil {
				logger.Error("Failed to remove instance data directory", "dir", instanceDir, "error", err)
			}
			nr.removeInstanceState(appName, instanceID, logger)
			// With RunID, we don't delete metrics. cleanupInstanceMetrics ensures _up=0 for this run.
			nr.cleanupInstanceMetrics(appInstance)
		}
	}()

	logger.Info("Monitoring process instance", "pid", appInstance.Pid)
	waitErr := appInstance.Cmd.Wait()

	exitCode := -1
	intentionalStop := false
	errMsg := ""
	finalStatus := StatusStopped

	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(0)

	if appInstance.Cmd.ProcessState != nil {
		if rusage, ok := appInstance.Cmd.ProcessState.SysUsage().(*syscall.Rusage); ok && rusage != nil {
			var rssBytes float64
			if nr.localOS == "linux" {
				rssBytes = float64(rusage.Maxrss * 1024)
			} else if nr.localOS == "darwin" {
				rssBytes = float64(rusage.Maxrss)
			} else {
				logger.Warn("MaxRSS reporting for OS not explicitly handled", "os", nr.localOS, "raw_maxrss", rusage.Maxrss)
				rssBytes = float64(rusage.Maxrss)
			}
			logger.Debug("Rusage details on process exit",
				"Maxrss_raw", rusage.Maxrss, "converted_rss_bytes", rssBytes,
				"Utime_sec", rusage.Utime.Sec, "Utime_usec", rusage.Utime.Usec,
				"Stime_sec", rusage.Stime.Sec, "Stime_usec", rusage.Stime.Usec)

			metrics.NarunNodeRunnerInstanceMemoryMaxRSSBytes.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(rssBytes)
			metrics.NarunNodeRunnerInstanceCPUUserSecondsTotal.WithLabelValues(appName, instanceID, nr.nodeID, runID).Add(metrics.TimevalToSeconds(rusage.Utime))
			metrics.NarunNodeRunnerInstanceCPUSystemSecondsTotal.WithLabelValues(appName, instanceID, nr.nodeID, runID).Add(metrics.TimevalToSeconds(rusage.Stime))
		} else {
			logger.Warn("Could not get Rusage for process")
		}
	} else {
		logger.Warn("ProcessState is nil, cannot get Rusage.")
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
			metrics.NarunNodeRunnerInstanceLastExitCode.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(float64(exitCode))
			isLauncherFailure := false
			if spec != nil && spec.Mode == "landlock" && strings.HasSuffix(appInstance.Cmd.Path, "node-runner") {
				switch exitCode {
				case landlockLauncherErrCode, landlockUnsupportedErrCode,
					landlockLockFailedErrCode, landlockTargetExecFailedCode:
					isLauncherFailure = true
					finalStatus = StatusFailed
					logger.Error("Landlock launcher failed", "exit_code", exitCode, "error", errMsg)
					intentionalStop = true
				}
			}
			if !isLauncherFailure {
				if status, okSys := exitErr.Sys().(syscall.WaitStatus); okSys && status.Signaled() {
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
			}
		} else {
			logger.Error("Process wait failed", "error", waitErr)
			finalStatus = StatusFailed
			intentionalStop = true
			metrics.NarunNodeRunnerInstanceLastExitCode.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(-1)
		}
	} else {
		exitCode = 0
		logger.Info("Process exited cleanly", "pid", appInstance.Pid)
		finalStatus = StatusStopped
		metrics.NarunNodeRunnerInstanceLastExitCode.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(0)
	}

	appInstance.Status = finalStatus
	appInstance.lastExitCode = &exitCode
	nr.publishStatusUpdate(instanceID, finalStatus, &appInstance.Pid, &exitCode, errMsg)

	if intentionalStop || finalStatus == StatusFailed || nr.globalCtx.Err() != nil {
		logger.Info("Instance run stopped/failed or runner shutting down. No restart for this run.",
			"status", finalStatus, "intentional", intentionalStop, "runner_shutdown", nr.globalCtx.Err() != nil)
		return
	}

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
		logger.Info("Configuration changed since instance run started. No restart.", "old_hash", appInstance.ConfigHash, "new_hash", appInfo.configHash)
		return
	}

	numericalReplicaIndex := extractReplicaIndex(instanceID)
	if numericalReplicaIndex == -1 {
		logger.Error("Could not determine numerical replica index for restarting instance. Aborting restart.", "instance_id", instanceID)
		return
	}

	if appInstance.restartCount >= MaxRestarts {
		logger.Error("Instance crashed too many times. Giving up.", "max_restarts", MaxRestarts)
		appInstance.Status = StatusFailed
		nr.publishStatusUpdate(instanceID, StatusFailed, &appInstance.Pid, &exitCode, "Exceeded max restarts")
		return
	}

	// This increments the restart count for the *logical* instance, not the run.
	// The NarunNodeRunnerInstanceRestartsTotal metric is also for the logical instance.
	appInstance.restartCount++
	metrics.NarunNodeRunnerInstanceRestartsTotal.WithLabelValues(appName, instanceID, nr.nodeID).Inc()
	logger.Warn("Instance run crashed/stopped unexpectedly. Attempting restart of logical instance.", "restart_count", appInstance.restartCount, "max_restarts", MaxRestarts, "delay", RestartDelay)
	appInstance.Status = StatusStarting // This status is for the *next* run
	nr.publishStatusUpdate(instanceID, StatusStarting, nil, nil, fmt.Sprintf("Restarting (%d/%d)", appInstance.restartCount, MaxRestarts))

	restartTimer := time.NewTimer(RestartDelay)
	select {
	case <-restartTimer.C:
	case <-nr.globalCtx.Done():
		logger.Info("Restart canceled due to runner shutdown.")
		restartTimer.Stop()
		return
	case <-appInstance.processCtx.Done(): // Should be current run's context
		logger.Warn("Instance run context cancelled during restart delay?")
		restartTimer.Stop()
		return
	case <-appInstance.StopSignal: // Should be current run's stop signal
		logger.Info("Restart canceled due to explicit stop signal for instance run.")
		restartTimer.Stop()
		return
	}

	appInfo.mu.Lock()
	defer appInfo.mu.Unlock()

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
		// Attempt the restart, this will create a new ManagedApp with a new RunID.
		err := nr.startAppInstance(context.Background(), appInfo, numericalReplicaIndex)
		if err != nil {
			logger.Error("Failed to restart application instance", "error", err)
			// The failed startAppInstance will set its own status to Failed for its new RunID.
			// This current monitor, for the old RunID, has already set its status.
		} else {
			logger.Info("Instance restarted successfully (new run started)")
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
	if err := nr.nc.Publish(subject, updateData); err != nil {
		nr.logger.Warn("Failed to publish status update to NATS", "instance_id", instanceID, "subject", subject, "error", err)
	}
}

// fetchAndStoreBinary downloads the binary from NATS Object Store for the node's platform.
// It uses the object's digest to version the local storage path.
func (nr *NodeRunner) fetchAndStoreBinary(ctx context.Context, tag, appName, configHash string) (string, error) {
	targetObjectName := fmt.Sprintf("%s-%s-%s", tag, nr.localOS, nr.localArch)
	logger := nr.logger.With("app", appName, "tag", tag, "object", targetObjectName)

	objInfo, getInfoErr := nr.appBinaries.GetInfo(ctx, targetObjectName)
	if getInfoErr != nil {
		if errors.Is(getInfoErr, jetstream.ErrObjectNotFound) {
			return "", fmt.Errorf("binary object '%s' for platform %s/%s not found in object store: %w", targetObjectName, nr.localOS, nr.localArch, getInfoErr)
		}
		return "", fmt.Errorf("failed to get info for binary object '%s': %w", targetObjectName, getInfoErr)
	}

	versionID := objInfo.Digest
	if versionID == "" {
		logger.Error("Object store info is missing digest", "object", targetObjectName)
		return "", fmt.Errorf("binary object '%s' info is missing digest", targetObjectName)
	}
	if !strings.HasPrefix(versionID, "SHA-256=") {
		return "", fmt.Errorf("unsupported digest format (expected 'SHA-256=' prefix): %s", versionID)
	}
	versionID = strings.TrimPrefix(versionID, "SHA-256=")
	if versionID == "" {
		logger.Error("Object store digest value is empty after parsing", "object", targetObjectName, "original_digest", objInfo.Digest)
		return "", fmt.Errorf("binary object '%s' digest value is empty", targetObjectName)
	}

	safeAppName := sanitizePathElement(appName)
	safeVersionID := sanitizePathElement(versionID)
	safeObjectName := sanitizePathElement(targetObjectName)
	versionedDir := filepath.Join(nr.dataDir, "binaries", safeAppName, safeVersionID)
	localPath := filepath.Join(versionedDir, safeObjectName)

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

	logger.Info("Downloading specific binary version", "version_id", versionID, "local_path", localPath)
	if err := os.MkdirAll(versionedDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create versioned binary directory %s: %w", versionedDir, err)
	}
	tempLocalPath := localPath + ".tmp." + fmt.Sprintf("%d", time.Now().UnixNano())
	outFile, err := os.Create(tempLocalPath)
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file %s: %w", tempLocalPath, err)
	}
	var finalErr error
	defer func() {
		if outFile != nil {
			outFile.Close()
		}
		if finalErr != nil {
			logger.Warn("Removing temporary file due to error", "path", tempLocalPath, "error", finalErr)
			if remErr := os.Remove(tempLocalPath); remErr != nil && !errors.Is(remErr, os.ErrNotExist) {
				logger.Error("Failed to remove temporary download file", "path", tempLocalPath, "error", remErr)
			}
		}
	}()

	objResult, err := nr.appBinaries.Get(ctx, targetObjectName)
	if err != nil {
		finalErr = fmt.Errorf("failed to get object reader for %s: %w", targetObjectName, err)
		return "", finalErr
	}
	defer objResult.Close()

	bytesCopied, err := io.Copy(outFile, objResult)
	if err != nil {
		finalErr = fmt.Errorf("failed to copy object data to %s: %w", tempLocalPath, err)
		return "", finalErr
	}
	logger.Debug("Binary data copied", "temp_path", tempLocalPath, "bytes_copied", bytesCopied)

	if syncErr := outFile.Sync(); syncErr != nil {
		logger.Error("Failed to sync temporary file", "path", tempLocalPath, "error", syncErr)
	}
	if closeErr := outFile.Close(); closeErr != nil {
		outFile = nil
		finalErr = fmt.Errorf("failed to close temporary file %s: %w", tempLocalPath, closeErr)
		return "", finalErr
	}
	outFile = nil

	hashMatches, verifyErr := verifyLocalFileHash(tempLocalPath, objInfo.Digest)
	if verifyErr != nil || !hashMatches {
		errMsg := fmt.Sprintf("hash verification failed (err: %v, match: %v)", verifyErr, hashMatches)
		logger.Error(errMsg, "path", tempLocalPath, "expected_digest", objInfo.Digest)
		finalErr = fmt.Errorf("hash verification failed for %s: %s", tempLocalPath, errMsg)
		return "", finalErr
	}

	logger.Debug("Verification successful, renaming", "from", tempLocalPath, "to", localPath)
	if err = os.Rename(tempLocalPath, localPath); err != nil {
		logger.Error("Failed to rename temporary binary file", "temp", tempLocalPath, "final", localPath, "error", err)
		finalErr = fmt.Errorf("failed to finalize binary download for %s: %w", localPath, err)
		_ = os.Remove(tempLocalPath)
		return "", finalErr
	}

	logger.Info("Binary version downloaded and verified successfully", "path", localPath)
	return localPath, nil
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

	actualBase64Hash := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(hashBytes)
	actualHexHash := fmt.Sprintf("%x", hashBytes)

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
	var sb strings.Builder
	sb.Grow(len(element))

	for _, r := range element {
		if ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || ('0' <= r && r <= '9') || r == '-' || r == '_' || r == '.' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}

	sanitized := sb.String()
	const maxLen = 100
	if len(sanitized) > maxLen {
		sanitized = sanitized[:maxLen]
	}
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
				instance.processCancel()
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

// doPollMemoryAndUpdateMetric polls memory usage for a running instance (Linux specific for now)
// and updates the Prometheus gauge.
func (nr *NodeRunner) doPollMemoryAndUpdateMetric(instance *ManagedApp, logger *slog.Logger) {
	if instance.Cmd == nil || instance.Cmd.Process == nil || instance.Pid <= 0 {
		logger.Debug("Skipping memory poll, process or PID not valid", "pid", instance.Pid)
		return
	}

	pid := instance.Pid
	var VmHWMkB int64 = -1 // VmHWM (Peak resident set size) in KB

	// This logic is Linux-specific, reading /proc/[pid]/status
	if runtime.GOOS == "linux" {
		filePath := fmt.Sprintf("/proc/%d/status", pid)
		file, err := os.Open(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				logger.Debug("Failed to open proc status file (process likely exited while polling)", "pid", pid, "path", filePath, "error", err)
			} else {
				logger.Warn("Failed to open proc status file", "pid", pid, "path", filePath, "error", err)
			}
			return
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "VmHWM:") {
				parts := strings.Fields(line)
				if len(parts) >= 2 {
					val, parseErr := strconv.ParseInt(parts[1], 10, 64)
					if parseErr == nil {
						VmHWMkB = val
						break
					} else {
						logger.Warn("Failed to parse VmHWM value from proc status", "pid", pid, "line", line, "error", parseErr)
					}
				}
			}
		}
		if err := scanner.Err(); err != nil {
			logger.Warn("Error scanning proc status file", "pid", pid, "path", filePath, "error", err)
			return
		}
	} else {
		// For non-Linux, we don't poll live memory this way.
		// The final Rusage will provide MaxRSS on exit.
		return
	}

	if VmHWMkB != -1 {
		memoryBytes := float64(VmHWMkB * 1024) // Convert KB to Bytes
		metrics.NarunNodeRunnerInstanceMemoryMaxRSSBytes.WithLabelValues(
			InstanceIDToAppName(instance.InstanceID),
			instance.InstanceID,
			nr.nodeID,
			instance.RunID,
		).Set(memoryBytes)
		logger.Debug("Updated live memory (VmHWM) metric", "pid", pid, "VmHWM_kB", VmHWMkB, "bytes", memoryBytes)
	} else {
		// VmHWM not found, or not Linux. Log less verbosely or not at all.
		// logger.Debug("VmHWM not found in proc status or not on Linux, memory metric not updated this cycle", "pid", pid)
	}
}
