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
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/akhenakh/narun/internal/crypto"
	"github.com/akhenakh/narun/internal/metrics" // Added for metrics
	"github.com/google/uuid"                     // For unique Run IDs
	"github.com/hashicorp/go-set/v2"
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
	internalLaunchFlag           = "--internal-launch"
	envLandlockConfigJSON        = "NARUN_INTERNAL_LANDLOCK_CONFIG_JSON"
	envLandlockTargetCmd         = "NARUN_INTERNAL_LANDLOCK_TARGET_CMD"
	envLandlockTargetArgsJSON    = "NARUN_INTERNAL_LANDLOCK_TARGET_ARGS_JSON"
	envLandlockInstanceRoot      = "NARUN_INTERNAL_INSTANCE_ROOT"
	envLandlockMountInfosJSON    = "NARUN_INTERNAL_MOUNT_INFOS_JSON"
	envLandlockLocalPorts        = "NARUN_INTERNAL_LOCAL_PORTS"    // network ports
	envLandlockMetricsConfig     = "NARUN_INTERNAL_METRICS_CONFIG" // for metrics
	landlockLauncherErrCode      = 120                             // Generic launcher setup error
	landlockUnsupportedErrCode   = 121                             // Landlock not supported on OS/Kernel
	landlockLockFailedErrCode    = 122                             // l.Lock() failed
	landlockTargetExecFailedCode = 123                             // syscall.Exec() failed
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

// Helper to check if cgroup based resource limiting should be attempted
func (nr *NodeRunner) usesCgroupsForResourceLimits(spec *ServiceSpec) bool {
	return runtime.GOOS == "linux" && spec.CgroupParent != "" && (spec.MemoryMB > 0 || spec.CPUCores > 0)
}

// Helper to check if namespacing (user, pid, ipc) should be attempted
func (nr *NodeRunner) usesNamespacing(spec *ServiceSpec) bool {
	return runtime.GOOS == "linux" && (spec.User != "" || spec.NetworkNamespacePath != "")
	// PID and IPC namespaces are generally good to use with User or Network NS.
}

// lookupUser finds UID, GID, and home directory for a username.
func lookupUser(username string) (uid, gid uint32, homeDir string, err error) {
	u, err := user.Lookup(username)
	if err != nil {
		return 0, 0, "", fmt.Errorf("failed to find user %q: %w", username, err)
	}
	uid64, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		return 0, 0, "", fmt.Errorf("failed to parse uid for user %q: %w", username, err)
	}
	gid64, err := strconv.ParseUint(u.Gid, 10, 32)
	if err != nil {
		return 0, 0, "", fmt.Errorf("failed to parse gid for user %q: %w", username, err)
	}
	return uint32(uid64), uint32(gid64), u.HomeDir, nil
}

// flattenEnv prepares environment variables, similar to pledge's logic.
func flattenEnv(baseEnv []string, username, homeDir string, appEnv []EnvVar) []string {
	// Inspired by pledge.flatten
	useless := set.From([]string{"LS_COLORS", "XAUTHORITY", "DISPLAY", "COLORTERM", "MAIL"}) // TMPDIR handled separately

	// Convert baseEnv (slice) to map for easier manipulation
	envMap := make(map[string]string)
	for _, e := range baseEnv {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		} else {
			envMap[parts[0]] = "" // Variable without value
		}
	}

	// Override/add with app-specific env vars (already resolved from secrets by caller)
	for _, ae := range appEnv {
		envMap[ae.Name] = ae.Value // Value is already resolved string
	}

	var result []string
	for k, v := range envMap {
		switch {
		case k == "USER" && username != "":
			result = append(result, "USER="+username)
		case k == "HOME" && homeDir != "":
			result = append(result, "HOME="+homeDir)
		case useless.Contains(k):
			continue
		case v == "":
			result = append(result, k)
		default:
			result = append(result, k+"="+v)
		}
	}

	// Ensure USER and HOME are set if not overridden
	if _, ok := envMap["USER"]; !ok && username != "" {
		result = append(result, "USER="+username)
	}
	if _, ok := envMap["HOME"]; !ok && homeDir != "" {
		result = append(result, "HOME="+homeDir)
	}

	// Ensure TMPDIR is set and sensible
	result = append(result, "TMPDIR="+os.TempDir()) // Or instance-specific temp dir
	return result
}

// Helper to find a free port on the host
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// startAppInstance attempts to download the binary, configure, and start a single instance.
// It assumes the calling code (handleAppConfigUpdate) holds the lock on appInfo.
func (nr *NodeRunner) startAppInstance(ctx context.Context, appInfo *appInfo, replicaIndex int) (returnedErr error) {
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
	binaryPath, fetchErr := nr.fetchAndStoreBinary(ctx, tag, appName, appInfo.configHash)
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

	// Handle Mounts (Fetch files *before* preparing command env/args)
	mountInfosForEnv := make([]MountInfoForEnv, 0, len(spec.Mounts))
	for _, mount := range spec.Mounts {
		if mount.Source.ObjectStore != "" {
			err := nr.fetchAndPlaceFile(ctx, mount.Source.ObjectStore, workDir, mount.Path, logger)
			if err != nil {
				errMsg := fmt.Sprintf("Failed to process mount %s: %v", mount.Path, err)
				logger.Error(errMsg, "source_object", mount.Source.ObjectStore)
				nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
				metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
				return fmt.Errorf("failed to process mount %s: %w", mount.Path, err)
			}
			absMountPath := filepath.Join(workDir, filepath.Clean(mount.Path))
			mountInfosForEnv = append(mountInfosForEnv, MountInfoForEnv{Path: mount.Path, Source: mount.Source.ObjectStore, ResolvedAbs: absMountPath})
		}
	}
	logger.Info("File mounts processed successfully", "count", len(spec.Mounts))

	processCtx, processCancel := context.WithCancel(nr.globalCtx)

	// Handle Network (LocalPorts & Metrics)
	var auxCmds []*exec.Cmd
	var localPortsList []PortForward // Guest -> Host traffic
	var hostMetricsPort int          // Host -> Guest traffic

	// Check if we need the sockets directory (either for NoNet ports OR Metrics)
	needsSocketDir := len(spec.NoNet.LocalPorts) > 0 || spec.Metrics.Port > 0

	if needsSocketDir {
		socketsDir := filepath.Join(instanceDir, "sockets")
		if err := os.MkdirAll(socketsDir, 0755); err != nil {
			processCancel()
			return fmt.Errorf("failed to create sockets dir: %w", err)
		}
		if err := os.Chmod(socketsDir, 0777); err != nil {
			processCancel()
			return fmt.Errorf("failed to chmod sockets dir: %w", err)
		}

		// Mount socket dir (Common for both)
		guestSocketDir := socketsDir
		mountInfosForEnv = append(mountInfosForEnv, MountInfoForEnv{
			Path:        guestSocketDir, // Guest sees this path (absolute path on host)
			ResolvedAbs: socketsDir,     // Host actual path
			Source:      "local-sockets",
		})

		// Handle Outbound (Guest -> Host) Ports
		for _, pf := range spec.NoNet.LocalPorts {
			port := pf.Port
			proto := pf.Protocol // "tcp" or "udp" verified in config

			// Socket naming convention: port_proto.sock
			socketPath := filepath.Join(socketsDir, fmt.Sprintf("%d_%s.sock", port, proto))
			_ = os.Remove(socketPath)

			// Determine Socat Address Type
			targetAddr := fmt.Sprintf("TCP:127.0.0.1:%d", port)
			if proto == "udp" {
				targetAddr = fmt.Sprintf("UDP:127.0.0.1:%d", port)
			}

			// Host-side: Listen on Unix Socket -> Forward to Host Port
			hostSocatArgs := []string{fmt.Sprintf("UNIX-LISTEN:%s,fork,mode=777", socketPath), targetAddr}
			socatCmd := exec.CommandContext(processCtx, "socat", hostSocatArgs...)

			socatCmd.Stderr = nr.newLogWriter(logger.With("component", "socat-outbound", "port", port))

			if err := socatCmd.Start(); err != nil {
				logger.Error("Failed to start host-side socat", "port", port, "error", err)
				processCancel()
				return fmt.Errorf("failed to start host proxy for port %d: %w", port, err)
			}
			auxCmds = append(auxCmds, socatCmd)
			localPortsList = append(localPortsList, pf)
		}

		// Handle Inbound Metrics (Host -> Guest)
		if spec.Metrics.Port > 0 {
			// Pick ephemeral port on host
			freePort, err := getFreePort()
			if err != nil {
				processCancel()
				return fmt.Errorf("failed to get free port for metrics: %w", err)
			}
			hostMetricsPort = freePort

			// Socket path: metrics.sock
			socketPath := filepath.Join(socketsDir, "metrics.sock")
			_ = os.Remove(socketPath)

			socketPathHost := filepath.Join(socketsDir, "metrics_host.sock")
			// Ensure clean state, though Guest will recreate
			_ = os.Remove(socketPathHost)

			// Host side: Expose TCP, write to Unix Socket
			hostSocatArgs := []string{
				fmt.Sprintf("TCP-LISTEN:%d,fork,bind=0.0.0.0,reuseaddr", hostMetricsPort),
				fmt.Sprintf("UNIX-CONNECT:%s,retry=10,interval=1", socketPathHost),
			}

			socatCmd := exec.CommandContext(processCtx, "socat", hostSocatArgs...)

			// Capture stderr for debugging
			socatCmd.Stderr = nr.newLogWriter(logger.With("component", "socat-metrics"))

			if err := socatCmd.Start(); err != nil {
				logger.Error("Failed to start host-side metrics socat", "error", err)
				processCancel()
				return fmt.Errorf("failed to start host proxy for metrics port %d: %w", spec.Metrics.Port, err)
			}
			auxCmds = append(auxCmds, socatCmd)
			logger.Info("Started host-side metrics proxy", "host_port", hostMetricsPort, "socket", socketPathHost)
		}
	}

	// User and Group ID resolution
	var targetUID, targetGID uint32
	var targetHomeDir string
	currentUser, _ := user.Current() // Fallback to node-runner's user

	if spec.User != "" {
		var err error
		targetUID, targetGID, targetHomeDir, err = lookupUser(spec.User)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to lookup user '%s': %v", spec.User, err)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0)
			processCancel()
			return fmt.Errorf(errMsg)
		}
		logger.Info("Target user resolved", "user", spec.User, "uid", targetUID, "gid", targetGID)
	} else {
		uid64, _ := strconv.ParseUint(currentUser.Uid, 10, 32)
		gid64, _ := strconv.ParseUint(currentUser.Gid, 10, 32)
		targetUID = uint32(uid64)
		targetGID = uint32(gid64)
		targetHomeDir = currentUser.HomeDir
		logger.Debug("Using node-runner's user for process", "uid", targetUID, "gid", targetGID)
	}

	// Prepare Environment Variables (including resolved secrets) for the *target application*
	resolvedAppEnv := make([]EnvVar, len(spec.Env))
	for i, envVar := range spec.Env {
		resolvedAppEnv[i] = envVar // Copy name
		if envVar.ValueFromSecret != "" {
			val, err := nr.resolveSecret(ctx, envVar.ValueFromSecret, logger)
			if err != nil {
				logger.Error("Failed to resolve secret for env var", "env_var", envVar.Name, "error", err)
				errMsg := fmt.Sprintf("Failed to resolve secret '%s': %v", envVar.ValueFromSecret, err)
				nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
				metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
				processCancel()
				return fmt.Errorf("failed to resolve secret '%s': %w", envVar.ValueFromSecret, err)
			}
			resolvedAppEnv[i].Value = val
		} else {
			resolvedAppEnv[i].Value = envVar.Value
		}
	}
	baseOsEnv := os.Environ()
	// For landlock launcher env, not the intermediate unshare/nsenter
	// These will be inherited by the target app via the launcher.
	launcherTargetEnv := flattenEnv(baseOsEnv, spec.User, targetHomeDir, resolvedAppEnv)
	launcherTargetEnv = append(launcherTargetEnv,
		fmt.Sprintf("NARUN_APP_NAME=%s", appName),
		fmt.Sprintf("NARUN_INSTANCE_ID=%s", instanceID),
		fmt.Sprintf("NARUN_RUN_ID=%s", currentRunID),
		fmt.Sprintf("NARUN_REPLICA_INDEX=%d", replicaIndex),
		fmt.Sprintf("NARUN_NODE_ID=%s", nr.nodeID),
		fmt.Sprintf("NARUN_INSTANCE_ROOT=%s", workDir), // This is for the target app
		fmt.Sprintf("NARUN_NODE_OS=%s", nr.localOS),
		fmt.Sprintf("NARUN_NODE_ARCH=%s", nr.localArch),
	)

	// For landlock launcher specific setup
	selfPath, err := os.Executable()
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get node-runner executable path: %v", err)
		logger.Error(errMsg)
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0) // Mark as down for this run
		processCancel()
		return fmt.Errorf("failed to get node-runner exec path: %w", err)
	}
	landlockConfigJSON, _ := json.Marshal(spec.Landlock)
	targetArgsJSON, _ := json.Marshal(spec.Args)
	mountInfosJSON, _ := json.Marshal(mountInfosForEnv)

	// Environment for the landlock launcher process itself
	// It needs these to configure landlock and then exec the target.
	// The launcher itself will run as the node-runner user initially, then landlock execs target.
	// If unshare is used, unshare drops privs *before* landlock launcher.
	envForLauncher := slices.Clone(launcherTargetEnv) // Start with target env, then add launcher specifics
	envForLauncher = append(envForLauncher,
		fmt.Sprintf("%s=%s", envLandlockConfigJSON, string(landlockConfigJSON)),
		fmt.Sprintf("%s=%s", envLandlockTargetCmd, binaryPath), // Target app binary
		fmt.Sprintf("%s=%s", envLandlockTargetArgsJSON, string(targetArgsJSON)),
		fmt.Sprintf("%s=%s", envLandlockMountInfosJSON, string(mountInfosJSON)),
		fmt.Sprintf("%s=%s", envLandlockInstanceRoot, workDir),
		fmt.Sprintf("NARUN_TARGET_UID=%d", targetUID), // Pass Target UID/GID to Launcher via Environment
		fmt.Sprintf("NARUN_TARGET_GID=%d", targetGID),
	)
	// Pass network config to launcher as JSON
	if len(localPortsList) > 0 {
		portsJSON, _ := json.Marshal(localPortsList)
		envForLauncher = append(envForLauncher, fmt.Sprintf("%s=%s", envLandlockLocalPorts, string(portsJSON)))
	}

	if hostMetricsPort > 0 {
		// We assume the socket directory is already mounted at same path
		socketPathHost := filepath.Join(filepath.Join(instanceDir, "sockets"), "metrics_host.sock")

		metricsConfig := map[string]interface{}{
			"socket":     socketPathHost,
			"targetPort": spec.Metrics.Port,
		}
		metricsJSON, _ := json.Marshal(metricsConfig)
		envForLauncher = append(envForLauncher, fmt.Sprintf("%s=%s", envLandlockMetricsConfig, string(metricsJSON)))
	}

	// Cgroup Setup (Platform specific)
	var cgroupPath string
	var cgroupFd int = -1
	var cgroupCleanupFunc func() error // Changed to return error

	if nr.usesCgroupsForResourceLimits(spec) {
		var cgErr error
		// Abstracted Cgroup creation
		cgroupPath, cgroupFd, cgroupCleanupFunc, cgErr = nr.createCgroupPlatform(spec, instanceID, logger)
		if cgErr != nil {
			errMsg := fmt.Sprintf("Failed to create cgroup: %v", cgErr)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0)
			processCancel()
			return fmt.Errorf(errMsg)
		}
		// Defer cleanup only if cgroup was successfully created and we might fail later in start
		defer func() {
			if returnedErr != nil && cgroupCleanupFunc != nil {
				logger.Warn("Cleaning up cgroup due to start failure", "error", returnedErr)
				if err := cgroupCleanupFunc(); err != nil {
					logger.Error("Error during cgroup cleanup on start failure", "cgroup_path", cgroupPath, "error", err)
				}
			}
		}()
		// Abstracted Cgroup Constraints application
		if err := nr.applyCgroupConstraintsPlatform(spec, cgroupPath, logger); err != nil {
			errMsg := fmt.Sprintf("Failed to apply cgroup constraints: %v", err)
			processCancel()
			// cgroupCleanupFunc will be called by the defer above
			return fmt.Errorf(errMsg)
		}
	}

	// Command Assembly (Platform Specific)
	cmd, err := nr.configureCmdPlatform(processCtx, spec, workDir, envForLauncher, selfPath, binaryPath, cgroupFd, cgroupPath, logger)
	if err != nil {
		processCancel()
		return fmt.Errorf("failed to configure platform command: %w", err)
	}

	// Pipes
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil { /* ... handle error, cancel context, cleanup cgroup if any ... */
		processCancel()
		return err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil { /* ... handle error, cancel context, cleanup cgroup if any ... */
		processCancel()
		return err
	}

	// Create/Update ManagedApp State in the manager

	appInstance := &ManagedApp{
		InstanceID:      instanceID,
		RunID:           currentRunID,
		Spec:            spec,
		Cmd:             cmd, // Store the top-level command (nsenter/unshare/launcher)
		AuxCmds:         auxCmds,
		Status:          StatusStarting,
		ConfigHash:      appInfo.configHash,
		BinaryPath:      binaryPath, // Path to the *actual application binary*
		StdoutPipe:      stdoutPipe,
		StderrPipe:      stderrPipe,
		StopSignal:      make(chan struct{}),
		processCtx:      processCtx,
		processCancel:   processCancel,
		restartCount:    0,
		HostMetricsPort: hostMetricsPort,
		// Cgroup info
		cgroupPath:    cgroupPath,
		cgroupFd:      cgroupFd, // Store the cgroup FD
		cgroupCleanup: cgroupCleanupFunc,
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
	logger.Info("Starting process execution", "command", cmd.Path, "args", cmd.Args)

	if startErr := cmd.Start(); startErr != nil {
		returnedErr = fmt.Errorf("failed to start %s: %w", instanceID, startErr) // Set returnedErr for defer
		logger.Error("Failed to start process", "error", returnedErr)
		appInstance.Status = StatusFailed
		appInstance.processCancel()
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, returnedErr.Error())
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0)
		nr.removeInstanceState(appName, instanceID, logger)
		nr.cleanupInstanceMetrics(appInstance)
		// The defer for cgroupCleanupFunc will run here if cgroup was created
		return returnedErr
	}

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
		// Even if stopped, ensure aux commands are killed
		for _, c := range appInstance.AuxCmds {
			if c.Process != nil {
				_ = c.Process.Kill()
			}
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

	pid := appInstance.Pid // PID of the top-level process (nsenter/unshare/launcher)
	logger.Info("Stopping application instance process", "pid", pid)
	appInstance.Status = StatusStopping
	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(0)
	nr.publishStatusUpdate(instanceID, StatusStopping, &pid, nil, "")

	// Stop Aux Commands first
	for _, c := range appInstance.AuxCmds {
		if c.Process != nil {
			logger.Debug("Killing auxiliary command", "pid", c.Process.Pid)
			_ = c.Process.Kill()
		}
	}

	// Close stop signal channel if not already closed
	select {
	case <-appInstance.StopSignal:
	default:
		close(appInstance.StopSignal)
	}

	// Platform specific termination logic
	if err := nr.terminateProcessPlatform(appInstance.Cmd.Process, pid); err != nil {
		logger.Error("Failed to send termination signal", "pid", pid, "error", err)
		appInstance.processCancel() // Cancel context anyway
		return fmt.Errorf("failed to terminate %s (PID %d, RunID %s): %w", instanceID, pid, runID, err)
	}

	// Wait for graceful shutdown or timeout
	termTimer := time.NewTimer(StopTimeout)
	defer termTimer.Stop()
	select {
	case <-termTimer.C:
		logger.Warn("Graceful shutdown timed out. Sending Force Kill.", "pid", pid)
		// Try to force kill via platform specific means (cgroup if available, then kill)
		if appInstance.cgroupPath != "" && nr.usesCgroupsForResourceLimits(appInstance.Spec) {
			logger.Info("Attempting to kill via cgroup", "cgroup_path", appInstance.cgroupPath)
			if err := nr.killCgroupPlatform(appInstance.cgroupPath, logger); err != nil {
				logger.Error("Failed to kill cgroup, falling back to Process Kill", "error", err)
				_ = nr.forceKillProcessPlatform(appInstance.Cmd.Process, pid)
			}
		} else {
			_ = nr.forceKillProcessPlatform(appInstance.Cmd.Process, pid)
		}
		appInstance.processCancel() // Ensure context is cancelled if timeout occurs
	case <-appInstance.processCtx.Done():
		logger.Info("Process exited or context canceled during stop wait.")
	}
	return nil
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

	// Start periodic memory polling goroutine
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
					nr.pollMemoryPlatform(appInstance, logger)
				}
			case <-appInstance.processCtx.Done():
				logger.Debug("Stopping periodic memory metric poller due to process context done")
				return
			}
		}
	}()

	defer func() {
		appInstance.processCancel() // This will also stop the memory poller goroutine
		appInstance.LogWg.Wait()    // Wait for log forwarders AND memory poller
		logger.Debug("Monitor finished and auxiliary goroutines (logs, memory poller) completed")

		// Cgroup cleanup is crucial here
		if appInstance.cgroupCleanup != nil {
			logger.Info("Cleaning up cgroup for instance", "path", appInstance.cgroupPath)
			if err := appInstance.cgroupCleanup(); err != nil {
				logger.Error("Error during cgroup cleanup", "cgroup_path", appInstance.cgroupPath, "error", err)
			}
			appInstance.cgroupCleanup = nil // Prevent double cleanup
		}

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

	// Ensure any remaining AuxCmds are killed when main process exits
	for _, c := range appInstance.AuxCmds {
		if c.Process != nil {
			_ = c.Process.Kill()
		}
	}

	// Wait for logs to fully drain before processing exit status.
	// Since Cmd.Wait() has returned, the process is dead and its pipes are closed (EOF).
	// forwardLogs will exit, and LogWg will decrement.
	// We wait here to ensure all error logs (like Landlock failures printed to stderr)
	// are published to NATS before we publish the 'Crashed' status update.
	appInstance.LogWg.Wait()

	exitCode := -1
	intentionalStop := false
	errMsg := ""
	finalStatus := StatusStopped

	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(0)

	// Extract Rusage (Generic Wrapper)
	if appInstance.Cmd.ProcessState != nil {
		rssBytes, uTime, sTime := nr.extractRusagePlatform(appInstance.Cmd.ProcessState, logger)
		metrics.NarunNodeRunnerInstanceMemoryMaxRSSBytes.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(rssBytes)
		metrics.NarunNodeRunnerInstanceCPUUserSecondsTotal.WithLabelValues(appName, instanceID, nr.nodeID, runID).Add(uTime)
		metrics.NarunNodeRunnerInstanceCPUSystemSecondsTotal.WithLabelValues(appName, instanceID, nr.nodeID, runID).Add(sTime)
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
				// Platform specific check for signaling
				isSignaled, sig := nr.isProcessSignaledPlatform(exitErr)
				if isSignaled {
					logger.Warn("Process killed by signal", "pid", appInstance.Pid, "signal", sig)
					// Determine if signal was expected (SIGTERM/SIGINT during stop)
					// Note: sig is generic string here, checking equality with "terminated" or similar depends on OS.
					// For strict correctness, we assume intentionalStop handles most of this logic.
					if intentionalStop {
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
		logger.Info("Config changed since instance run started. No restart.", "old_hash", appInstance.ConfigHash, "new_hash", appInfo.configHash)
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
	targetObjectName := fmt.Sprintf("%s-%s-%s-%s", appName, tag, nr.localOS, nr.localArch)
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

// Helper to pipe exec.Cmd output to slog (add this to process.go or a utils file)
type logWriter struct {
	logger *slog.Logger
}

func (nr *NodeRunner) newLogWriter(logger *slog.Logger) *logWriter {
	return &logWriter{logger: logger}
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	lines := strings.Split(string(p), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			w.logger.Warn(line) // Log as Warn or Info
		}
	}
	return len(p), nil
}
