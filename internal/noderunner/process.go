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
	"os/user"
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
	"github.com/hashicorp/go-set/v2"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sys/unix"
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
	envLandlockInstanceRoot      = "NARUN_INTERNAL_INSTANCE_ROOT"
	envLandlockMountInfosJSON    = "NARUN_INTERNAL_MOUNT_INFOS_JSON"
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

// --- Cgroup Helper Functions ---
const cgroupfsBase = "/sys/fs/cgroup"

func (nr *NodeRunner) getFullCgroupParentPath(specCgroupParent string) string {
	if filepath.IsAbs(specCgroupParent) { // Assumes it's /sys/fs/cgroup/...
		return filepath.Clean(specCgroupParent)
	}
	return filepath.Join(cgroupfsBase, filepath.Clean(specCgroupParent))
}

func (nr *NodeRunner) createCgroup(spec *ServiceSpec, instanceID string, logger *slog.Logger) (cgroupPath string, cgroupFd int, cleanup func() error, err error) {
	if runtime.GOOS != "linux" {
		return "", -1, nil, fmt.Errorf("cgroups are only supported on Linux")
	}
	if spec.CgroupParent == "" {
		return "", -1, nil, fmt.Errorf("cgroupParent must be specified in ServiceSpec to use cgroups")
	}

	parentPath := nr.getFullCgroupParentPath(spec.CgroupParent)
	// Ensure parent cgroup exists and node-runner has perms to write cgroup.subtree_control
	// This check is basic; real permission issues will surface on mkdir/write.
	parentInfo, statErr := os.Stat(parentPath)
	if statErr != nil {
		return "", -1, nil, fmt.Errorf("cgroup parent path '%s' not accessible: %w", parentPath, statErr)
	}
	if !parentInfo.IsDir() {
		return "", -1, nil, fmt.Errorf("cgroup parent path '%s' is not a directory", parentPath)
	}

	// Instance cgroup name, e.g., narun-myapp-0.scope
	instanceCgroupName := fmt.Sprintf("narun-%s.scope", instanceID)
	finalCgroupPath := filepath.Join(parentPath, instanceCgroupName)

	logger.Info("Creating cgroup", "path", finalCgroupPath)

	// Enable necessary controllers in the parent's cgroup.subtree_control
	//    This allows the new cgroup to use these controllers.
	//    The node-runner process needs write permission to this file.
	subtreeControlPath := filepath.Join(parentPath, "cgroup.subtree_control")
	controllersToEnable := "+cpu +memory" // Add others like +io if needed
	if writeErr := os.WriteFile(subtreeControlPath, []byte(controllersToEnable), 0644); writeErr != nil {
		// If EACCES, it's a permission issue.
		// If EINVAL, controllers might already be enabled or not available.
		// This part is tricky; robust error handling or pre-flight checks are ideal.
		// For now, log warning and proceed, applyCgroupConstraints will fail if controllers not active
		logger.Warn("Potentially failed to enable controllers in parent cgroup. This might be okay if already enabled.",
			"path", subtreeControlPath, "error", writeErr)
	}

	// Create the cgroup directory
	if err = os.Mkdir(finalCgroupPath, 0755); err != nil {
		// Check if it already exists from a previous unclean shutdown
		if os.IsExist(err) {
			logger.Warn("Cgroup directory already exists, attempting to reuse.", "path", finalCgroupPath)
			// Try to remove it first, in case it's in a bad state
			// unix.Rmdir might fail if it has processes, but that's okay if we're about to put a new one in.
			_ = unix.Rmdir(finalCgroupPath)
			if err = os.Mkdir(finalCgroupPath, 0755); err != nil {
				return "", -1, nil, fmt.Errorf("failed to create cgroup directory '%s' even after attempting cleanup: %w", finalCgroupPath, err)
			}
		} else {
			return "", -1, nil, fmt.Errorf("failed to create cgroup directory '%s': %w", finalCgroupPath, err)
		}
	}

	// Open the cgroup directory to get a file descriptor
	fd, openErr := unix.Open(finalCgroupPath, unix.O_PATH|unix.O_CLOEXEC, 0)
	if openErr != nil {
		_ = unix.Rmdir(finalCgroupPath)
		return "", -1, nil, fmt.Errorf("failed to open cgroup path '%s' for fd: %w", finalCgroupPath, openErr)
	}

	cleanupFunc := func() error {
		closeErr := unix.Close(fd)
		rmErr := unix.Rmdir(finalCgroupPath) // This will fail if processes are still in it
		if closeErr != nil && rmErr != nil {
			return fmt.Errorf("cgroup cleanup: failed to close fd (%v) AND rmdir (%v)", closeErr, rmErr)
		}
		if closeErr != nil {
			return fmt.Errorf("cgroup cleanup: failed to close fd: %w", closeErr)
		}
		if rmErr != nil {
			// Log this, as it might indicate an issue, but don't always fail the cleanup.
			// Cgroup might be cleaned up by system eventually if it has no tasks.
			logger.Warn("Failed to rmdir cgroup during cleanup (might have tasks or sub-cgroups)", "path", finalCgroupPath, "error", rmErr)
			// Attempt to kill remaining processes in the cgroup
			_ = nr.writeCgroupFile(finalCgroupPath, "cgroup.kill", "1")
			// Retry rmdir after a short delay
			time.Sleep(100 * time.Millisecond)
			_ = unix.Rmdir(finalCgroupPath)
		}
		logger.Info("Cgroup cleaned up", "path", finalCgroupPath)
		return nil
	}

	return finalCgroupPath, fd, cleanupFunc, nil
}

func (nr *NodeRunner) writeCgroupFile(cgroupPath, file, content string) error {
	// Open with O_WRONLY | O_TRUNC. Some cgroup files require specific modes.
	fullPath := filepath.Join(cgroupPath, file)
	f, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open cgroup file '%s': %w", fullPath, err)
	}
	defer f.Close()
	_, err = f.WriteString(content)
	if err != nil {
		return fmt.Errorf("failed to write to cgroup file '%s': %w", fullPath, err)
	}
	return nil
}

func (nr *NodeRunner) applyCgroupConstraints(spec *ServiceSpec, cgroupPath string, logger *slog.Logger) error {
	logger.Info("Applying cgroup constraints", "path", cgroupPath)
	// CPU Limit (cpu.max: quota period)
	// Period is typically 100000 (100ms). Quota is how much of that period the cgroup can use.
	// If CPUCores = 1.5, quota = 1.5 * 100000 = 150000.
	if spec.CPUCores > 0 {
		cpuPeriod := 100000 // Default CFS period in microseconds
		cpuQuota := int(spec.CPUCores * float64(cpuPeriod))
		cpuMaxContent := fmt.Sprintf("%d %d", cpuQuota, cpuPeriod)
		if err := nr.writeCgroupFile(cgroupPath, "cpu.max", cpuMaxContent); err != nil {
			logger.Warn("Failed to set cpu.max, CPU limits may not apply", "error", err)
			// Don't fail entirely, but log. CPU controller might not be available/enabled.
		} else {
			logger.Debug("Set cpu.max", "value", cpuMaxContent)
		}
	}

	// Memory Limits (in bytes)
	if spec.MemoryMB > 0 {
		memBytes := spec.MemoryMB * 1024 * 1024
		memMaxBytes := memBytes
		if spec.MemoryMaxMB > 0 {
			memMaxBytes = spec.MemoryMaxMB * 1024 * 1024
		}

		// memory.low (soft limit)
		if err := nr.writeCgroupFile(cgroupPath, "memory.low", strconv.FormatUint(memBytes, 10)); err != nil {
			logger.Warn("Failed to set memory.low, memory reclaim might not be prioritized", "error", err)
		} else {
			logger.Debug("Set memory.low", "bytes", memBytes)
		}

		// memory.max (hard limit)
		if err := nr.writeCgroupFile(cgroupPath, "memory.max", strconv.FormatUint(memMaxBytes, 10)); err != nil {
			// This is more critical. If it fails, hard memory limit won't apply.
			return fmt.Errorf("failed to set memory.max: %w. Ensure memory controller is enabled for parent cgroup.", err)
		} else {
			logger.Debug("Set memory.max", "bytes", memMaxBytes)
		}
		// Could also set memory.swap.max = 0 to disable swap for the cgroup
		// if err := nr.writeCgroupFile(cgroupPath, "memory.swap.max", "0"); err != nil {
		//    logger.Warn("Failed to disable swap (set memory.swap.max=0)", "error", err)
		// }
	}
	return nil
}

func (nr *NodeRunner) killCgroup(cgroupPath string, logger *slog.Logger) error {
	if cgroupPath == "" {
		return nil
	}
	logger.Info("Attempting to kill all processes in cgroup", "path", cgroupPath)
	// Writing "1" to cgroup.kill sends SIGKILL to all processes in the cgroup and its descendants.
	// This requires the cgroup.kill file to be present (unified hierarchy / cgroup v2).
	err := nr.writeCgroupFile(cgroupPath, "cgroup.kill", "1")
	if err != nil {
		logger.Error("Failed to write to cgroup.kill", "path", cgroupPath, "error", err)
		return err
	}
	return nil
}

// startAppInstance attempts to download the binary, configure, and start a single instance.
// It assumes the calling code (handleAppConfigUpdate or triggerCronJobExecution) holds locks on appInfo if needed for spec access.
// The instanceID must be unique for this run (e.g., appName-0 for replicas, appName-cron-timestamp for cron jobs).
func (nr *NodeRunner) startAppInstance(ctx context.Context, appInfoToUse *appInfo, instanceID string, isCronJobRun bool, jobTimeout time.Duration) (returnedErr error) {
	// Read spec under appInfo's lock to ensure consistency if spec is being updated concurrently
	appInfoToUse.mu.RLock()
	spec := appInfoToUse.spec
	if spec == nil {
		appInfoToUse.mu.RUnlock()
		return fmt.Errorf("cannot start instance %s, app spec is nil for config hash %s", instanceID, appInfoToUse.configHash)
	}
	// Make a copy of the spec to use after unlocking
	currentSpec := *spec
	configHashForThisRun := appInfoToUse.configHash
	appInfoToUse.mu.RUnlock()

	appName := currentSpec.Name
	tag := currentSpec.Tag
	currentRunID := generateRunID()
	logger := nr.logger.With("app", appName, "instance_id", instanceID, "run_id", currentRunID, "tag", tag, "mode", currentSpec.Mode, "is_cron_job", isCronJobRun)
	logger.Info("Attempting to start application instance")

	// Fetch Binary First
	binaryPath, fetchErr := nr.fetchAndStoreBinary(ctx, currentSpec.Tag, appName, configHashForThisRun)
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

	mountInfosForEnv := make([]MountInfoForEnv, 0, len(currentSpec.Mounts))
	for _, mount := range currentSpec.Mounts {
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
	logger.Info("File mounts processed successfully", "count", len(currentSpec.Mounts))

	var targetUID, targetGID uint32
	var targetHomeDir string
	currentUser, _ := user.Current()

	if currentSpec.User != "" {
		var err error
		targetUID, targetGID, targetHomeDir, err = lookupUser(currentSpec.User)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to lookup user '%s': %v", currentSpec.User, err)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0)
			return fmt.Errorf(errMsg)
		}
		logger.Info("Target user resolved", "user", currentSpec.User, "uid", targetUID, "gid", targetGID)
	} else {
		uid64, _ := strconv.ParseUint(currentUser.Uid, 10, 32)
		gid64, _ := strconv.ParseUint(currentUser.Gid, 10, 32)
		targetUID = uint32(uid64)
		targetGID = uint32(gid64)
		targetHomeDir = currentUser.HomeDir
		logger.Debug("Using node-runner's user for process", "uid", targetUID, "gid", targetGID)
	}

	// Prepare Environment Variables (including resolved secrets) for the *target application*
	resolvedAppEnv := make([]EnvVar, len(currentSpec.Env))
	for i, envVar := range currentSpec.Env {
		resolvedAppEnv[i] = envVar
		if envVar.ValueFromSecret != "" {
			val, err := nr.resolveSecret(ctx, envVar.ValueFromSecret, logger)
			if err != nil {
				logger.Error("Failed to resolve secret for env var", "env_var", envVar.Name, "error", err)
				errMsg := fmt.Sprintf("Failed to resolve secret '%s': %v", envVar.ValueFromSecret, err)
				nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
				metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0)
				return fmt.Errorf("failed to resolve secret '%s': %w", envVar.ValueFromSecret, err)
			}
			resolvedAppEnv[i].Value = val
		} else {
			resolvedAppEnv[i].Value = envVar.Value
		}
	}
	baseOsEnv := os.Environ()
	launcherTargetEnv := flattenEnv(baseOsEnv, currentSpec.User, targetHomeDir, resolvedAppEnv)
	launcherTargetEnv = append(launcherTargetEnv,
		fmt.Sprintf("NARUN_APP_NAME=%s", appName),
		fmt.Sprintf("NARUN_INSTANCE_ID=%s", instanceID),
		fmt.Sprintf("NARUN_RUN_ID=%s", currentRunID),
		// NARUN_REPLICA_INDEX only makes sense for non-cron jobs
		// For cron jobs, it's not a replica in the same sense.
		// We can omit it or set to -1 for cron jobs.
		fmt.Sprintf("NARUN_REPLICA_INDEX=%d", extractReplicaIndex(instanceID)), // Will be -1 for cron jobs
		fmt.Sprintf("NARUN_NODE_ID=%s", nr.nodeID),
		fmt.Sprintf("NARUN_INSTANCE_ROOT=%s", workDir),
		fmt.Sprintf("NARUN_NODE_OS=%s", nr.localOS),
		fmt.Sprintf("NARUN_NODE_ARCH=%s", nr.localArch),
		fmt.Sprintf("NARUN_IS_CRON_JOB=%t", isCronJobRun),
	)

	selfPath, err := os.Executable()
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get node-runner executable path: %v", err)
		logger.Error(errMsg)
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0)
		return fmt.Errorf("failed to get node-runner exec path: %w", err)
	}
	landlockConfigJSON, _ := json.Marshal(currentSpec.Landlock)
	targetArgsJSON, _ := json.Marshal(currentSpec.Args)
	mountInfosJSON, _ := json.Marshal(mountInfosForEnv)

	// Environment for the landlock launcher process itself
	// It needs these to configure landlock and then exec the target.
	// The launcher itself will run as the node-runner user initially, then landlock execs target.
	// If unshare is used, unshare drops privs *before* landlock launcher.
	envForLauncher := slices.Clone(launcherTargetEnv) // Start with target env, then add launcher specifics
	envForLauncher = append(envForLauncher,
		fmt.Sprintf("%s=%s", envLandlockConfigJSON, string(landlockConfigJSON)),
		fmt.Sprintf("%s=%s", envLandlockTargetCmd, binaryPath),
		fmt.Sprintf("%s=%s", envLandlockTargetArgsJSON, string(targetArgsJSON)),
		fmt.Sprintf("%s=%s", envLandlockMountInfosJSON, string(mountInfosJSON)),
		fmt.Sprintf("%s=%s", envLandlockInstanceRoot, workDir),
	)

	var cgroupPath string
	var cgroupFd int = -1
	var cgroupCleanupFunc func() error

	if nr.usesCgroupsForResourceLimits(&currentSpec) {
		var cgErr error
		cgroupPath, cgroupFd, cgroupCleanupFunc, cgErr = nr.createCgroup(&currentSpec, instanceID, logger)
		if cgErr != nil {
			errMsg := fmt.Sprintf("Failed to create cgroup: %v", cgErr)
			logger.Error(errMsg)
			nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, errMsg)
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0)
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

		if err := nr.applyCgroupConstraints(&currentSpec, cgroupPath, logger); err != nil {
			errMsg := fmt.Sprintf("Failed to apply cgroup constraints: %v", err)
			// cgroupCleanupFunc will be called by the defer above
			return fmt.Errorf(errMsg)
		}
	}

	// Process context: Use global context as parent. If it's a cron job with a timeout, create a derived context.
	var processCtx context.Context
	var processCancel context.CancelFunc

	if isCronJobRun && jobTimeout > 0 {
		processCtx, processCancel = context.WithTimeout(nr.globalCtx, jobTimeout)
		logger.Info("Applying job timeout for cron run", "timeout", jobTimeout)
	} else {
		processCtx, processCancel = context.WithCancel(nr.globalCtx)
	}

	var cmd *exec.Cmd
	var finalExecCommandParts []string

	landlockLauncherCmd := selfPath
	landlockLauncherArgs := []string{internalLaunchFlag}

	useNamespacingOrCgroups := nr.usesNamespacing(&currentSpec) || nr.usesCgroupsForResourceLimits(&currentSpec)

	if useNamespacingOrCgroups && runtime.GOOS == "linux" {
		var commandBuilder []string
		if currentSpec.NetworkNamespacePath != "" {
			commandBuilder = append(commandBuilder, "nsenter", "--no-fork", fmt.Sprintf("--net=%s", currentSpec.NetworkNamespacePath), "--")
		}
		unshareCmd := "unshare"
		unshareArgsList := []string{
			"--ipc", "--pid", "--mount-proc", "--fork", "--kill-child=SIGKILL",
			// User/Group. If spec.User is empty, targetUID/GID is node-runner's user.
			"--setuid", strconv.Itoa(int(targetUID)),
			"--setgid", strconv.Itoa(int(targetGID)),
			// TODO: Add --map-root-user if needed for some scenarios (e.g. user namespaces for rootless containers)
			// This requires more complex UID/GID mapping setup. For now, assume target user exists on host.
			"--", // Separator for the command unshare will run
		}
		commandBuilder = append(commandBuilder, unshareCmd)
		commandBuilder = append(commandBuilder, unshareArgsList...)
		commandBuilder = append(commandBuilder, landlockLauncherCmd)
		commandBuilder = append(commandBuilder, landlockLauncherArgs...)
		finalExecCommandParts = commandBuilder
	} else {
		// Standard landlock launcher path (or direct exec if mode != "landlock")
		// If on non-Linux, cgroups/namespacing are skipped.
		if currentSpec.Mode == "landlock" {
			finalExecCommandParts = append([]string{landlockLauncherCmd}, landlockLauncherArgs...)
		} else {
			finalExecCommandParts = append([]string{binaryPath}, currentSpec.Args...)
			envForLauncher = launcherTargetEnv
		}
	}

	logger.Info("Final command to execute", "parts", finalExecCommandParts)
	cmd = exec.CommandContext(processCtx, finalExecCommandParts[0], finalExecCommandParts[1:]...)
	// This env is for the landlock launcher, or direct exec
	cmd.Env = envForLauncher
	cmd.Dir = workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if cgroupFd != -1 && nr.usesCgroupsForResourceLimits(&currentSpec) {
		cmd.SysProcAttr.UseCgroupFD = true
		cmd.SysProcAttr.CgroupFD = cgroupFd
		logger.Debug("Configured command to use CgroupFD", "fd", cgroupFd)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	appInstance := &ManagedApp{
		InstanceID:    instanceID,
		RunID:         currentRunID,
		Spec:          &currentSpec, // Store pointer to the copied spec for this run
		Cmd:           cmd,          // Store the top-level command (nsenter/unshare/launcher)
		Status:        StatusStarting,
		ConfigHash:    configHashForThisRun,
		BinaryPath:    binaryPath, // Path to the actual application binary
		StdoutPipe:    stdoutPipe,
		StderrPipe:    stderrPipe,
		StopSignal:    make(chan struct{}),
		processCtx:    processCtx,
		processCancel: processCancel,
		restartCount:  0, // Initial restart count
		cgroupPath:    cgroupPath,
		cgroupFd:      cgroupFd,
		cgroupCleanup: cgroupCleanupFunc,
		IsCronJobRun:  isCronJobRun,
	}

	// Add instance to appInfo.instances under lock
	appInfoToUse.mu.Lock()
	foundAndReplaced := false
	// For persistent services, we might replace an old entry. For cron jobs, they are always new.
	if !isCronJobRun {
		for i, existing := range appInfoToUse.instances {
			if existing.InstanceID == instanceID { // This implies it's a persistent replica being replaced
				if existing.processCancel != nil {
					existing.processCancel()
				}
				// Restart count for the logical replica ID is inherited
				appInstance.restartCount = existing.restartCount
				appInfoToUse.instances[i] = appInstance
				foundAndReplaced = true
				logger.Debug("Replacing existing state for persistent instance in appInfo", "index", i, "old_run_id", existing.RunID, "new_run_id", appInstance.RunID)
				break
			}
		}
	}
	if !foundAndReplaced { // Either a new persistent replica or a cron job
		appInfoToUse.instances = append(appInfoToUse.instances, appInstance)
		logger.Debug("Appending new instance state to appInfo")
	}
	appInfoToUse.mu.Unlock()

	nr.publishStatusUpdate(instanceID, StatusStarting, nil, nil, "Starting process")
	logger.Info("Starting process execution", "command", cmd.Path, "args", cmd.Args)

	if startErr := cmd.Start(); startErr != nil {
		returnedErr = fmt.Errorf("failed to start %s: %w", instanceID, startErr)
		logger.Error("Failed to start process", "error", returnedErr)
		appInstance.Status = StatusFailed
		appInstance.processCancel()
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, returnedErr.Error())
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0)
		nr.removeInstanceState(appName, instanceID, logger) // remove from appInfo.instances
		nr.cleanupInstanceMetrics(appInstance)
		return returnedErr
	}

	// If cgroupFd was used by cmd.Start(), it's consumed. We can close our copy if we still have it.
	// However, the cgroupFd in ManagedApp (appInstance.cgroupFd) is the one unix.Open'd.
	// The cgroupCleanupFunc is responsible for closing this original fd.
	// The kernel duplicates the fd for the child process.
	// Let's ensure the original descriptor passed to createCgroup is closed if distinct from what SysProcAttr needs,
	// but unix.Open with O_CLOEXEC should handle this for non-child processes.
	// For UseCgroupFD, the child inherits it. The pledge example closes fd *after* cmd.Start().
	// This means our cgroupCleanup (which includes unix.Close(fd)) should be called when the instance *stops*, not immediately after start.
	// The `defer` handling `returnedErr` already covers failure *during* start.
	appInstance.Pid = cmd.Process.Pid // Get the PID of the started process
	appInstance.Pid = cmd.Process.Pid
	appInstance.StartTime = time.Now()
	appInstance.Status = StatusRunning
	logger.Info("Process started successfully", "pid", appInstance.Pid)
	nr.publishStatusUpdate(instanceID, StatusRunning, &appInstance.Pid, nil, "")

	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(1)
	metrics.NarunNodeRunnerInstanceInfo.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID, currentSpec.Tag, currentSpec.Mode, binaryPath).Set(1)
	metrics.NarunNodeRunnerInstanceCPUUserSecondsTotal.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Add(0)
	metrics.NarunNodeRunnerInstanceCPUSystemSecondsTotal.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Add(0)
	metrics.NarunNodeRunnerInstanceMemoryMaxRSSBytes.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0)
	metrics.NarunNodeRunnerInstanceLastExitCode.WithLabelValues(appName, instanceID, nr.nodeID, currentRunID).Set(0)

	// Start log forwarding and monitoring goroutines
	appInstance.LogWg.Add(2)
	go nr.forwardLogs(appInstance, appInstance.StdoutPipe, "stdout", logger)
	go nr.forwardLogs(appInstance, appInstance.StderrPipe, "stderr", logger)
	go nr.monitorAppInstance(appInstance, logger)

	return nil
}

// stopAppInstance gracefully stops an application instance process.
func (nr *NodeRunner) stopAppInstance(appInstance *ManagedApp, intentional bool) error {
	instanceID := appInstance.InstanceID
	runID := appInstance.RunID
	appName := InstanceIDToAppName(instanceID)
	logger := nr.logger.With("app", appName, "instance_id", instanceID, "run_id", runID)

	if appInstance.Status == StatusStopped || appInstance.Status == StatusStopping || appInstance.Status == StatusFailed {
		logger.Info("Stop request: Instance not running or already stopping/stopped/failed.", "status", appInstance.Status)
		if appInstance.Status != StatusStopping {
			metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(0)
		}
		return nil
	}
	if appInstance.Cmd == nil || appInstance.Cmd.Process == nil {
		logger.Warn("Stop request: Instance state inconsistent (no command/process). Cleaning up state.")
		appInstance.Status = StatusFailed
		if appInstance.processCancel != nil {
			appInstance.processCancel()
		}
		nr.publishStatusUpdate(instanceID, StatusFailed, nil, nil, "Inconsistent state during stop")
		metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(0)
		return fmt.Errorf("inconsistent state for instance %s (run %s)", instanceID, runID)
	}

	pid := appInstance.Pid
	logger.Info("Stopping application instance process", "pid", pid)
	appInstance.Status = StatusStopping
	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nr.nodeID, runID).Set(0)
	nr.publishStatusUpdate(instanceID, StatusStopping, &pid, nil, "")

	select {
	case <-appInstance.StopSignal:
	default:
		close(appInstance.StopSignal)
	}

	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			logger.Warn("Stop request: Process already exited before SIGTERM", "pid", pid)
			appInstance.processCancel()
			return nil
		}
		logger.Error("Failed to send SIGTERM", "pid", pid, "error", err)
		appInstance.processCancel()
		return fmt.Errorf("failed to send SIGTERM to %s (PID %d, RunID %s): %w", instanceID, pid, runID, err)
	}

	termTimer := time.NewTimer(StopTimeout)
	defer termTimer.Stop()
	select {
	case <-termTimer.C:
		logger.Warn("Graceful shutdown timed out. Sending SIGKILL.", "pid", pid)
		if appInstance.cgroupPath != "" && nr.usesCgroupsForResourceLimits(appInstance.Spec) {
			logger.Info("Attempting to kill via cgroup.kill", "cgroup_path", appInstance.cgroupPath)
			if err := nr.killCgroup(appInstance.cgroupPath, logger); err != nil {
				logger.Error("Failed to kill cgroup, falling back to SIGKILL PID", "error", err)
				if killErr := syscall.Kill(-pid, syscall.SIGKILL); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
					logger.Error("Failed to send SIGKILL to pgid", "pid", pid, "error", killErr)
				}
			}
		} else {
			if killErr := syscall.Kill(-pid, syscall.SIGKILL); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
				logger.Error("Failed to send SIGKILL to pgid", "pid", pid, "error", killErr)
			}
		}
		appInstance.processCancel()
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

	getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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

	logger = logger.With("stream", streamName, "run_id", appInstance.RunID)
	logger.Debug("Starting log forwarding")
	appName := InstanceIDToAppName(appInstance.InstanceID)
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
				// Check if process is still considered running before polling
				case <-pollTicker.C:
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

		if appInstance.cgroupCleanup != nil {
			logger.Info("Cleaning up cgroup for instance", "path", appInstance.cgroupPath)
			if err := appInstance.cgroupCleanup(); err != nil {
				logger.Error("Error during cgroup cleanup", "cgroup_path", appInstance.cgroupPath, "error", err)
			}
			appInstance.cgroupCleanup = nil // Prevent double cleanup
		}

		// For cron jobs, always remove their state and disk after they complete (success or fail).
		// For persistent services, only remove disk if permanently failed or runner shutting down.
		shouldCleanupDisk := appInstance.IsCronJobRun // Cron jobs are always cleaned up

		if !appInstance.IsCronJobRun { // Logic for persistent services
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
				if appInfo.spec == nil { // If the app config was deleted
					shouldCleanupDisk = true
					logger.Info("App config deleted, scheduling disk cleanup.")
				}
				appInfo.mu.RUnlock()
			}
		} else {
			logger.Info("Cron job run completed, scheduling disk cleanup.", "cron_instance_id", instanceID)
		}

		if shouldCleanupDisk {
			instanceDir := filepath.Dir(appInstance.Cmd.Dir) // workDir is Cmd.Dir
			logger.Info("Performing permanent cleanup of instance data directory", "dir", instanceDir)
			if err := os.RemoveAll(instanceDir); err != nil {
				logger.Error("Failed to remove instance data directory", "dir", instanceDir, "error", err)
			}
			// Always remove instance state from manager, regardless of disk cleanup success
			nr.removeInstanceState(appName, instanceID, logger)
			// With RunID, we don't delete metrics. cleanupInstanceMetrics ensures _up=0 for this run.
			nr.cleanupInstanceMetrics(appInstance)
		} else {
			// For persistent services that stopped cleanly and aren't being pruned,
			// still remove their runtime state from the manager if they are truly stopped (not restarting).
			// This is now handled better by the restart logic directly.
			// The instance state in appInfo.instances is managed by start/stop/restart logic.
			// If shouldCleanupDisk is false, it means it's a persistent service that stopped (e.g., intentionally)
			// and is not being pruned. It might be restarted. If it's not restarted, its state
			// remains in appInfo.instances until a config change or explicit deletion.
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
		logger.Info("Process exit detected after context cancellation (possibly job timeout or runner shutdown)")
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

	// Skip restart logic for cron job runs or if intentionally stopped/failed or runner shutting down
	if appInstance.IsCronJobRun || intentionalStop || finalStatus == StatusFailed || nr.globalCtx.Err() != nil {
		logger.Info("Instance run stopped/failed. No restart for this run.",
			"status", finalStatus, "intentional", intentionalStop, "is_cron", appInstance.IsCronJobRun, "runner_shutdown", nr.globalCtx.Err() != nil)
		// Defer will handle cleanup of disk and state for cron jobs or permanently failed/pruned instances.
		return
	}

	// --- Restart logic for persistent services only ---
	appInfo := nr.state.GetAppInfo(appName)
	appInfo.mu.RLock()
	configHashMatches := appInfo.configHash == appInstance.ConfigHash
	instanceStillManaged := false
	// Check if this specific *persistent replica ID* is still supposed to be managed.
	// If appInfo.instances was cleared due to scale-down or config delete, this instance might not be there.
	for _, inst := range appInfo.instances {
		if inst.InstanceID == instanceID { // This refers to the specific replica ID, e.g., "appName-0"
			instanceStillManaged = true
			break
		}
	}
	appInfo.mu.RUnlock()

	if !instanceStillManaged {
		logger.Warn("Persistent instance disappeared from state before restart check (likely scaled down or app deleted). Aborting restart for this specific run.", "instance_id", instanceID)
		// Defer will handle cleanup if needed for this run.
		return
	}
	if !configHashMatches {
		logger.Info("Configuration changed since persistent instance run started. No restart for this run.", "old_hash", appInstance.ConfigHash, "new_hash", appInfo.configHash)
		// A new reconciliation loop in handleAppConfigUpdate will manage starting instances with new config.
		return
	}

	numericalReplicaIndex := extractReplicaIndex(instanceID)
	if numericalReplicaIndex == -1 { // Should not happen for persistent replicas
		logger.Error("Could not determine numerical replica index for restarting persistent instance. Aborting restart.", "instance_id", instanceID)
		return
	}

	// Restart count is for the logical replica (e.g., appName-0), not the runID.
	// When a persistent instance (like appName-0) crashes and needs restart, we look up its
	// current ManagedApp state (which should be appInstance here) to get its restartCount.
	if appInstance.restartCount >= MaxRestarts {
		logger.Error("Persistent instance crashed too many times. Giving up.", "max_restarts", MaxRestarts)
		appInstance.Status = StatusFailed // Mark the logical instance as failed
		nr.publishStatusUpdate(instanceID, StatusFailed, &appInstance.Pid, &exitCode, "Exceeded max restarts")
		// The defer will clean up this specific failed *run*. The logical instance appName-0 remains failed.
		return
	}

	appInstance.restartCount++ // Increment for the logical replica ID
	metrics.NarunNodeRunnerInstanceRestartsTotal.WithLabelValues(appName, instanceID, nr.nodeID).Inc()
	logger.Warn("Persistent instance run crashed/stopped unexpectedly. Attempting restart of logical instance.", "restart_count", appInstance.restartCount, "max_restarts", MaxRestarts, "delay", RestartDelay)
	appInstance.Status = StatusStarting // Status for the *next run* of this logical replica
	nr.publishStatusUpdate(instanceID, StatusStarting, nil, nil, fmt.Sprintf("Restarting (%d/%d)", appInstance.restartCount, MaxRestarts))

	restartTimer := time.NewTimer(RestartDelay)
	select {
	case <-restartTimer.C:
	case <-nr.globalCtx.Done():
		logger.Info("Restart canceled due to runner shutdown.")
		restartTimer.Stop()
		return
	case <-appInstance.processCtx.Done(): // Should be current run's context that just finished
		logger.Warn("Instance run context cancelled during restart delay? This is unexpected for restart logic.")
		restartTimer.Stop()
		return
		// StopSignal is for the run that just completed, not relevant for the next run's decision.
	}

	// Re-check config and managed state before actual restart attempt.
	// This uses appInfo (the manager's view of the app), not appInstance (the completed run's view).
	appInfo.mu.Lock() // Lock for starting the new instance
	defer appInfo.mu.Unlock()

	currentConfigHashAfterDelay := appInfo.configHash
	instanceStillExistsAfterDelay := false
	for _, inst := range appInfo.instances { // Check appInfo.instances for the persistent ID
		if inst.InstanceID == instanceID {
			instanceStillExistsAfterDelay = true
			break
		}
	}

	if !instanceStillExistsAfterDelay {
		logger.Warn("Persistent instance was removed from managed state during restart delay. Aborting restart.")
	} else if currentConfigHashAfterDelay != appInstance.ConfigHash { // Compare with the hash of the run that crashed
		logger.Info("Config changed during restart delay. Aborting restart. New config will be reconciled.")
	} else {
		// Attempt the restart. This will create a new ManagedApp with a new RunID for the same persistent instanceID.
		// The instanceID (e.g. "app-0") is passed, and startAppInstance will handle restartCount.
		err := nr.startAppInstance(context.Background(), appInfo, instanceID, false, 0)
		if err != nil {
			logger.Error("Failed to restart application instance", "instance_id", instanceID, "error", err)
		} else {
			logger.Info("Persistent instance restarted successfully (new run started)", "instance_id", instanceID)
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
	expectedBase64Hash = strings.TrimRight(expectedBase64Hash, "=")
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
	var VmHWMkB int64 = -1

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
		return
	}

	if VmHWMkB != -1 {
		memoryBytes := float64(VmHWMkB * 1024)
		metrics.NarunNodeRunnerInstanceMemoryMaxRSSBytes.WithLabelValues(
			InstanceIDToAppName(instance.InstanceID),
			instance.InstanceID,
			nr.nodeID,
			instance.RunID,
		).Set(memoryBytes)
		logger.Debug("Updated live memory (VmHWM) metric", "pid", pid, "VmHWM_kB", VmHWMkB, "bytes", memoryBytes)
	}
}
