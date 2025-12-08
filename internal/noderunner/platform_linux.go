//go:build linux

package noderunner

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/akhenakh/narun/internal/metrics"
	"golang.org/x/sys/unix"
)

// Cgroup Helper Functions
const cgroupfsBase = "/sys/fs/cgroup"

func (nr *NodeRunner) getFullCgroupParentPath(specCgroupParent string) string {
	if filepath.IsAbs(specCgroupParent) { // Assumes it's /sys/fs/cgroup/...
		return filepath.Clean(specCgroupParent)
	}
	return filepath.Join(cgroupfsBase, filepath.Clean(specCgroupParent))
}

// createIsolationPlatform creates a Cgroup (Linux implementation of isolation).
// Returns cgroupPath as isolationID, cgroupFd as isolationFd.
func (nr *NodeRunner) createIsolationPlatform(spec *ServiceSpec, instanceID string, logger *slog.Logger) (isolationID string, isolationFd int, cleanup func() error, err error) {
	if spec.CgroupParent == "" {
		return "", -1, nil, fmt.Errorf("cgroupParent must be specified in ServiceSpec to use cgroups")
	}

	parentPath := nr.getFullCgroupParentPath(spec.CgroupParent)
	parentInfo, statErr := os.Stat(parentPath)
	if statErr != nil {
		return "", -1, nil, fmt.Errorf("cgroup parent path '%s' not accessible: %w", parentPath, statErr)
	}
	if !parentInfo.IsDir() {
		return "", -1, nil, fmt.Errorf("cgroup parent path '%s' is not a directory", parentPath)
	}

	instanceCgroupName := fmt.Sprintf("narun-%s.scope", instanceID)
	finalCgroupPath := filepath.Join(parentPath, instanceCgroupName)

	logger.Info("Creating cgroup", "path", finalCgroupPath)

	subtreeControlPath := filepath.Join(parentPath, "cgroup.subtree_control")
	controllersToEnable := "+cpu +memory"
	if writeErr := os.WriteFile(subtreeControlPath, []byte(controllersToEnable), 0644); writeErr != nil {
		logger.Warn("Potentially failed to enable controllers in parent cgroup. This might be okay if already enabled.",
			"path", subtreeControlPath, "error", writeErr)
	}

	if err = os.Mkdir(finalCgroupPath, 0755); err != nil {
		if os.IsExist(err) {
			logger.Warn("Cgroup directory already exists, attempting to reuse.", "path", finalCgroupPath)
			_ = unix.Rmdir(finalCgroupPath)
			if err = os.Mkdir(finalCgroupPath, 0755); err != nil {
				return "", -1, nil, fmt.Errorf("failed to create cgroup directory '%s' even after attempting cleanup: %w", finalCgroupPath, err)
			}
		} else {
			return "", -1, nil, fmt.Errorf("failed to create cgroup directory '%s': %w", finalCgroupPath, err)
		}
	}

	fd, openErr := unix.Open(finalCgroupPath, unix.O_PATH|unix.O_CLOEXEC, 0)
	if openErr != nil {
		_ = unix.Rmdir(finalCgroupPath)
		return "", -1, nil, fmt.Errorf("failed to open cgroup path '%s' for fd: %w", finalCgroupPath, openErr)
	}

	cleanupFunc := func() error {
		closeErr := unix.Close(fd)
		rmErr := unix.Rmdir(finalCgroupPath)
		if closeErr != nil && rmErr != nil {
			return fmt.Errorf("cgroup cleanup: failed to close fd (%v) AND rmdir (%v)", closeErr, rmErr)
		}
		if closeErr != nil {
			return fmt.Errorf("cgroup cleanup: failed to close fd: %w", closeErr)
		}
		if rmErr != nil {
			logger.Warn("Failed to rmdir cgroup during cleanup (might have tasks or sub-cgroups)", "path", finalCgroupPath, "error", rmErr)
			_ = nr.writeCgroupFile(finalCgroupPath, "cgroup.kill", "1")
			time.Sleep(100 * time.Millisecond)
			_ = unix.Rmdir(finalCgroupPath)
		}
		logger.Info("Cgroup cleaned up", "path", finalCgroupPath)
		return nil
	}

	// isolationID is the path, isolationFd is the FD
	return finalCgroupPath, fd, cleanupFunc, nil
}

func (nr *NodeRunner) writeCgroupFile(cgroupPath, file, content string) error {
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

// applyResourceLimitsPlatform applies Cgroup constraints (Linux implementation).
func (nr *NodeRunner) applyResourceLimitsPlatform(spec *ServiceSpec, isolationID string, logger *slog.Logger) error {
	cgroupPath := isolationID // On Linux, isolationID is the path
	logger.Info("Applying cgroup constraints", "path", cgroupPath)

	if spec.CPUCores > 0 {
		cpuPeriod := 100000
		cpuQuota := int(spec.CPUCores * float64(cpuPeriod))
		cpuMaxContent := fmt.Sprintf("%d %d", cpuQuota, cpuPeriod)
		if err := nr.writeCgroupFile(cgroupPath, "cpu.max", cpuMaxContent); err != nil {
			logger.Warn("Failed to set cpu.max, CPU limits may not apply", "error", err)
		} else {
			logger.Debug("Set cpu.max", "value", cpuMaxContent)
		}
	}

	if spec.MemoryMB > 0 {
		memBytes := spec.MemoryMB * 1024 * 1024
		memMaxBytes := memBytes
		if spec.MemoryMaxMB > 0 {
			memMaxBytes = spec.MemoryMaxMB * 1024 * 1024
		}

		if err := nr.writeCgroupFile(cgroupPath, "memory.low", strconv.FormatUint(memBytes, 10)); err != nil {
			logger.Warn("Failed to set memory.low, memory reclaim might not be prioritized", "error", err)
		} else {
			logger.Debug("Set memory.low", "bytes", memBytes)
		}

		if err := nr.writeCgroupFile(cgroupPath, "memory.max", strconv.FormatUint(memMaxBytes, 10)); err != nil {
			return fmt.Errorf("failed to set memory.max: %w. Ensure memory controller is enabled for parent cgroup.", err)
		} else {
			logger.Debug("Set memory.max", "bytes", memMaxBytes)
		}
	}
	return nil
}

// destroyIsolationPlatform kills the Cgroup (Linux implementation).
func (nr *NodeRunner) destroyIsolationPlatform(isolationID string, logger *slog.Logger) error {
	cgroupPath := isolationID
	if cgroupPath == "" {
		return nil
	}
	logger.Info("Attempting to kill all processes in cgroup", "path", cgroupPath)
	err := nr.writeCgroupFile(cgroupPath, "cgroup.kill", "1")
	if err != nil {
		logger.Error("Failed to write to cgroup.kill", "path", cgroupPath, "error", err)
		return err
	}
	return nil
}

// configureCmdPlatform prepares the exec.Cmd with platform specific attributes.
func (nr *NodeRunner) configureCmdPlatform(ctx context.Context, spec *ServiceSpec, workDir string, env []string, selfPath, binaryPath string, isolationFd int, isolationID string, logger *slog.Logger) (*exec.Cmd, error) {
	landlockLauncherCmd := selfPath
	landlockLauncherArgs := []string{internalLaunchFlag}

	cgroupFd := isolationFd // Map generic arg to platform specific var

	// Determine if we need nsenter/unshare wrappers
	// usesIsolation covers Cgroups on Linux
	useNamespacingOrCgroups := nr.usesNamespacing(spec) || nr.usesIsolation(spec)

	var finalExecCommandParts []string

	if useNamespacingOrCgroups {
		var commandBuilder []string
		if spec.NetworkNamespacePath != "" {
			commandBuilder = append(commandBuilder, "nsenter", "--no-fork", fmt.Sprintf("--net=%s", spec.NetworkNamespacePath), "--")
		}

		unshareCmd := "unshare"
		unshareArgsList := []string{
			"--ipc",
			"--net",
			"--pid",
			"--mount-proc",
			"--fork",
			"--kill-child=SIGKILL",
			"--",
		}
		commandBuilder = append(commandBuilder, unshareCmd)
		commandBuilder = append(commandBuilder, unshareArgsList...)
		commandBuilder = append(commandBuilder, landlockLauncherCmd)
		commandBuilder = append(commandBuilder, landlockLauncherArgs...)

		finalExecCommandParts = commandBuilder
	} else {
		if spec.Mode == "landlock" {
			finalExecCommandParts = append([]string{landlockLauncherCmd}, landlockLauncherArgs...)
		} else {
			finalExecCommandParts = append([]string{binaryPath}, spec.Args...)
		}
	}

	logger.Info("Final command to execute", "parts", finalExecCommandParts)
	cmd := exec.CommandContext(ctx, finalExecCommandParts[0], finalExecCommandParts[1:]...)
	cmd.Env = env
	cmd.Dir = workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Use generic usesIsolation to check if cgroups are in use
	if cgroupFd != -1 && nr.usesIsolation(spec) {
		cmd.SysProcAttr.UseCgroupFD = true
		cmd.SysProcAttr.CgroupFD = cgroupFd
		logger.Debug("Configured command to use CgroupFD", "fd", cgroupFd)
	}

	return cmd, nil
}

// pollMemoryPlatform polls memory usage for a running instance on Linux.
func (nr *NodeRunner) pollMemoryPlatform(instance *ManagedApp, logger *slog.Logger) {
	if instance.Cmd == nil || instance.Cmd.Process == nil || instance.Pid <= 0 {
		logger.Debug("Skipping memory poll, process or PID not valid", "pid", instance.Pid)
		return
	}

	pid := instance.Pid
	var VmHWMkB int64 = -1

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

func (nr *NodeRunner) extractRusagePlatform(state *os.ProcessState, logger *slog.Logger) (rssBytes float64, uTime float64, sTime float64) {
	if rusage, ok := state.SysUsage().(*syscall.Rusage); ok && rusage != nil {
		rssBytes = float64(rusage.Maxrss * 1024)
		uTime = metrics.TimevalToSeconds(rusage.Utime)
		sTime = metrics.TimevalToSeconds(rusage.Stime)

		logger.Debug("Rusage details on process exit",
			"Maxrss_raw", rusage.Maxrss, "converted_rss_bytes", rssBytes,
			"Utime_sec", rusage.Utime.Sec, "Utime_usec", rusage.Utime.Usec,
			"Stime_sec", rusage.Stime.Sec, "Stime_usec", rusage.Stime.Usec)
		return
	}
	logger.Warn("Could not get Rusage for process")
	return 0, 0, 0
}

func (nr *NodeRunner) terminateProcessPlatform(proc *os.Process, pid int) error {
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
		return err
	}
	return nil
}

func (nr *NodeRunner) forceKillProcessPlatform(proc *os.Process, pid int) error {
	return syscall.Kill(-pid, syscall.SIGKILL)
}

func (nr *NodeRunner) isProcessSignaledPlatform(exitErr *exec.ExitError) (bool, string) {
	if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return true, status.Signal().String()
	}
	return false, ""
}
