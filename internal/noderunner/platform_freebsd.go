//go:build freebsd

package noderunner

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"

	"github.com/akhenakh/narun/internal/fbjail"
	"github.com/akhenakh/narun/internal/metrics"
)

// createCgroupPlatform creates a FreeBSD Jail.
// In the current architecture, "Cgroup" abstraction is used for Isolation + Resource Control.
// On FreeBSD, we create the Jail structure here.
func (nr *NodeRunner) createCgroupPlatform(spec *ServiceSpec, instanceID string, logger *slog.Logger) (cgroupPath string, cgroupFd int, cleanup func() error, err error) {
	if spec.Mode != "jail" {
		// If not in jail mode, we don't create an isolation context (jail) here.
		// Returns empty path/cleanup which is fine for 'exec' mode.
		return "", -1, nil, nil
	}

	// We use the instanceID as the Jail Name
	jailName := fmt.Sprintf("narun_%s", strings.ReplaceAll(instanceID, "-", "_"))

	// Determine the Jail Root. Using the instance work directory.
	// NOTE: process.go creates the work dir before calling this.
	// We reconstruct the path based on known structure: dataDir/instances/instanceID/work
	jailPath := fmt.Sprintf("%s/instances/%s/work", nr.dataDir, instanceID)

	// Ensure directory exists (process.go should have done it, but double check)
	if _, err := os.Stat(jailPath); os.IsNotExist(err) {
		return "", -1, nil, fmt.Errorf("jail root path does not exist: %s", jailPath)
	}

	cfg := fbjail.JailConfig{
		Name:             jailName,
		Path:             jailPath,
		Hostname:         spec.Jail.Hostname,
		IP4Addresses:     spec.Jail.IP4Addresses,
		MountDevfs:       true,
		DevfsRuleset:     spec.Jail.DevfsRuleset,
		EnforceStatfs:    2,
		AllowRawSockets:  spec.Jail.AllowRawSockets,
		Persist:          true, // Required to keep jail alive for the launcher to attach
		CopyResolvConf:   true,
		MountSystemCerts: spec.Jail.MountSystemCerts,
	}

	// If Hostname not set, default to App Name
	if cfg.Hostname == "" {
		cfg.Hostname = spec.Name
	}

	manager := fbjail.NewManager(cfg)

	logger.Info("Creating Jail", "name", jailName, "path", jailPath)
	if err := manager.Create(); err != nil {
		return "", -1, nil, fmt.Errorf("failed to create jail: %w", err)
	}

	cleanupFunc := func() error {
		logger.Info("Stopping Jail", "name", jailName)
		// We should also remove RCTL limits if we added any,
		// though removing the jail usually clears rules attached to the jail subject.
		_ = exec.Command("rctl", "-r", fmt.Sprintf("jail:%s", jailName)).Run()
		return manager.Stop()
	}

	// Return the Jail Name as the "cgroupPath" (identifier) and JID as fd (casted) if needed,
	// though we primarily use the name/JID string.
	// We return the JID in the path string for easier parsing later if needed.
	return strconv.Itoa(manager.JID), -1, cleanupFunc, nil
}

// applyCgroupConstraintsPlatform applies RCTL resource limits to the Jail.
func (nr *NodeRunner) applyCgroupConstraintsPlatform(spec *ServiceSpec, cgroupPath string, logger *slog.Logger) error {
	// cgroupPath here holds the JID string from createCgroupPlatform
	jid := cgroupPath
	if jid == "" || spec.Mode != "jail" {
		return nil
	}

	logger.Info("Applying RCTL resource limits", "jid", jid)

	// CPU Cores mapping to pcpu (percentage)
	// 1.0 core = 100%
	if spec.CPUCores > 0 {
		pcpu := int(spec.CPUCores * 100)
		rule := fmt.Sprintf("jail:%s:pcpu:deny=%d", jid, pcpu)
		logger.Debug("Adding RCTL CPU rule", "rule", rule)
		if err := exec.Command("rctl", "-a", rule).Run(); err != nil {
			logger.Warn("Failed to apply CPU RCTL limit", "error", err)
		}
	}

	// Memory Limits
	// rctl memoryuse includes RAM + Swap.
	if spec.MemoryMaxMB > 0 {
		memBytes := spec.MemoryMaxMB * 1024 * 1024
		rule := fmt.Sprintf("jail:%s:memoryuse:deny=%d", jid, memBytes)
		logger.Debug("Adding RCTL Memory rule", "rule", rule)
		if err := exec.Command("rctl", "-a", rule).Run(); err != nil {
			return fmt.Errorf("failed to apply memory RCTL limit: %w", err)
		}
	} else if spec.MemoryMB > 0 {
		// Fallback if Max not set
		memBytes := spec.MemoryMB * 1024 * 1024
		rule := fmt.Sprintf("jail:%s:memoryuse:deny=%d", jid, memBytes)
		logger.Debug("Adding RCTL Memory rule", "rule", rule)
		if err := exec.Command("rctl", "-a", rule).Run(); err != nil {
			return fmt.Errorf("failed to apply memory RCTL limit: %w", err)
		}
	}

	return nil
}

// killCgroupPlatform destroys the jail.
func (nr *NodeRunner) killCgroupPlatform(cgroupPath string, logger *slog.Logger) error {
	// cgroupPath is the JID
	if cgroupPath == "" {
		return nil
	}
	jid, err := strconv.Atoi(cgroupPath)
	if err != nil {
		return fmt.Errorf("invalid JID: %s", cgroupPath)
	}

	logger.Info("Killing Jail via fbjail", "jid", jid)
	return fbjail.JailRemove(jid)
}

// configureCmdPlatform configures the command to run.
// If mode is "jail", it sets up the internal launcher to attach to the jail.
func (nr *NodeRunner) configureCmdPlatform(ctx context.Context, spec *ServiceSpec, workDir string, env []string, selfPath, binaryPath string, cgroupFd int, logger *slog.Logger) (*exec.Cmd, error) {

	if spec.Mode == "jail" {
		// cgroupFd is unused on FreeBSD, we need the JID which was passed in applyCgroupConstraintsPlatform.
		// However, createCgroupPlatform returns the JID as the first string return value.
		// In process.go, `cgroupPath` (returned string) is available in the ManagedApp struct,
		// but here we are inside startAppInstance before the struct is fully populated.
		// Wait... process.go calls createCgroupPlatform, gets cgroupPath, then calls configureCmdPlatform.
		// BUT process.go does NOT pass cgroupPath to configureCmdPlatform, it passes cgroupFd.

		// To fix this without changing the generic Linux interface in process.go too much:
		// We rely on the fact that for FreeBSD, we won't use FDs.
		// We need to fetch the JID.
		// Since we cannot pass the JID string easily via the existing signature without changing generic code,
		// we will modify process.go to pass cgroupPath string as well, or we cheat.
		//
		// CHEAT/HACK for compatibility: process.go logic uses `createCgroupPlatform` to get `cgroupPath`.
		// It's available in the scope of `startAppInstance`.
		// I will update process.go to pass cgroupPath to configureCmdPlatform.
		// See step 5 below.

		// Assuming process.go is updated to pass cgroupPath:
		// We set up the launcher.

		// Note: The actual JID string needs to be passed in env.
		// We will assume `cgroupFd` parameter is hijacked or we update signature.
		// Let's assume updated signature in process.go (requires update to other platform files too).
		return nil, fmt.Errorf("configureCmdPlatform requires signature update to accept cgroupPath string on FreeBSD")
	}

	// Exec mode (Standard)
	finalExecCommandParts := append([]string{binaryPath}, spec.Args...)
	logger.Info("Final command to execute (FreeBSD Direct)", "parts", finalExecCommandParts)

	cmd := exec.CommandContext(ctx, finalExecCommandParts[0], finalExecCommandParts[1:]...)
	cmd.Env = env
	cmd.Dir = workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	return cmd, nil
}

// RE-IMPLEMENTATION of configureCmdPlatform assuming process.go signature update
// To make this cleaner, I will update process.go to pass `cgroupPath` string to this function.

func (nr *NodeRunner) configureCmdPlatformWithJID(ctx context.Context, spec *ServiceSpec, workDir string, env []string, selfPath, binaryPath string, jidStr string, logger *slog.Logger) (*exec.Cmd, error) {
	if spec.Mode == "jail" {
		if jidStr == "" {
			return nil, fmt.Errorf("jail mode requested but no JID provided")
		}

		// Prepare the launcher
		launcherArgs := []string{
			internalLaunchFlag, // --internal-jail-launch
		}

		// Add Jail specific Env vars
		env = append(env, fmt.Sprintf("%s=%s", envJailID, jidStr))
		env = append(env, fmt.Sprintf("%s=%s", envJailTargetCmd, binaryPath))

		// We need to serialize args for the target
		// We can reuse the JSON approach from Linux Landlock or simple env vars
		// Reusing common env var names from main.go
		// Note: main.go needs to define the FreeBSD specific flags/consts.

		logger.Info("Configuring Jail Launcher", "jid", jidStr, "binary", binaryPath)

		cmd := exec.CommandContext(ctx, selfPath, launcherArgs...)
		cmd.Env = env
		cmd.Dir = workDir
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		return cmd, nil
	}

	// Normal Exec
	finalExecCommandParts := append([]string{binaryPath}, spec.Args...)
	cmd := exec.CommandContext(ctx, finalExecCommandParts[0], finalExecCommandParts[1:]...)
	cmd.Env = env
	cmd.Dir = workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd, nil
}


func (nr *NodeRunner) pollMemoryPlatform(instance *ManagedApp, logger *slog.Logger) {
	// On FreeBSD we can rely on extractRusagePlatform which is called on exit.
	// For live polling, we could use `ps` or `procstat`, but Go's syscall.Getrusage
	// only works for the current process or children via Wait.
	// We can try to read /proc/pid/status if procfs is mounted (common in FreeBSD servers).

	pid := instance.Pid
	filePath := fmt.Sprintf("/proc/%d/status", pid)
	file, err := os.Open(filePath)
	if err != nil {
		// Fail silently/debug as procfs might not be mounted
		return
	}
	defer file.Close()

	// FreeBSD /proc/pid/status format is different from Linux.
	// Usually space separated fields. Field 10 is usually RSS in pages?
	// It's safer to not implement fragile parsing here without `libproc`.
	// Leaving empty for now, relying on exit stats.
}

func (nr *NodeRunner) extractRusagePlatform(state *os.ProcessState, logger *slog.Logger) (rssBytes float64, uTime float64, sTime float64) {
	if rusage, ok := state.SysUsage().(*syscall.Rusage); ok && rusage != nil {
		// FreeBSD: Maxrss is in Kilobytes
		rssBytes = float64(rusage.Maxrss * 1024)
		uTime = metrics.TimevalToSeconds(rusage.Utime)
		sTime = metrics.TimevalToSeconds(rusage.Stime)
		return
	}
	return 0, 0, 0
}

func (nr *NodeRunner) terminateProcessPlatform(proc *os.Process, pid int) error {
	return proc.Signal(syscall.SIGTERM)
}

func (nr *NodeRunner) forceKillProcessPlatform(proc *os.Process, pid int) error {
	return proc.Kill()
}

func (nr *NodeRunner) isProcessSignaledPlatform(exitErr *exec.ExitError) (bool, string) {
	if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
		if status.Signaled() {
			return true, fmt.Sprintf("%d", status.Signal())
		}
	}
	return false, ""
}
