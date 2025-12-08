//go:build !linux && !freebsd

package noderunner

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
)

func (nr *NodeRunner) createIsolationPlatform(spec *ServiceSpec, instanceID string, logger *slog.Logger) (isolationID string, isolationFd int, cleanup func() error, err error) {
	return "", -1, nil, fmt.Errorf("isolation is only supported on Linux (Cgroups) and FreeBSD (Jails)")
}

func (nr *NodeRunner) applyResourceLimitsPlatform(spec *ServiceSpec, isolationID string, logger *slog.Logger) error {
	return nil
}

func (nr *NodeRunner) destroyIsolationPlatform(isolationID string, logger *slog.Logger) error {
	return nil
}

// configureCmdPlatform stub for non-Linux
func (nr *NodeRunner) configureCmdPlatform(ctx context.Context, spec *ServiceSpec, workDir string, env []string, selfPath, binaryPath string, isolationFd int, isolationID string, logger *slog.Logger) (*exec.Cmd, error) {
	// Direct execution fallback, ignoring namespaces/landlock/cgroups
	finalExecCommandParts := append([]string{binaryPath}, spec.Args...)
	logger.Info("Final command to execute (Non-Linux Direct Exec)", "parts", finalExecCommandParts)

	cmd := exec.CommandContext(ctx, finalExecCommandParts[0], finalExecCommandParts[1:]...)
	cmd.Env = env
	cmd.Dir = workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	return cmd, nil
}

// pollMemoryPlatform stub
func (nr *NodeRunner) pollMemoryPlatform(instance *ManagedApp, logger *slog.Logger) {
	// Not implemented for non-Linux
}

func (nr *NodeRunner) extractRusagePlatform(state *os.ProcessState, logger *slog.Logger) (rssBytes float64, uTime float64, sTime float64) {
	// Basic generic fallback if available, or 0
	// syscall.Rusage exists on Darwin/BSD, but fields might differ.
	// For simplicity in this stub:
	return 0, 0, 0
}

func (nr *NodeRunner) terminateProcessPlatform(proc *os.Process, pid int) error {
	// Portable signal is limited to Kill (SIGKILL) or Interrupt (SIGINT).
	// os.Process.Signal(os.Interrupt) or syscall.SIGTERM if available in generic syscall (it usually is)
	return proc.Signal(syscall.SIGTERM)
}

func (nr *NodeRunner) forceKillProcessPlatform(proc *os.Process, pid int) error {
	return proc.Kill()
}

func (nr *NodeRunner) isProcessSignaledPlatform(exitErr *exec.ExitError) (bool, string) {
	// Generic checking of signal status is tricky without OS specific syscall.WaitStatus logic
	return false, ""
}
