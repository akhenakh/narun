//go:build freebsd

package noderunner

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/akhenakh/narun/internal/fbjail"
	"github.com/akhenakh/narun/internal/metrics"
)

// createIsolationPlatform creates a FreeBSD Jail.
// Returns the JID as the isolationID string.
func (nr *NodeRunner) createIsolationPlatform(spec *ServiceSpec, instanceID string, logger *slog.Logger) (isolationID string, isolationFd int, cleanup func() error, err error) {
	if spec.Mode != "jail" {
		return "", -1, nil, nil
	}

	jailName := fmt.Sprintf("narun_%s", strings.ReplaceAll(instanceID, "-", "_"))
	jailPath := fmt.Sprintf("%s/instances/%s/work", nr.dataDir, instanceID)

	if _, err := os.Stat(jailPath); os.IsNotExist(err) {
		return "", -1, nil, fmt.Errorf("jail root path does not exist: %s", jailPath)
	}

	// Manage IP Aliases on Host Interfaces
	// We must do this BEFORE creating the jail so the IPs are valid on the host.
	if err := nr.manageIpAliases(spec.Jail.IP4Addresses, true, logger); err != nil {
		return "", -1, nil, fmt.Errorf("failed to add IP aliases: %w", err)
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
		Persist:          true,
		CopyResolvConf:   true,
		MountSystemCerts: spec.Jail.MountSystemCerts,
	}

	if cfg.Hostname == "" {
		cfg.Hostname = spec.Name
	}

	manager := fbjail.NewManager(cfg)

	logger.Info("Creating Jail", "name", jailName, "path", jailPath)
	if err := manager.Create(); err != nil {
		// Cleanup aliases if creation fails
		_ = nr.manageIpAliases(spec.Jail.IP4Addresses, false, logger)
		return "", -1, nil, fmt.Errorf("failed to create jail: %w", err)
	}

	cleanupFunc := func() error {
		logger.Info("Stopping Jail", "name", jailName)
		_ = exec.Command("rctl", "-r", fmt.Sprintf("jail:%s", jailName)).Run()
		stopErr := manager.Stop()
		// Remove aliases after stop
		aliasErr := nr.manageIpAliases(spec.Jail.IP4Addresses, false, logger)

		if stopErr != nil {
			return stopErr
		}
		return aliasErr
	}

	return strconv.Itoa(manager.JID), -1, cleanupFunc, nil
}

// manageIpAliases adds or removes IP aliases from host interfaces based on subnet matching.
func (nr *NodeRunner) manageIpAliases(ips []string, add bool, logger *slog.Logger) error {
	if len(ips) == 0 {
		return nil
	}

	interfaces, err := net.Interfaces()
	if err != nil {
		return fmt.Errorf("failed to list interfaces: %w", err)
	}

	for _, ipStr := range ips {
		ip := net.ParseIP(ipStr)
		if ip == nil {
			logger.Warn("Invalid IP format in config, skipping alias", "ip", ipStr)
			continue
		}
		ip4 := ip.To4()
		if ip4 == nil {
			logger.Warn("Only IPv4 aliases supported currently, skipping", "ip", ipStr)
			continue
		}

		// Find suitable interface
		var targetIface string

		// Check Loopback
		if ip4.IsLoopback() {
			targetIface = "lo0" // Default freebsd loopback
		} else {
			//  Scan interfaces for matching subnet
			for _, iface := range interfaces {
				addrs, err := iface.Addrs()
				if err != nil {
					continue
				}
				for _, addr := range addrs {
					if ipNet, ok := addr.(*net.IPNet); ok && ipNet.IP.To4() != nil {
						// Check if our IP fits in this subnet
						if ipNet.Contains(ip4) {
							targetIface = iface.Name
							break
						}
					}
				}
				if targetIface != "" {
					break
				}
			}
		}

		if targetIface == "" {
			logger.Warn("Could not find matching host interface subnet for IP. Alias NOT added. Jail networking might fail.", "ip", ipStr)
			continue
		}

		// Execute ifconfig
		// ifconfig <iface> <ip>/32 alias
		// ifconfig <iface> <ip>/32 -alias
		action := "alias"
		if !add {
			action = "-alias"
		}

		// Use /32 for aliases to avoid subnet overlap issues
		cidr := fmt.Sprintf("%s/32", ipStr)

		logger.Info("Managing IP alias", "action", action, "interface", targetIface, "ip", cidr)
		cmd := exec.Command("ifconfig", targetIface, "inet", cidr, action)
		if out, err := cmd.CombinedOutput(); err != nil {
			// Ignore "Can't assign requested address" on delete if it was already gone
			if !add && strings.Contains(string(out), "Can't assign requested address") {
				continue
			}
			return fmt.Errorf("ifconfig failed for %s on %s: %v (%s)", cidr, targetIface, err, string(out))
		}
	}
	return nil
}

// applyResourceLimitsPlatform applies RCTL resource limits to the Jail (isolationID is the JID).
func (nr *NodeRunner) applyResourceLimitsPlatform(spec *ServiceSpec, isolationID string, logger *slog.Logger) error {
	jid := isolationID
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

// destroyIsolationPlatform kills the jail.
func (nr *NodeRunner) destroyIsolationPlatform(isolationID string, logger *slog.Logger) error {
	if isolationID == "" {
		return nil
	}
	jid, err := strconv.Atoi(isolationID)
	if err != nil {
		return fmt.Errorf("invalid JID: %s", isolationID)
	}

	logger.Info("Killing Jail via fbjail", "jid", jid)
	return fbjail.JailRemove(jid)
}

// configureCmdPlatform configures the command to run.
func (nr *NodeRunner) configureCmdPlatform(ctx context.Context, spec *ServiceSpec, workDir string, env []string, selfPath, binaryPath string, isolationFd int, isolationID string, logger *slog.Logger) (*exec.Cmd, error) {
	if spec.Mode == "jail" {
		if isolationID == "" {
			return nil, fmt.Errorf("jail mode requested but no JID (isolationID) provided")
		}

		// The target binary must be accessible INSIDE the jail.
		// Copy/Hardlink it to the jail root (workDir).
		// Host path: workDir/app_binary
		// Jail path: /app_binary
		jailBinaryName := "app_binary"
		hostBinaryDest := filepath.Join(workDir, jailBinaryName)

		// Remove if exists to ensure clean state
		os.Remove(hostBinaryDest)

		// Try hardlink first (fast), fallback to copy
		if err := os.Link(binaryPath, hostBinaryDest); err != nil {
			logger.Debug("Hardlink failed, falling back to copy", "error", err)
			if err := copyFile(binaryPath, hostBinaryDest); err != nil {
				return nil, fmt.Errorf("failed to copy binary to jail root: %w", err)
			}
			if err := os.Chmod(hostBinaryDest, 0755); err != nil {
				return nil, fmt.Errorf("failed to chmod jail binary: %w", err)
			}
		}

		// Prepare the launcher (internal self-call)
		launcherArgs := []string{
			internalLaunchFlag, // --internal-launch (shared flag)
		}

		// Add Jail specific Env vars
		// We pass the JID (isolationID) to the launcher so it knows what to attach to
		env = append(env, fmt.Sprintf("%s=%s", envJailID, isolationID))
		// TARGET CMD MUST BE PATH INSIDE JAIL
		env = append(env, fmt.Sprintf("%s=/%s", envJailTargetCmd, jailBinaryName))

		logger.Info("Configuring Jail Launcher", "jid", isolationID, "binary_internal", "/"+jailBinaryName)

		cmd := exec.CommandContext(ctx, selfPath, launcherArgs...)
		cmd.Env = env
		cmd.Dir = workDir
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		return cmd, nil
	}

	// Normal Exec
	finalExecCommandParts := append([]string{binaryPath}, spec.Args...)
	logger.Info("Final command to execute (FreeBSD Direct)", "parts", finalExecCommandParts)

	cmd := exec.CommandContext(ctx, finalExecCommandParts[0], finalExecCommandParts[1:]...)
	cmd.Env = env
	cmd.Dir = workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	return cmd, nil
}

func (nr *NodeRunner) pollMemoryPlatform(instance *ManagedApp, logger *slog.Logger) {
	// On FreeBSD, isolationID holds the JID string.
	if instance.isolationID == "" {
		return
	}

	// Use rctl to query the jail's resource usage.
	cmd := exec.Command("rctl", "-u", "jail:"+instance.isolationID)
	out, err := cmd.Output()
	if err != nil {
		return
	}

	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		// Look for the memoryuse usage line
		if strings.Contains(line, ":memoryuse:usage=") {
			parts := strings.Split(line, "=")
			if len(parts) == 2 {
				valStr := strings.TrimSpace(parts[1])
				memBytes, err := strconv.ParseFloat(valStr, 64)
				if err == nil {
					metrics.NarunNodeRunnerInstanceMemoryMaxRSSBytes.WithLabelValues(
						InstanceIDToAppName(instance.InstanceID),
						instance.InstanceID,
						nr.nodeID,
						instance.RunID,
					).Set(memBytes)
					return // Found and updated
				} else {
					logger.Debug("Failed to parse rctl memory value", "value", valStr, "error", err)
				}
			}
		}
	}
}

func (nr *NodeRunner) extractRusagePlatform(state *os.ProcessState, logger *slog.Logger) (rssBytes float64, uTime float64, sTime float64) {
	if rusage, ok := state.SysUsage().(*syscall.Rusage); ok && rusage != nil {
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

// Helper function to copy file
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}
