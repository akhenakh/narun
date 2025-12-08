//go:build linux

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"

	"github.com/akhenakh/narun/internal/noderunner"
	ll "github.com/shoenig/go-landlock"
	"golang.org/x/sys/unix"
)

// runLauncher is executed by the child "launcher" process.
// It applies Landlock and then replaces itself with the target application.
func runLauncher() {
	// Use stderr for launcher logs as stdout might be piped
	fmt.Fprintf(os.Stderr, "[narun-launcher] Process started in Landlock launcher mode.\n")

	// Configure loopback NETWORK SETUP (Must be done as Root)
	if err := exec.Command("ip", "addr", "add", "127.0.0.1/8", "dev", "lo").Run(); err != nil {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Warning: Failed to assign 127.0.0.1 to lo: %v\n", err)
	}
	if err := enableLoopback(); err != nil {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Warning: Failed to set lo UP: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Loopback interface configured.\n")
	}

	// Retrieve Configuration from Environment Variables
	configJSON := os.Getenv(envLandlockConfigJSON)
	targetCmdPath := os.Getenv(envLandlockTargetCmd) // Path to the actual application binary
	targetArgsJSON := os.Getenv(envLandlockTargetArgsJSON)
	instanceRoot := os.Getenv(envLandlockInstanceRoot) // Get instance root from env
	mountInfosJSON := os.Getenv(envLandlockMountInfosJSON)
	localPortsJSON := os.Getenv(envLandlockLocalPorts)
	metricsConfigJSON := os.Getenv(envLandlockMetricsConfig)

	//  DROP PRIVILEGES
	targetUIDStr := os.Getenv(envTargetUID)
	targetGIDStr := os.Getenv(envTargetGID)

	if targetUIDStr != "" && targetGIDStr != "" {
		uid, _ := strconv.Atoi(targetUIDStr)
		gid, _ := strconv.Atoi(targetGIDStr)

		fmt.Fprintf(os.Stderr, "[narun-launcher] Dropping privileges to UID: %d, GID: %d\n", uid, gid)

		// Set GID first
		if err := syscall.Setgid(gid); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to set GID: %v\n", err)
			os.Exit(landlockLauncherErrCode)
		}

		// Set UID
		if err := syscall.Setuid(uid); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to set UID: %v\n", err)
			os.Exit(landlockLauncherErrCode)
		}
	}

	if configJSON == "" || targetCmdPath == "" || targetArgsJSON == "" {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Missing required environment variables for basic operation (%s, %s, %s).\n",
			envLandlockConfigJSON, envLandlockTargetCmd, envLandlockTargetArgsJSON)
		os.Exit(landlockLauncherErrCode)
	}

	if instanceRoot == "" {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Missing required environment variable %s.\n", envLandlockInstanceRoot)
		os.Exit(landlockMissingInstanceRootCode)
	}
	fmt.Fprintf(os.Stderr, "[narun-launcher] Instance root: %s\n", instanceRoot)

	var landlockSpec noderunner.LandlockSpec
	if err := json.Unmarshal([]byte(configJSON), &landlockSpec); err != nil {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to parse Landlock config JSON: %v\n", err)
		os.Exit(landlockLauncherErrCode)
	}

	var targetArgs []string
	if err := json.Unmarshal([]byte(targetArgsJSON), &targetArgs); err != nil {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to parse Landlock target args JSON: %v\n", err)
		os.Exit(landlockLauncherErrCode)
	}

	var mountInfos []noderunner.MountInfoForEnv
	if mountInfosJSON != "" {
		if err := json.Unmarshal([]byte(mountInfosJSON), &mountInfos); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to parse mount info JSON: %v\n", err)
			os.Exit(landlockLauncherErrCode)
		}
	}

	fmt.Fprintf(os.Stderr, "[narun-launcher] Target application path: %s\n", targetCmdPath)
	fmt.Fprintf(os.Stderr, "[narun-launcher] Target args: %v\n", targetArgs)

	// Start Guest-Side Socat Proxies (if configured)
	if localPortsJSON != "" {
		var localPorts []noderunner.PortForward
		if err := json.Unmarshal([]byte(localPortsJSON), &localPorts); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to parse local ports JSON: %v\n", err)
		} else {
			// Find the sockets directory from mounts (source="local-sockets")
			var socketsDir string
			for _, m := range mountInfos {
				if m.Source == "local-sockets" {
					socketsDir = m.Path
					break
				}
			}

			if socketsDir == "" {
				fmt.Fprintf(os.Stderr, "[narun-launcher] Warning: Local ports configured but no 'local-sockets' mount found.\n")
			} else {
				for _, pf := range localPorts {
					// Guest Side: Listen on Localhost Port -> Connect to Unix Socket
					port := pf.Port
					proto := pf.Protocol

					// Must match naming convention in process.go
					socketPath := fmt.Sprintf("%s/%d_%s.sock", socketsDir, port, proto)

					// Determine Listen Type
					listenAddr := fmt.Sprintf("TCP-LISTEN:%d,fork,bind=127.0.0.1", port)
					if proto == "udp" {
						listenAddr = fmt.Sprintf("UDP-LISTEN:%d,fork,bind=127.0.0.1", port)
					}

					args := []string{
						listenAddr,
						fmt.Sprintf("UNIX-CONNECT:%s", socketPath),
					}

					cmd := exec.Command("socat", args...)
					//  Do NOT attach Stdout/Stderr to avoid holding pipes open
					// Ensure socat dies when parent (launcher/app) dies
					cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}

					if err := cmd.Start(); err != nil {
						fmt.Fprintf(os.Stderr, "[narun-launcher] Failed to start guest socat for %s/%d: %v\n", proto, port, err)
					} else {
						fmt.Fprintf(os.Stderr, "[narun-launcher] Started guest socat for %s/%d (pid %d)\n", proto, port, cmd.Process.Pid)
					}
				}
			}
		}
	}

	// Start Guest-Side Metrics Proxy (Host -> Guest / Inbound)
	if metricsConfigJSON != "" {
		var mc struct {
			Socket     string `json:"socket"`
			TargetPort int    `json:"targetPort"`
		}
		if err := json.Unmarshal([]byte(metricsConfigJSON), &mc); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-launcher] Error parsing metrics config: %v\n", err)
		} else {
			// Guest Side: Listen on Socket -> Forward to App
			args := []string{
				fmt.Sprintf("UNIX-LISTEN:%s,fork,reuseaddr,mode=777", mc.Socket),
				fmt.Sprintf("TCP:127.0.0.1:%d", mc.TargetPort),
			}

			cmd := exec.Command("socat", args...)
			// Do NOT attach Stdout/Stderr to avoid holding pipes open
			// Ensure socat dies when parent (launcher/app) dies
			cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}

			if err := cmd.Start(); err != nil {
				fmt.Fprintf(os.Stderr, "[narun-launcher] Failed to start metrics proxy: %v\n", err)
			} else {
				fmt.Fprintf(os.Stderr, "[narun-launcher] Started metrics proxy (pid %d) -> :%d\n", cmd.Process.Pid, mc.TargetPort)
			}
		}
	}

	// Build Landlock Paths from Spec
	var landlockPaths []*ll.Path
	if landlockSpec.Shared {
		landlockPaths = append(landlockPaths, ll.Shared())
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock group: Shared\n")
	}
	if landlockSpec.Stdio {
		landlockPaths = append(landlockPaths, ll.Stdio())
	}
	if landlockSpec.TTY {
		landlockPaths = append(landlockPaths, ll.TTY())
	}
	if landlockSpec.Tmp {
		landlockPaths = append(landlockPaths, ll.Tmp())
	}
	if landlockSpec.VMInfo {
		landlockPaths = append(landlockPaths, ll.VMInfo())
	}
	if landlockSpec.DNS {
		landlockPaths = append(landlockPaths, ll.DNS())
	}
	if landlockSpec.Certs {
		landlockPaths = append(landlockPaths, ll.Certs())
	}

	// Add Landlock rules for mounted files (read from environment)
	if len(mountInfos) > 0 {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying rules for %d mounted file(s)...\n", len(mountInfos))
		for _, mount := range mountInfos {
			// If it's a socket directory mount (for localPorts), we need RW access
			if mount.Source == "local-sockets" {
				// Use the Path which is now the absolute path on host (and guest view)
				landlockPaths = append(landlockPaths, ll.Dir(mount.Path, "rwc"))
				fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock socket dir rule: Path=%s Modes=rwc\n", mount.Path)
			} else {
				// Standard mounts are read-only for now
				landlockPaths = append(landlockPaths, ll.File(mount.ResolvedAbs, "r"))
				fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock mount rule: Path=%s Modes=r\n", mount.ResolvedAbs)
			}
		}
	}

	for _, p := range landlockSpec.Paths {
		landlockPaths = append(landlockPaths, ll.File(p.Path, p.Modes))
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock custom path: Path=%s Modes=%s\n", p.Path, p.Modes)
	}

	// Crucially, allow read+execute access to the target application binary itself.
	landlockPaths = append(landlockPaths, ll.File(targetCmdPath, "rx"))
	fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock access for target binary: Path=%s Modes=rx\n", targetCmdPath)

	// Apply Landlock Rules to this launcher process
	l := ll.New(landlockPaths...)
	applyMode := ll.Mandatory
	fmt.Fprintf(os.Stderr, "[narun-launcher] Attempting to apply Landlock rules (Mode: %v)...\n", applyMode)
	err := l.Lock(applyMode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to apply Landlock rules: %v\n", err)
		os.Exit(landlockLockFailedErrCode)
	}
	fmt.Fprintf(os.Stderr, "[narun-launcher] Landlock rules applied successfully.\n")

	// Prepare arguments for syscall.Exec
	argv := []string{targetCmdPath}
	argv = append(argv, targetArgs...)

	// Prepare environment
	envv := os.Environ()

	// Execute the Target Application (Replace this Launcher Process)
	fmt.Fprintf(os.Stderr, "[narun-launcher] Executing target application: %s with env: %v\n", targetCmdPath, envv)
	err = syscall.Exec(targetCmdPath, argv, envv)

	// If syscall.Exec returns, it's always an error.
	fmt.Fprintf(os.Stderr, "[narun-launcher] Error: syscall.Exec failed for target '%s': %v\n", targetCmdPath, err)
}

// enableLoopback brings up the 'lo' interface using netlink/ioctl via unix package.
// Equivalent to `ip link set lo up`.
func enableLoopback() error {
	s, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, 0)
	if err != nil {
		return fmt.Errorf("socket: %w", err)
	}
	defer unix.Close(s)

	ifName := "lo"
	ifr, err := unix.NewIfreq(ifName)
	if err != nil {
		return fmt.Errorf("new ifreq: %w", err)
	}

	// Get current flags (SIOCGIFFLAGS)
	if err := unix.IoctlIfreq(s, unix.SIOCGIFFLAGS, ifr); err != nil {
		return fmt.Errorf("ioctl get flags: %w", err)
	}

	// Set IFF_UP and IFF_RUNNING (usually 0x1 | 0x40)
	flags := ifr.Uint16()
	flags |= unix.IFF_UP | unix.IFF_RUNNING
	ifr.SetUint16(flags)

	// Set new flags (SIOCSIFFLAGS)
	if err := unix.IoctlIfreq(s, unix.SIOCSIFFLAGS, ifr); err != nil {
		return fmt.Errorf("ioctl set flags: %w", err)
	}

	return nil
}
