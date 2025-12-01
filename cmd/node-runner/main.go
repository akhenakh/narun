package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"

	ll "github.com/shoenig/go-landlock"
	"golang.org/x/sys/unix"

	"github.com/akhenakh/narun/internal/noderunner"
)

const (
	internalLaunchFlag              = "--internal-landlock-launch"
	envLandlockConfigJSON           = "NARUN_INTERNAL_LANDLOCK_CONFIG_JSON"
	envLandlockTargetCmd            = "NARUN_INTERNAL_LANDLOCK_TARGET_CMD"
	envLandlockTargetArgsJSON       = "NARUN_INTERNAL_LANDLOCK_TARGET_ARGS_JSON"
	envLandlockInstanceRoot         = "NARUN_INTERNAL_INSTANCE_ROOT"
	envLandlockMountInfosJSON       = "NARUN_INTERNAL_MOUNT_INFOS_JSON"
	envLandlockLocalPorts           = "NARUN_INTERNAL_LOCAL_PORTS"
	envLandlockMetricsConfig        = "NARUN_INTERNAL_METRICS_CONFIG" // New Env var for metrics
	envTargetUID                    = "NARUN_TARGET_UID"
	envTargetGID                    = "NARUN_TARGET_GID"
	landlockLauncherErrCode         = 120 // Specific exit code for launcher errors
	landlockUnsupportedErrCode      = 121 // Specific exit code if landlock unsupported
	landlockLockFailedErrCode       = 122 // Specific exit code if l.Lock fails
	landlockTargetExecFailedCode    = 123 // Specific exit code if syscall.Exec fails
	landlockMissingInstanceRootCode = 124 // Specific exit code if NARUN_INTERNAL_INSTANCE_ROOT is missing

)

func main() {
	// Early Check for Internal Landlock Launch Mode
	// it will become the launcher process in the fork/exec sequence.
	isInternalLaunch := slices.Contains(os.Args, internalLaunchFlag)

	if isInternalLaunch {
		// This execution is the intermediate launcher process.
		runLandlockLauncher()
		// If runLandlockLauncher returns, it means syscall.Exec failed.
		os.Exit(landlockTargetExecFailedCode) // Exit with specific code if exec fails
	}

	natsURL := flag.String("nats-url", "nats://localhost:4222", "NATS server URL")
	nodeID := flag.String("node-id", "", "Unique ID for this node (defaults to hostname)")
	dataDir := flag.String("data-dir", "./narun-data", "Directory for storing binaries and data")
	logLevel := flag.String("log-level", os.Getenv("LOG_LEVEL"), "Log level (debug, info, warn, error)")
	masterKey := flag.String("master-key", os.Getenv("NARUN_MASTER_KEY"), "Base64 encoded AES-256 master key for secrets (or use NARUN_MASTER_KEY env var)")
	masterKeyPath := flag.String("master-key-path", os.Getenv("NARUN_MASTER_KEY_PATH"), "The path to the file containing the base64 encoded AES-256 master key for secrets")
	metricsAddr := flag.String("metrics-addr", ":9100", "Address for Prometheus metrics HTTP endpoint (e.g., :9100). Set to empty to disable.")
	adminAddr := flag.String("admin-addr", ":9101", "Address for admin UI HTTP endpoint (e.g., :9101). Set to empty to disable.")

	flag.Parse()

	// Setup Logger
	levelMap := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	level, ok := levelMap[strings.ToLower(*logLevel)]
	if !ok {
		slog.Warn("Invalid log level specified, using info", "level", *logLevel)
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     level,
		AddSource: true, // Add source file/line to logs
	}))
	slog.SetDefault(logger)

	if *masterKeyPath != "" {
		keyBytes, err := os.ReadFile(*masterKeyPath)
		if err != nil {
			slog.Error("Failed to read master key file", "error", err)
			os.Exit(1)
		}
		*masterKey = string(keyBytes)
	}

	// Create Node Runner
	runner, err := noderunner.NewNodeRunner(*nodeID, *natsURL, *dataDir, *masterKey, *metricsAddr, *adminAddr, logger)
	if err != nil {
		logger.Error("Failed to initialize node runner", "error", err)
		os.Exit(1)
	}

	// Handle OS Signals for Graceful Shutdown
	// This signal handler now primarily tells the runner to stop.
	// The runner's Stop() method will then manage its internal components including the metrics server.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine to listen for signals and initiate shutdown
	go func() {
		sig := <-sigChan
		logger.Info("Received OS signal, initiating shutdown...", "signal", sig.String())
		runner.Stop() // Trigger graceful shutdown in the runner
	}()

	// Run the Node Runner (this will block until shutdown)
	if err := runner.Run(); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("Node runner exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info("Node runner exited cleanly.")
	os.Exit(0)
}

// runLandlockLauncher is executed by the child "launcher" process.
// It applies Landlock and then replaces itself with the target application.
func runLandlockLauncher() {
	// Use stderr for launcher logs as stdout might be piped
	fmt.Fprintf(os.Stderr, "[narun-launcher] Process started in Landlock launcher mode.\n")

	// Check OS
	if runtime.GOOS != "linux" {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Landlock is only supported on Linux.\n")
		os.Exit(landlockUnsupportedErrCode)
	}

	// Configure loopback NETWORK SETUP (Must be done as Root)
	if err := exec.Command("ip", "addr", "add", "127.0.0.1/8", "dev", "lo").Run(); err != nil {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Warning: Failed to assign 127.0.0.1 to lo: %v\n", err)
	}
	if err := exec.Command("ip", "link", "set", "lo", "up").Run(); err != nil {
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

	// Socat needs to bind to the unix socket (which needs write perms on the directory)
	// and bind to 127.0.0.1 (which is fine for user as it's > 1024).
	// It's safer to run socat as the target user, but the unix socket directory
	// permissions must allow it. In `process.go` you did `os.Chmod(socketsDir, 0777)`,
	// so running socat as user is fine.
	// HOWEVER, if we start socat here as root, they will run as root.
	// It's better to drop privileges BEFORE starting socat if possible,
	// OR ensure socat drops them.

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
		var localPorts []noderunner.PortForward // Decode into struct slice
		if err := json.Unmarshal([]byte(localPortsJSON), &localPorts); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to parse local ports JSON: %v\n", err)
		} else {
			// Find the sockets directory from mounts (source="local-sockets")
			var socketsDir string
			for _, m := range mountInfos {
				if m.Source == "local-sockets" {
					// Use Path as the guest-visible path (which is now absolute)
					socketsDir = m.Path
					break
				}
			}

			if socketsDir == "" {
				fmt.Fprintf(os.Stderr, "[narun-launcher] Warning: Local ports configured but no 'local-sockets' mount found.\n")
			} else {
				for _, pf := range localPorts {
					port := pf.Port
					proto := pf.Protocol

					// Must match naming convention in process.go
					socketPath := fmt.Sprintf("%s/%d_%s.sock", socketsDir, port, proto)

					// Determine Listen Type
					listenAddr := fmt.Sprintf("TCP-LISTEN:%d,fork,bind=127.0.0.1", port)
					if proto == "udp" {
						listenAddr = fmt.Sprintf("UDP-LISTEN:%d,fork,bind=127.0.0.1", port)
					}

					// Guest Side: Listen on Localhost Port -> Connect to Unix Socket
					args := []string{
						listenAddr,
						fmt.Sprintf("UNIX-CONNECT:%s", socketPath),
					}

					cmd := exec.Command("socat", args...)
					cmd.Stdout = os.Stdout
					cmd.Stderr = os.Stderr

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
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
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

	// Ensure NARUN_INSTANCE_ROOT is correctly set for the final application
	// It was retrieved from the environment above, so it will be part of os.Environ()
	// If we wanted to be absolutely sure or override, we could:
	// envv = append(envv, fmt.Sprintf("NARUN_INSTANCE_ROOT=%s", instanceRoot))
	// However, since instanceRoot is already read from the environment set by the parent,
	// it will be passed along correctly via os.Environ().

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

	// Create ifreq structure for "lo"
	// struct ifreq {
	//     char ifr_name[IFNAMSIZ]; /* Interface name */
	//     union {
	//         struct sockaddr ifr_addr;
	//         ...
	//         short ifr_flags;
	//     };
	// };
	// In Go unix package, standard way involves raw bytes or specific structs depending on arch.
	// A simpler cross-platform Go way for Linux is manipulating the flags directly.

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
