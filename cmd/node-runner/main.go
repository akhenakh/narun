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
	"strings"
	"syscall"

	ll "github.com/shoenig/go-landlock"

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

	// Retrieve Configuration from Environment Variables
	configJSON := os.Getenv(envLandlockConfigJSON)
	targetCmdPath := os.Getenv(envLandlockTargetCmd) // Path to the actual application binary
	targetArgsJSON := os.Getenv(envLandlockTargetArgsJSON)
	instanceRoot := os.Getenv(envLandlockInstanceRoot) // Get instance root from env
	mountInfosJSON := os.Getenv(envLandlockMountInfosJSON)
	localPortsJSON := os.Getenv(envLandlockLocalPorts)

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

	fmt.Fprintf(os.Stderr, "[narun-launcher] Target application path: %s\n", targetCmdPath)
	fmt.Fprintf(os.Stderr, "[narun-launcher] Target args: %v\n", targetArgs)

	// Start Guest-Side Socat Proxies (if configured)
	if localPortsJSON != "" {
		var localPorts []noderunner.PortForward // Decode into struct slice
		if err := json.Unmarshal([]byte(localPortsJSON), &localPorts); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to parse local ports JSON: %v\n", err)
		} else {
			for _, pf := range localPorts {
				port := pf.Port
				proto := pf.Protocol

				// Must match naming convention in process.go
				socketPath := fmt.Sprintf("/.narun/sockets/%d_%s.sock", port, proto)

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

	// Build Landlock Paths from Spec
	var landlockPaths []*ll.Path
	if landlockSpec.Shared {
		landlockPaths = append(landlockPaths, ll.Shared())
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock group: Shared\n")
	}
	if landlockSpec.Stdio {
		landlockPaths = append(landlockPaths, ll.Stdio())
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock group: Stdio\n")
	}
	if landlockSpec.TTY {
		landlockPaths = append(landlockPaths, ll.TTY())
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock group: TTY\n")
	}
	if landlockSpec.Tmp {
		landlockPaths = append(landlockPaths, ll.Tmp())
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock group: Tmp\n")
	}
	if landlockSpec.VMInfo {
		landlockPaths = append(landlockPaths, ll.VMInfo())
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock group: VMInfo\n")
	}
	if landlockSpec.DNS {
		landlockPaths = append(landlockPaths, ll.DNS())
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock group: DNS\n")
	}
	if landlockSpec.Certs {
		landlockPaths = append(landlockPaths, ll.Certs())
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock group: Certs\n")
	}

	// Add Landlock rules for mounted files (read from environment)
	if mountInfosJSON != "" {
		var mountInfos []noderunner.MountInfoForEnv
		if err := json.Unmarshal([]byte(mountInfosJSON), &mountInfos); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to parse mount info JSON: %v\n", err)
			os.Exit(landlockLauncherErrCode)
		}
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying rules for %d mounted file(s)...\n", len(mountInfos))
		for _, mount := range mountInfos {
			// If it's a socket directory mount (for localPorts), we need RW access
			if mount.Source == "local-sockets" {
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

	// Prepare environment for the final application process
	// Start with the inherited environment
	envv := os.Environ()
	// Ensure NARUN_INSTANCE_ROOT is correctly set for the final application
	// It was retrieved from the environment above, so it will be part of os.Environ()
	// If we wanted to be absolutely sure or override, we could:
	// envv = append(envv, fmt.Sprintf("NARUN_INSTANCE_ROOT=%s", instanceRoot))
	// However, since instanceRoot is already read from the environment set by the parent,
	// it will be passed along correctly via os.Environ().

	// Execute the Target Application (Replace this Launcher Process)
	fmt.Fprintf(os.Stderr, "[narun-launcher] Executing target application: %s with env: %v\n", targetCmdPath, envv) // Log env for debugging
	err = syscall.Exec(targetCmdPath, argv, envv)

	// If syscall.Exec returns, it's always an error.
	fmt.Fprintf(os.Stderr, "[narun-launcher] Error: syscall.Exec failed for target '%s': %v\n", targetCmdPath, err)
	// Exit code is set outside this function based on exec failure.
}
