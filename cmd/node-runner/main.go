package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	ll "github.com/shoenig/go-landlock"

	"slices"

	"github.com/akhenakh/narun/internal/noderunner"
)

const (
	internalLaunchFlag           = "--internal-landlock-launch"
	envLandlockConfigJSON        = "NARUN_INTERNAL_LANDLOCK_CONFIG_JSON"
	envLandlockTargetCmd         = "NARUN_INTERNAL_LANDLOCK_TARGET_CMD"
	envLandlockTargetArgsJSON    = "NARUN_INTERNAL_LANDLOCK_TARGET_ARGS_JSON"
	landlockLauncherErrCode      = 120 // Specific exit code for launcher errors
	landlockUnsupportedErrCode   = 121 // Specific exit code if landlock unsupported
	landlockLockFailedErrCode    = 122 // Specific exit code if l.Lock fails
	landlockTargetExecFailedCode = 123 // Specific exit code if syscall.Exec fails
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
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	masterKey := flag.String("master-key", os.Getenv("NARUN_MASTER_KEY"), "Base64 encoded AES-256 master key for secrets (or use NARUN_MASTER_KEY env var)")
	masterKeyPath := flag.String("master-key-path", os.Getenv("NARUN_MASTER_KEY_PATH"), "The path to the file containing the base64 encoded AES-256 master key for secrets")

	flag.Parse()

	// Setup Logger
	levelMap := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	}
	level, ok := levelMap[*logLevel]
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
	runner, err := noderunner.NewNodeRunner(*nodeID, *natsURL, *dataDir, *masterKey, logger)
	if err != nil {
		logger.Error("Failed to initialize node runner", "error", err)
		os.Exit(1)
	}

	// Handle OS Signals for Graceful Shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigChan
		logger.Info("Received OS signal, initiating shutdown...", "signal", sig.String())
		runner.Stop() // Trigger graceful shutdown in the runner
	}()

	// Run the Node Runner
	if err := runner.Run(); err != nil && err != context.Canceled {
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

	if configJSON == "" || targetCmdPath == "" || targetArgsJSON == "" {
		fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Missing required environment variables (%s, %s, %s).\n",
			envLandlockConfigJSON, envLandlockTargetCmd, envLandlockTargetArgsJSON)
		os.Exit(landlockLauncherErrCode)
	}

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
	mountInfosJSON := os.Getenv("NARUN_INTERNAL_MOUNT_INFOS_JSON")
	if mountInfosJSON != "" {
		var mountInfos []noderunner.MountInfoForEnv
		if err := json.Unmarshal([]byte(mountInfosJSON), &mountInfos); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to parse mount info JSON: %v\n", err)
			os.Exit(landlockLauncherErrCode)
		}
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying rules for %d mounted file(s)...\n", len(mountInfos))
		for _, mount := range mountInfos {
			// Assume read-only access for mounted files by default.
			// Use the absolute path resolved by the parent process.
			landlockPaths = append(landlockPaths, ll.File(mount.ResolvedAbs, "r"))
			fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock mount rule: Path=%s Modes=r\n", mount.ResolvedAbs)
		}
	}

	for _, p := range landlockSpec.Paths {
		// Use ll.File - it should handle directories appropriately for most access modes.
		// If specific directory creation ('c' on a dir path) is needed, ll.Dir might be required,
		// but let's start simple.
		landlockPaths = append(landlockPaths, ll.File(p.Path, p.Modes))
		fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock custom path: Path=%s Modes=%s\n", p.Path, p.Modes)
	}

	// Crucially, allow read+execute access to the target application binary itself.
	landlockPaths = append(landlockPaths, ll.File(targetCmdPath, "rx"))
	fmt.Fprintf(os.Stderr, "[narun-launcher] Applying Landlock access for target binary: Path=%s Modes=rx\n", targetCmdPath)

	// Apply Landlock Rules to this launcher process
	l := ll.New(landlockPaths...)
	// Use Mandatory: fail hard if Landlock isn't supported or rules are bad.
	// Use OnlySupported if you want to allow running on older kernels without Landlock, but still attempt it.
	// For security, Mandatory is often preferred.
	applyMode := ll.Mandatory
	fmt.Fprintf(os.Stderr, "[narun-launcher] Attempting to apply Landlock rules (Mode: %v)...\n", applyMode)
	err := l.Lock(applyMode)
	if err != nil {
		// Check for unsupported error specifically if needed, though Mandatory implies failure.
		// Example check (might depend on library error types):
		// if errors.Is(err, ll.ErrLandlockUnsupported) { exitCode = landlockUnsupportedErrCode }
		fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Failed to apply Landlock rules: %v\n", err)
		os.Exit(landlockLockFailedErrCode)
	}
	fmt.Fprintf(os.Stderr, "[narun-launcher] Landlock rules applied successfully.\n")

	// Prepare arguments for syscall.Exec
	// argv[0] must be the path to the executable being run (the target application).
	argv := []string{targetCmdPath}
	argv = append(argv, targetArgs...) // Append args from the spec

	// Prepare environment for the final application process
	// Start with the inherited environment (which includes NARUN vars set by parent)
	envv := os.Environ() // Inherit the environment prepared by the parent node-runner
	// Add the instance root based on the launcher's working directory
	workDir := os.Getenv("PWD") // Should be set by the parent's cmd.Dir
	if workDir == "" {          // Add a check just in case
		fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Could not determine working directory (PWD env var missing).\n")
		os.Exit(landlockLauncherErrCode)
	}
	envv = append(envv, fmt.Sprintf("NARUN_INSTANCE_ROOT=%s", workDir))

	// Execute the Target Application (Replace this Launcher Process)
	fmt.Fprintf(os.Stderr, "[narun-launcher] Executing target application: %s ...\n", targetCmdPath)
	err = syscall.Exec(targetCmdPath, argv, envv)

	// If syscall.Exec returns, it's always an error.
	fmt.Fprintf(os.Stderr, "[narun-launcher] Error: syscall.Exec failed for target '%s': %v\n", targetCmdPath, err)
	// Exit code is set outside this function based on exec failure.
}
