package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

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
		// Implementation depends on the build tag (launcher_linux.go or launcher_other.go)
		runLandlockLauncher()
		// If runLandlockLauncher returns, it means syscall.Exec failed or OS is unsupported.
		os.Exit(landlockTargetExecFailedCode)
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
