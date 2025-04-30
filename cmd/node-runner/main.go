package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/akhenakh/narun/internal/noderunner"
)

func main() {
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
