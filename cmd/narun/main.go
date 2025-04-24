package main

import (
	"context"
	"errors"
	"flag"
	"fmt" // Added for error formatting in server error channel
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync" // Added for WaitGroup
	"syscall"
	"time"

	"github.com/akhenakh/narun/internal/config"
	"github.com/akhenakh/narun/internal/handler"
	"github.com/akhenakh/narun/internal/natsutil"

	// Import metrics package to register collectors
	_ "github.com/akhenakh/narun/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// (Logging setup unchanged)
	logLevel := slog.LevelInfo
	if levelStr := os.Getenv("LOG_LEVEL"); levelStr != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(levelStr)); err == nil {
			logLevel = level
		} else {
			slog.Warn("Invalid LOG_LEVEL specified, using default", "level", levelStr, "default", logLevel.String())
		}
	}
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true,
	})
	logger := slog.New(jsonHandler)
	slog.SetDefault(logger)

	configFile := flag.String("config", "config.yaml", "Path to the configuration file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		slog.Error("Failed to load configuration", "file", *configFile, "error", err)
		os.Exit(1)
	}
	slog.Info("Configuration loaded successfully", "file", *configFile)

	nc, err := natsutil.ConnectNATS(cfg.NatsURL)
	if err != nil {
		slog.Error("Failed to connect to NATS", "url", cfg.NatsURL, "error", err)
		os.Exit(1)
	}
	defer nc.Close()
	slog.Info("Connected to NATS server", "url", nc.ConnectedUrl())

	// Setup JetStream and get the context
	js, err := natsutil.SetupJetStream(logger, nc, cfg.NatsStream) // Get JS context
	if err != nil {
		slog.Error("Failed to setup JetStream", "stream", cfg.NatsStream, "error", err)
		os.Exit(1)
	}
	slog.Info("JetStream setup complete", "stream", cfg.NatsStream)

	// Pass the JetStream context to the handler
	httpHandler := handler.NewHttpHandler(logger, nc, js, cfg) // Pass js context

	// (Server mux and setup unchanged)
	mainMux := http.NewServeMux()
	mainMux.Handle("/", httpHandler)
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	mainServer := &http.Server{
		Addr:              cfg.ServerAddr,
		Handler:           mainMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	metricsServer := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// (Server startup and shutdown logic unchanged)
	serverErrChan := make(chan error, 2)
	go func() {
		slog.Info("Starting main HTTP server", "addr", cfg.ServerAddr)
		err := mainServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Main HTTP server ListenAndServe error", "error", err)
			serverErrChan <- fmt.Errorf("main server error: %w", err)
		} else {
			slog.Info("Main HTTP server shut down")
			serverErrChan <- nil
		}
	}()
	go func() {
		slog.Info("Starting metrics server", "addr", cfg.MetricsAddr)
		err := metricsServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Metrics server ListenAndServe error", "error", err)
			serverErrChan <- fmt.Errorf("metrics server error: %w", err)
		} else {
			slog.Info("Metrics server shut down")
			serverErrChan <- nil
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-serverErrChan:
		if err != nil {
			slog.Error("Server failed to start or unexpectedly stopped", "error", err)
		} else {
			slog.Info("A server shut down cleanly (likely during shutdown process)")
		}
	case sig := <-quit:
		slog.Info("Shutdown signal received", "signal", sig.String())
	}
	slog.Info("Initiating graceful shutdown...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdownWg := &sync.WaitGroup{}
	shutdownWg.Add(2)
	go func() {
		defer shutdownWg.Done()
		if err := mainServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("Main HTTP server shutdown error", "error", err)
		} else {
			slog.Info("Main HTTP server gracefully stopped")
		}
	}()
	go func() {
		defer shutdownWg.Done()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("Metrics server shutdown error", "error", err)
		} else {
			slog.Info("Metrics server gracefully stopped")
		}
	}()
	shutdownComplete := make(chan struct{})
	go func() {
		shutdownWg.Wait()
		close(shutdownComplete)
	}()
	select {
	case <-shutdownComplete:
		slog.Info("All servers gracefully shut down.")
	case <-shutdownCtx.Done():
		slog.Error("Shutdown timeout exceeded.")
	}
	close(serverErrChan)
	for err := range serverErrChan {
		if err != nil {
			slog.Warn("Server reported error during shutdown process", "error", err)
		}
	}
	slog.Info("Server exiting.")
}
