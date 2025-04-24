package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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
	// Setup structured JSON logger
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})
	logger := slog.New(jsonHandler)
	slog.SetDefault(logger)

	configFile := flag.String("config", "config.yaml", "Path to the configuration file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.ConnectNATS(cfg.NatsURL)
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	// Use the global NatsStream from the config
	_, err = natsutil.SetupJetStream(logger, nc, cfg.NatsStream) // Pass the single stream name
	if err != nil {
		slog.Error("Failed to setup JetStream", "error", err)
		os.Exit(1)
	}

	httpHandler := handler.NewHttpHandler(logger, nc, cfg)

	mainMux := http.NewServeMux()
	mainMux.Handle("/", httpHandler) // Handle all paths through our handler

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler()) // Expose Prometheus metrics

	mainServer := &http.Server{
		Addr:    cfg.ServerAddr,
		Handler: mainMux,
	}

	metricsServer := &http.Server{
		Addr:    cfg.MetricsAddr,
		Handler: metricsMux,
	}

	go func() {
		slog.Info("Starting main HTTP server", "addr", cfg.ServerAddr)
		if err := mainServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Main HTTP server ListenAndServe error", "error", err)
			os.Exit(1)
		}
		slog.Info("Main HTTP server shut down")
	}()

	go func() {
		slog.Info("Starting metrics server", "addr", cfg.MetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Metrics server ListenAndServe error", "error", err)
			os.Exit(1)
		}
		slog.Info("Metrics server shut down")
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("Shutdown signal received, initiating graceful shutdown")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Adjust timeout as needed
	defer cancel()

	if err := mainServer.Shutdown(ctx); err != nil {
		slog.Error("Main HTTP server shutdown error", "error", err)
	} else {
		slog.Info("Main HTTP server gracefully stopped")
	}

	if err := metricsServer.Shutdown(ctx); err != nil {
		slog.Error("Metrics server shutdown error", "error", err)
	} else {
		slog.Info("Metrics server gracefully stopped")
	}

	slog.Info("Server exiting")
}
