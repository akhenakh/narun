package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"

	"sync"
	"syscall"
	"time"

	"github.com/akhenakh/narun/internal/grpcgateway"
	"github.com/akhenakh/narun/internal/gwconfig"
	"github.com/akhenakh/narun/internal/handler"
	"github.com/akhenakh/narun/internal/natsutil"

	// Import metrics package to register collectors
	_ "github.com/akhenakh/narun/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

func main() {
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

	cfg, err := gwconfig.LoadConfig(*configFile)
	if err != nil {
		slog.Error("Failed to load configuration", "file", *configFile, "error", err)
		os.Exit(1)
	}

	nc, err := natsutil.ConnectNATS(cfg.NatsURL)
	if err != nil {
		slog.Error("Failed to connect to NATS", "url", cfg.NatsURL, "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	// Create the HTTP handler
	httpHandler := handler.NewHttpHandler(logger, nc, cfg)

	//  Server Setup
	mainMux := http.NewServeMux()
	mainMux.Handle("/", httpHandler) // Root handler for HTTP

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

	//  Conditional gRPC Server Setup
	var grpcServer *grpc.Server
	var grpcListener net.Listener
	numServers := 2 // HTTP + Metrics initially

	if cfg.GrpcAddr != "" {
		// Validate configuration found at least one gRPC route if addr is set
		hasGrpcRoute := false
		for _, r := range cfg.Routes {
			if r.Type == gwconfig.RouteTypeGRPC {
				hasGrpcRoute = true
				break
			}
		}
		if !hasGrpcRoute {
			slog.Warn("grpc_addr is configured, but no gRPC routes are defined. gRPC server will not be started.")
			// Don't proceed with gRPC setup if no routes use it
		} else {
			// Only create handler, listener, server if GrpcAddr is set *and* routes exist
			grpcHandler := grpcgateway.NewGrpcHandler(logger, nc, cfg)

			grpcListener, err = net.Listen("tcp", cfg.GrpcAddr)
			if err != nil {
				slog.Error("Failed to listen on gRPC address", "addr", cfg.GrpcAddr, "error", err)
				os.Exit(1)
			}
			grpcServer = grpc.NewServer(
				grpc.UnknownServiceHandler(grpcHandler.HandleUnknownStream),
			)
			slog.Info("gRPC listener created", "addr", cfg.GrpcAddr)
			numServers++ // Increment server count only if gRPC will actually start
		}
	}
	//  End Conditional gRPC Setup

	//  Start Servers
	serverErrChan := make(chan error, numServers) // Size based on actual servers to start

	// Start HTTP Server
	go func() {
		slog.Info("Starting main HTTP server", "addr", cfg.ServerAddr)
		err := mainServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Main HTTP server ListenAndServe error", "error", err)
			serverErrChan <- fmt.Errorf("main server error: %w", err)
		} else {
			slog.Info("Main HTTP server shut down")
			serverErrChan <- nil // Signal clean shutdown or expected close
		}
	}()

	// Start Metrics Server
	go func() {
		slog.Info("Starting metrics server", "addr", cfg.MetricsAddr)
		err := metricsServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Metrics server ListenAndServe error", "error", err)
			serverErrChan <- fmt.Errorf("metrics server error: %w", err)
		} else {
			slog.Info("Metrics server shut down")
			serverErrChan <- nil // Signal clean shutdown or expected close
		}
	}()

	// Start gRPC Server
	if grpcServer != nil {
		go func() {
			slog.Info("Starting gRPC server", "addr", cfg.GrpcAddr)
			// Serve blocks, so this runs until server stops
			err := grpcServer.Serve(grpcListener)
			// ErrServerStopped is returned by Serve after Stop or GracefulStop.
			// Don't treat it as an application error.
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				slog.Error("gRPC server Serve error", "error", err)
				serverErrChan <- fmt.Errorf("gRPC server error: %w", err)
			} else {
				slog.Info("gRPC server shut down")
				serverErrChan <- nil // Signal clean shutdown or expected stop
			}
		}()
	}

	//  Wait for signal or server error
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErrChan:
		// If an error occurs during startup (e.g., port already bound), log it and exit.
		if err != nil {
			slog.Error("Server failed to start or unexpectedly stopped", "error", err)
			// Optionally attempt shutdown of other running servers before exiting?
			// For now, just exit. A more robust system might signal others.
			os.Exit(1) // Exit if a server fails critically on startup/runtime
		} else {
			// This path is less likely now with the exit above, but handles a server stopping cleanly unexpectedly.
			slog.Info("A server shut down cleanly (unexpectedly)")
		}
	case sig := <-quit:
		slog.Info("Shutdown signal received", "signal", sig.String())
	}

	//  Graceful Shutdown
	slog.Info("Initiating graceful shutdown...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	shutdownWg := &sync.WaitGroup{}
	// Use numServers determined earlier, which accounts for conditional gRPC server
	shutdownWg.Add(numServers)

	// Shutdown HTTP Server
	go func() {
		defer shutdownWg.Done()
		slog.Info("Shutting down main HTTP server...")
		if err := mainServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("Main HTTP server shutdown error", "error", err)
		} else {
			slog.Info("Main HTTP server gracefully stopped")
		}
	}()

	// Shutdown Metrics Server
	go func() {
		defer shutdownWg.Done()
		slog.Info("Shutting down metrics server...")
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("Metrics server shutdown error", "error", err)
		} else {
			slog.Info("Metrics server gracefully stopped")
		}
	}()

	// Shutdown gRPC Server ( *** Only if grpcServer was initialized *** )
	if grpcServer != nil { // <<< Same check needed for shutdown
		go func() {
			defer shutdownWg.Done()
			slog.Info("Shutting down gRPC server...")
			grpcServer.GracefulStop() // Blocks until complete
			slog.Info("gRPC server gracefully stopped")
		}()
	}

	// Wait for all shutdowns or timeout
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
		// Force stop gRPC if timeout occurs during graceful shutdown
		if grpcServer != nil {
			slog.Warn("Forcing gRPC server stop due to timeout...")
			grpcServer.Stop()
		}
	}

	// Drain remaining server errors (less critical after shutdown attempt)
	close(serverErrChan)
	// Keep track if any startup error happened even if shutdown was signaled
	var finalErr error
	for err := range serverErrChan {
		if err != nil {
			slog.Warn("Server reported error during run or shutdown process", "error", err)
			if finalErr == nil {
				finalErr = err // Keep first error encountered
			}
		}
	}

	slog.Info("Server exiting.")
	// Exit with non-zero status if a critical error occurred during runtime/startup
	if finalErr != nil {
		os.Exit(1)
	}
}
