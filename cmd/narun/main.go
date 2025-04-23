package main

import (
	"context"
	"errors"
	"flag"
	"log"
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
	configFile := flag.String("config", "config.yaml", "Path to the configuration file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	nc, err := natsutil.ConnectNATS(cfg.NatsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	_, err = natsutil.SetupJetStream(nc, cfg.GetStreamNames())
	if err != nil {
		log.Fatalf("Failed to setup JetStream: %v", err)
	}

	httpHandler := handler.NewHttpHandler(nc, cfg)

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
		log.Printf("Starting main HTTP server on %s", cfg.ServerAddr)
		if err := mainServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Main HTTP server ListenAndServe error: %v", err)
		}
		log.Println("Main HTTP server shut down.")
	}()

	go func() {
		log.Printf("Starting metrics server on %s", cfg.MetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Metrics server ListenAndServe error: %v", err)
		}
		log.Println("Metrics server shut down.")
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutdown signal received, initiating graceful shutdown...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Adjust timeout as needed
	defer cancel()

	if err := mainServer.Shutdown(ctx); err != nil {
		log.Printf("Main HTTP server shutdown error: %v", err)
	} else {
		log.Println("Main HTTP server gracefully stopped.")
	}

	if err := metricsServer.Shutdown(ctx); err != nil {
		log.Printf("Metrics server shutdown error: %v", err)
	} else {
		log.Println("Metrics server gracefully stopped.")
	}

	log.Println("Server exiting.")
}
