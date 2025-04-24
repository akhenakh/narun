package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/akhenakh/narun/nconsumer" // Import the updated nconsumer
)

const (
	// Default values for flags
	DefaultNatsURL    = "nats://localhost:4222"
	DefaultStreamName = "TASK" // Default stream name (must match gateway config)
)

// helloHandler implements the business logic as an http.Handler.
type helloHandler struct{}

// ServeHTTP handles the reconstructed HTTP request.
// No changes needed here as it works with standard http interfaces.
func (h *helloHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	slog.Debug("handler received request",
		"method", r.Method,
		"path", r.URL.Path,
		"proto", r.Proto,
		"host", r.Host,
		"remote_addr", r.RemoteAddr, // Log remote addr passed from gateway
		"headers", r.Header, // Log headers passed from gateway
	)

	// Check path and method
	if r.URL.Path == "/hello/" && (r.Method == http.MethodPost || r.Method == http.MethodPut) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("error reading request body", "error", err)
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close() // Important to close the reconstructed request body

		// Input validation (simple JSON check)
		var requestPayload struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(body, &requestPayload); err != nil || requestPayload.Name == "" {
			slog.Warn("error unmarshalling JSON body or missing name", "error", err, "body_snippet", string(body[:min(len(body), 100)]))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			// Provide a more informative error message
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON payload: 'name' field is required."})
			return
		}

		slog.Debug("processed valid request", "method", r.Method, "name", requestPayload.Name)

		replyPayload := map[string]string{
			"message":       fmt.Sprintf("Hello, %s! (processed by %s)", requestPayload.Name, r.Method),
			"received_path": r.URL.Path,
			"consumer_host": getHostname(), // Add some consumer-specific info
		}

		slog.Debug("json response", "payload", replyPayload)

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Narun-Processed-By", getHostname())
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(replyPayload); err != nil {
			// Log error, but response headers/status likely already sent
			slog.Error("error encoding JSON response", "error", err)
		}
		return
	}

	// Handle unexpected paths/methods routed to this consumer
	slog.Warn("handler received unexpected request", "path", r.URL.Path, "method", r.Method)
	http.NotFound(w, r)
}

// Simple helper to get hostname or default
func getHostname() string {
	host, err := os.Hostname()
	if err != nil {
		return "unknown-consumer"
	}
	return host
}

// Simple min function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	// Setup structured JSON logger for the consumer
	logLevel := slog.LevelInfo
	if levelStr := os.Getenv("LOG_LEVEL"); levelStr != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(levelStr)); err == nil {
			logLevel = level
		}
	}
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true,
	})

	logger := slog.New(logHandler).With("service", "hello-consumer") // Add service context
	slog.SetDefault(logger)

	//  Flags remain the same
	natsURL := flag.String("nats-url", DefaultNatsURL, "NATS server URL")
	streamName := flag.String("stream", DefaultStreamName, "NATS JetStream stream name (must match gateway config)")
	appName := flag.String("app", "hello", "Application name for this consumer (used for subject derivation)")
	batchSize := flag.Int("batch-size", 10, "Number of messages to fetch in a batch")
	maxConcurrent := flag.Int("concurrency", runtime.NumCPU(), "Maximum number of concurrent requests to process")
	pollInterval := flag.Duration("poll-interval", 100*time.Millisecond, "Interval between fetch attempts when no messages")
	flag.Parse()

	// Validate required flags
	if *appName == "" {
		slog.Error("'-app' flag is required")
		os.Exit(1)
	}
	if *streamName == "" {
		slog.Error("'-stream' flag is required")
		os.Exit(1)
	}

	handler := &helloHandler{}

	// Configure nconsumer Options
	opts := nconsumer.Options{
		NATSURL:       *natsURL,
		AppName:       *appName,
		StreamName:    *streamName,
		Logger:        logger,
		MaxConcurrent: *maxConcurrent,
		BatchSize:     *batchSize,
		PollInterval:  *pollInterval,

		// AckWait: 30*time.Second // Example: Set custom AckWait if needed
	}

	// Start the consumer listener
	// Start the consumer listener
	logger.Info("Starting NATS batch consumer",
		"app", opts.AppName,
		"stream", opts.StreamName,
		"nats_url", opts.NATSURL,
		"batch_size", opts.BatchSize,
		"max_concurrent", opts.MaxConcurrent,
		"poll_interval", opts.PollInterval,
	)

	// ListenAndServe now uses Protobuf internally
	err := nconsumer.ListenAndServe(opts, handler)
	if err != nil {
		logger.Error("NATS consumer listener failed", "error", err)
		os.Exit(1)
	}

	// This part might not be reached if ListenAndServe exits uncleanly
	logger.Info("consumer listener stopped cleanly")
}
