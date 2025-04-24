// narun/consumers/cmd/hello/main.go
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	// Use the config constants for consistency

	"github.com/akhenakh/narun/nconsumer"
)

const (
	// Default values for flags
	DefaultNatsURL    = "nats://localhost:4222"
	DefaultStreamName = "TASK" // Default stream name (should match gateway config)
)

// helloHandler implements the business logic as an http.Handler.
type helloHandler struct{}

// ServeHTTP remains unchanged
func (h *helloHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	slog.Info("handler received request", "method", r.Method, "path", r.URL.Path, "proto", r.Proto, "host", r.Host)

	if r.URL.Path == "/hello/" && (r.Method == "POST" || r.Method == "PUT") {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			slog.Error("error reading request body", "error", err)
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close() // Important to close the reconstructed request body

		var requestPayload struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(body, &requestPayload); err != nil {
			slog.Warn("error unmarshalling JSON body", "error", err, "body_snippet", string(body[:min(len(body), 100)])) // Log warning + snippet
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON payload"})
			return
		}

		slog.Info("processed valid request", "method", r.Method, "name", requestPayload.Name)
		time.Sleep(50 * time.Millisecond) // Simulate work

		replyPayload := map[string]string{
			"message":       fmt.Sprintf("Hello, %s! (processed by %s)", requestPayload.Name, r.Method),
			"received_path": r.URL.Path,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(replyPayload); err != nil {
			slog.Error("error encoding JSON response", "error", err)
		}
		return
	}

	slog.Warn("handler received unexpected request", "path", r.URL.Path, "method", r.Method)
	http.NotFound(w, r)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	// --- Flags remain the same ---
	natsURL := flag.String("nats-url", DefaultNatsURL, "NATS server URL")
	streamName := flag.String("stream", DefaultStreamName, "NATS JetStream stream name (must match gateway config)")
	appName := flag.String("app", "hello", "Application name for this consumer (used for subject derivation)")
	flag.Parse()

	handler := &helloHandler{}

	// --- Update Options Instantiation ---
	opts := nconsumer.Options{
		NatsURL:    *natsURL,
		AppName:    *appName,    // Pass the app name
		StreamName: *streamName, // Pass the stream name
		Logger:     logger,
		// AckWait: 30*time.Second // Example: Set custom AckWait if needed
	}

	// --- Update Starting Log Message ---
	// ListenAndServe will log the derived values it uses
	slog.Info("starting NATS consumer",
		"app", *appName,
		"stream", *streamName,
	)

	err := nconsumer.ListenAndServe(opts, handler)
	if err != nil {
		slog.Error("NATS consumer listener failed", "error", err)
		os.Exit(1)
	}

	slog.Info("consumer listener stopped")
}
