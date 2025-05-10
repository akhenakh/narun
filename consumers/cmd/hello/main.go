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
	"strings"

	// "time" // No longer needed for default stream name constant

	"github.com/akhenakh/narun/nconsumer"
)

// Default values for flags
const (
	DefaultNatsURL = "nats://localhost:4222"
	// DefaultStreamName = "TASK" // REMOVED
)

// helloHandler implements the business logic as an http.Handler.
type helloHandler struct{}

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

func getHostname() string {
	host, err := os.Hostname()
	if err != nil {
		return "unknown-consumer"
	}
	return host
}

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

	// Logger setup
	logger := slog.New(logHandler).With("service", "hello-consumer") // Keep this logical name for logging
	slog.SetDefault(logger)

	//  Updated Flags
	natsURL := flag.String("nats-url", DefaultNatsURL, "NATS server URL")
	serviceName := flag.String("service", "hello", "NATS Micro service name to register/listen on")
	maxConcurrent := flag.Int("concurrency", runtime.NumCPU(), "Maximum number of concurrent requests")
	flag.Parse()

	// to validate secret system is working, let's read the ENV named secret
	logger.Debug("reading env SECRET value", "secret", os.Getenv("SECRET"))

	// to validate filesystem restriction using landlock try to read /etc/lsb-release
	b, err := os.ReadFile("/etc/lsb-release")
	if err != nil {
		logger.Error("failed to read /etc/lsb-release", "error", err)
	} else {
		logger.Debug("read /etc/lsb-release", "content", string(b))
	}

	// to validate filesystem restriction using landlock try to read /etc/lsb-release
	b, err = os.ReadFile("/etc/lsb-release")
	if err != nil {
		logger.Error("failed to read /etc/lsb-release", "error", err)
	} else {
		logger.Debug("read /etc/lsb-release", "content", string(b))
	}

	// to validate filesystem restriction using landlock try to read NARUN_INSTANCE_ROOT
	instanceRoot := os.Getenv("NARUN_INSTANCE_ROOT")
	if instanceRoot != "" {
		filePath := instanceRoot + "/mydata"
		b, err = os.ReadFile(filePath) // Reuse b and err variables declared above
		if err != nil {
			logger.Error("failed to read file in instance root", "path", filePath, "error", err)
		} else {
			logger.Debug("read file in instance root", "path", filePath, "content", string(b))
		}
	} else {
		logger.Warn("NARUN_INSTANCE_ROOT environment variable not set, skipping read test")
	}

	instanceRoot = os.Getenv("NARUN_INTERNAL_INSTANCE_ROOT")
	if instanceRoot != "" {
		filePath := instanceRoot + "/mydata"
		b, err = os.ReadFile(filePath) // Reuse b and err variables declared above
		if err != nil {
			logger.Error("failed to read file in instance root", "path", filePath, "error", err)
		} else {
			logger.Debug("read file in instance root", "path", filePath, "content", string(b))
		}
	} else {
		logger.Warn("NARUN_INERNAL_INSTANCE_ROOT environment variable not set, skipping read test")
	}

	// listing /
	files, err := ls()
	if err != nil {
		logger.Error("failed to read /", "error", err)
	} else {
		logger.Info("listed /", "files", files)
	}

	// Create handler
	handler := &helloHandler{}

	//  Configure options with ServiceName
	opts := nconsumer.Options{
		NATSURL:       *natsURL,
		ServiceName:   *serviceName,
		Logger:        logger,
		MaxConcurrent: *maxConcurrent,
		Description:   "Hello service example using NATS Micro",
		Version:       "1.0.0", // Keep versioning
	}

	// Start service
	logger.Info("Starting NATS Micro service",
		"service_name", opts.ServiceName, // Log service name
		"nats_url", opts.NATSURL)

	err = nconsumer.ListenAndServe(opts, handler)
	if err != nil {
		logger.Error("NATS Micro service failed", "error", err)
		os.Exit(1)
	}

	logger.Info("consumer listener stopped cleanly") // This might not be reached on error exit
}

func ls() (string, error) {
	// Path to list
	path := "/"

	// Open the directory
	dir, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer dir.Close()

	// Read directory entries
	entries, err := dir.ReadDir(-1) // -1 means read all entries
	if err != nil {
		return "", err
	}

	// Create a slice to store entry names
	fileNames := make([]string, 0, len(entries))

	// Add each entry name to the slice
	for _, entry := range entries {
		fileNames = append(fileNames, entry.Name())
	}

	// Join the names with commas to create a list string
	listString := "[" + strings.Join(fileNames, ", ") + "]"

	return listString, nil
}
