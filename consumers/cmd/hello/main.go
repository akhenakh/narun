package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/akhenakh/narun/nconsumer"
)

const (
	DefaultNatsURL    = "nats://localhost:4222"
	DefaultSubject    = "tasks.ORDERS.hello" // CHANGED: Match the config.yaml subject
	DefaultStreamName = "ORDERS"             // Must match the stream this subject belongs to
	DefaultQueueGroup = "hello-workers"      // Use a queue group for load balancing
)

// helloHandler implements the business logic as an http.Handler.
type helloHandler struct{}

func (h *helloHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Handler received request: Method=%s, Path=%s", r.Method, r.URL.Path)

	// --- The rest of your handler logic remains the same ---
	// --- Specific Logic for /hello/ POST ---
	if r.URL.Path == "/hello/" && r.Method == "POST" {
		// Expecting a JSON body like {"name": "myname"}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Error reading request body: %v", err)
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close() // Important to close the reconstructed request body

		var requestPayload struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(body, &requestPayload); err != nil {
			log.Printf("Error unmarshalling JSON body: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON payload"})
			return
		}

		// Process valid request
		log.Printf("Received name: %s", requestPayload.Name)
		// Simulate some work
		time.Sleep(50 * time.Millisecond)

		// Create success response payload
		replyPayload := map[string]string{
			"message":       fmt.Sprintf("Hello, %s!", requestPayload.Name),
			"received_path": r.URL.Path, // Access path from the request
		}

		// Send back a 200 OK with JSON body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(replyPayload); err != nil {
			// This write happens *after* WriteHeader, log error but can't change status
			log.Printf("Error encoding JSON response: %v", err)
		}
		return
	}

	// Handle other paths/methods this handler might receive (if subject covers more)
	log.Printf("Handler received unexpected request: Path=%s, Method=%s", r.URL.Path, r.Method)
	http.NotFound(w, r) // Or http.StatusNotImplemented, http.StatusMethodNotAllowed etc.
}

func main() {
	natsURL := flag.String("nats-url", DefaultNatsURL, "NATS server URL")
	subject := flag.String("subject", DefaultSubject, "NATS subject to subscribe to") // Default value is now correct
	streamName := flag.String("stream", DefaultStreamName, "NATS JetStream stream name")
	queueGroup := flag.String("queue", DefaultQueueGroup, "NATS queue group name")
	flag.Parse()

	// Create an instance of our handler
	handler := &helloHandler{}

	// Configure the NATS consumer listener options
	opts := nconsumer.Options{
		NatsURL:    *natsURL,
		Subject:    *subject, // Uses the (potentially overridden by flag) correct subject
		StreamName: *streamName,
		QueueGroup: *queueGroup,
		// AckWait: 30*time.Second // Example: Set custom AckWait
	}

	// Start the listener - this blocks until shutdown
	log.Printf("Starting NATS consumer for subject %s...", *subject)
	err := nconsumer.ListenAndServe(opts, handler)
	if err != nil {
		log.Fatalf("NATS consumer listener failed: %v", err)
	}

	// ListenAndServe handles graceful shutdown internally now
	log.Println("Consumer listener stopped.")
	os.Exit(0) // Ensure exit after ListenAndServe returns
}
