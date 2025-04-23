package handler

import (
	"io"
	"log"
	"net/http"
	"strconv" // For status code label
	"time"

	"github.com/akhenakh/narun/internal/config"
	"github.com/akhenakh/narun/internal/metrics"
	"github.com/akhenakh/narun/nconsumer"

	"github.com/fxamacker/cbor/v2" // CBOR encoding
	"github.com/nats-io/nats.go"
)

type HttpHandler struct {
	NatsConn *nats.Conn
	// JetStream nats.JetStreamContext // Not strictly needed here if using nc.Request
	Config *config.Config
}

func NewHttpHandler(nc *nats.Conn, cfg *config.Config) *HttpHandler {
	return &HttpHandler{
		NatsConn: nc,
		Config:   cfg,
	}
}

func (h *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	path := r.URL.Path
	method := r.Method

	// Find matching route config
	routeCfg, found := h.Config.FindRoute(path, method)
	if !found {
		log.Printf("No route found for %s %s", method, path)
		http.NotFound(w, r)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusNotFound)).Inc()
		// Don't record duration for 404s or record separately? Your choice.
		// metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return
	}

	// Read request body (limit size if necessary)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body for %s %s: %v", method, path, err)
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return
	}
	defer r.Body.Close()

	// Create normalized request
	normalizedReq := nconsumer.RequestData{
		Method:  method,
		Path:    path,
		Headers: r.Header,
		Query:   r.URL.RawQuery,
		Body:    body,
	}

	// Encode request using CBOR
	cborReqPayload, err := cbor.Marshal(normalizedReq)
	if err != nil {
		log.Printf("Error marshalling request to CBOR for %s %s: %v", method, path, err)
		http.Error(w, "Internal server error (encoding)", http.StatusInternalServerError)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return
	}

	log.Printf("Publishing NATS request to subject %s for %s %s", routeCfg.NatsSubject, method, path)

	// Publish request and wait for reply using NATS Request-Reply
	// nc.Request is simpler than js.Request and handles the reply inbox.
	natsMsg, err := h.NatsConn.Request(routeCfg.NatsSubject, cborReqPayload, h.Config.RequestTimeout)

	// Handle NATS response/error
	var statusCode int
	var natsStatus string

	if err != nil {
		if err == nats.ErrTimeout {
			log.Printf("NATS request timeout for subject %s (%s %s)", routeCfg.NatsSubject, method, path)
			http.Error(w, "Request timed out waiting for backend processor", http.StatusGatewayTimeout) // 504 is often better than 500 for timeouts
			statusCode = http.StatusGatewayTimeout
			natsStatus = metrics.StatusTimeout
		} else {
			log.Printf("NATS request error for subject %s (%s %s): %v", routeCfg.NatsSubject, method, path, err)
			http.Error(w, "Internal server error (NATS communication)", http.StatusInternalServerError)
			statusCode = http.StatusInternalServerError
			natsStatus = metrics.StatusError
		}
		// Increment counters
		metrics.NatsRequestsTotal.WithLabelValues(routeCfg.NatsSubject, natsStatus).Inc()
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return
	}

	// NATS request was successful (got a reply)
	metrics.NatsRequestsTotal.WithLabelValues(routeCfg.NatsSubject, metrics.StatusSuccess).Inc()
	log.Printf("Received NATS reply from subject %s for %s %s", routeCfg.NatsSubject, method, path)

	// Decode the CBOR response from the consumer
	var normalizedResp nconsumer.ResponseData
	err = cbor.Unmarshal(natsMsg.Data, &normalizedResp)
	if err != nil {
		log.Printf("Error unmarshalling CBOR response from NATS subject %s (%s %s): %v", routeCfg.NatsSubject, method, path, err)
		http.Error(w, "Internal server error (decoding response)", http.StatusInternalServerError)
		statusCode = http.StatusInternalServerError
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return
	}

	// Write response back to HTTP client
	if normalizedResp.Headers != nil {
		for key, values := range normalizedResp.Headers {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
	}

	// Ensure status code is valid, default to 200 if 0
	statusCode = normalizedResp.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}

	w.WriteHeader(statusCode)
	if normalizedResp.Body != nil {
		_, writeErr := w.Write(normalizedResp.Body)
		if writeErr != nil {
			// Client might have disconnected, log it but don't overwrite header
			log.Printf("Error writing response body for %s %s: %v", method, path, writeErr)
		}
	}

	// Record metrics for successful proxied request
	metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
	metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
}
