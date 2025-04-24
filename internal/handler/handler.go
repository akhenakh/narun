package handler

import (
	"io"
	"log/slog"
	"net/http"
	"strconv" // For status code label
	"time"

	"github.com/akhenakh/narun/internal/config"
	"github.com/akhenakh/narun/internal/metrics"
	"github.com/akhenakh/narun/nconsumer" // Use pooling functions

	"github.com/fxamacker/cbor/v2" // CBOR encoding
	"github.com/nats-io/nats.go"
)

type HttpHandler struct {
	NatsConn *nats.Conn
	Config   *config.Config
	Logger   *slog.Logger
}

func NewHttpHandler(logger *slog.Logger, nc *nats.Conn, cfg *config.Config) *HttpHandler {
	return &HttpHandler{
		NatsConn: nc,
		Config:   cfg,
		Logger:   logger,
	}
}

func (h *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	path := r.URL.Path
	method := r.Method // Original method case might be needed for some checks, but lookup uses Upper

	// Find matching route config using UPPERCASE method
	routeCfg, found := h.Config.FindRoute(path, method)
	if !found {
		h.Logger.Info("No route found", "method", method, "path", path)
		http.NotFound(w, r)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusNotFound)).Inc()
		// No duration metric needed for 404 in this setup, could be added if desired
		return
	}

	// Derive the NATS subject for this route
	natsSubject := h.Config.GetNatsSubject(routeCfg)
	if natsSubject == "" { // Should not happen if config loaded correctly
		h.Logger.Error("Could not derive NATS subject", "method", method, "path", path, "app", routeCfg.App)
		http.Error(w, "Internal server error (configuration)", http.StatusInternalServerError)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
		return
	}

	// Read request body (limit size if necessary)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.Logger.Error("Error reading request body", "method", method, "path", path, "nats_subject", natsSubject, "error", err)
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return
	}
	defer r.Body.Close()

	// --- Use Pool for RequestData ---
	normalizedReq := nconsumer.GetRequestData()
	defer nconsumer.PutRequestData(normalizedReq) // Put it back when ServeHTTP returns

	// Populate the pooled request object
	normalizedReq.Method = method // Keep original case for the consumer
	normalizedReq.Path = path
	for k, v := range r.Header {
		vc := make([]string, len(v))
		copy(vc, v)
		normalizedReq.Headers[k] = vc
	}
	normalizedReq.Query = r.URL.RawQuery
	normalizedReq.Body = body
	normalizedReq.Proto = r.Proto
	normalizedReq.Host = r.Host
	normalizedReq.RequestURI = r.RequestURI
	normalizedReq.RemoteAddr = r.RemoteAddr
	// --- End Pool Usage ---

	// Encode request using CBOR
	cborReqPayload, err := cbor.Marshal(normalizedReq)
	if err != nil {
		h.Logger.Error("Error marshalling request to CBOR", "method", method, "path", path, "nats_subject", natsSubject, "error", err)
		http.Error(w, "Internal server error (encoding)", http.StatusInternalServerError)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return // defer will still call PutRequestData
	}

	h.Logger.Debug("Publishing NATS request", "subject", natsSubject, "method", method, "path", path)

	// Publish request and wait for reply using NATS Request-Reply
	natsMsg, err := h.NatsConn.Request(natsSubject, cborReqPayload, h.Config.RequestTimeout)

	// Handle NATS response/error
	var statusCode int
	var natsStatus string

	if err != nil {
		if err == nats.ErrTimeout {
			h.Logger.Warn("NATS request timeout", "subject", natsSubject, "method", method, "path", path)
			http.Error(w, "Request timed out waiting for backend processor", http.StatusGatewayTimeout)
			statusCode = http.StatusGatewayTimeout
			natsStatus = metrics.StatusTimeout
		} else {
			h.Logger.Error("NATS request error", "subject", natsSubject, "method", method, "path", path, "error", err)
			http.Error(w, "Internal server error (NATS communication)", http.StatusInternalServerError)
			statusCode = http.StatusInternalServerError
			natsStatus = metrics.StatusError
		}
		// Use derived natsSubject for the metric label
		metrics.NatsRequestsTotal.WithLabelValues(natsSubject, natsStatus).Inc()
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return // defer will still call PutRequestData
	}

	// NATS request was successful (got a reply)
	// Use derived natsSubject for the metric label
	metrics.NatsRequestsTotal.WithLabelValues(natsSubject, metrics.StatusSuccess).Inc()
	h.Logger.Debug("Received NATS reply", "subject", natsSubject, "method", method, "path", path)

	// --- Use Pool for ResponseData ---
	normalizedResp := nconsumer.GetResponseData()
	defer nconsumer.PutResponseData(normalizedResp) // Put it back when ServeHTTP returns
	// --- End Pool Usage ---

	err = cbor.Unmarshal(natsMsg.Data, &normalizedResp)
	if err != nil {
		h.Logger.Error("Error unmarshalling CBOR response", "subject", natsSubject, "method", method, "path", path, "error", err)
		http.Error(w, "Internal server error (decoding response)", http.StatusInternalServerError)
		statusCode = http.StatusInternalServerError
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return // defer will call PutRequestData and PutResponseData
	}

	// Write response back to HTTP client
	if normalizedResp.Headers != nil {
		for key, values := range normalizedResp.Headers {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
	}

	statusCode = normalizedResp.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusOK // Default if consumer didn't set it
	}

	w.WriteHeader(statusCode)
	if normalizedResp.Body != nil {
		_, writeErr := w.Write(normalizedResp.Body)
		if writeErr != nil {
			// Log error, but headers and status are already sent
			h.Logger.Error("Error writing response body", "method", method, "path", path, "nats_subject", natsSubject, "error", writeErr)
		}
	}

	// Record metrics for successful proxied request
	metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
	metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
	// Defers handle putting objects back in pools
}
