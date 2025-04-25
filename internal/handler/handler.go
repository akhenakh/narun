package handler

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/akhenakh/narun/internal/config"
	"github.com/akhenakh/narun/internal/metrics"
	"github.com/nats-io/nats.go"
	// No micro import needed here for the client side
)

// Helper to check NATS errors - needed for errors.Is
var _ error = nats.ErrTimeout // Ensure we have the nats error variables available

type HttpHandler struct {
	NatsConn *nats.Conn // Use the standard NATS connection
	Config   *config.Config
	Logger   *slog.Logger
	// Remove serviceClient, it's not part of the standard micro client pattern
}

func NewHttpHandler(logger *slog.Logger, nc *nats.Conn, cfg *config.Config) *HttpHandler {
	// No need to create a micro.ServiceClient
	return &HttpHandler{
		NatsConn: nc,
		Config:   cfg,
		Logger:   logger,
	}
}

func (h *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	path := r.URL.Path
	method := r.Method

	routeCfg, found := h.Config.FindRoute(path, method)
	if !found {
		h.Logger.Info("No route found", "method", method, "path", path)
		http.NotFound(w, r)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusNotFound)).Inc()
		// Observe duration even for not found
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.Logger.Error("Error reading request body", "error", err)
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return
	}
	defer r.Body.Close()

	// --- Create NATS Request Message with Headers ---
	// Use GetMicroSubject which handles the service name logic from config
	subject := h.Config.GetMicroSubject(routeCfg)
	natsRequest := nats.NewMsg(subject)
	natsRequest.Data = body
	natsRequest.Header = make(nats.Header) // Use nats.Header (which is http.Header)

	// 1. Copy original HTTP headers to NATS headers
	for key, values := range r.Header {
		for _, value := range values {
			natsRequest.Header.Add(key, value)
		}
	}

	// 2. Add custom headers needed for reconstruction on the consumer side
	natsRequest.Header.Set("X-Original-Method", method)
	natsRequest.Header.Set("X-Original-Path", path)
	natsRequest.Header.Set("X-Original-Query", r.URL.RawQuery)
	natsRequest.Header.Set("X-Original-Host", r.Host)
	natsRequest.Header.Set("X-Original-RemoteAddr", r.RemoteAddr)
	// Add any other necessary context if needed

	h.Logger.Debug("Sending NATS request",
		"subject", natsRequest.Subject,
		"headers", natsRequest.Header, // Log headers being sent
		"body_len", len(natsRequest.Data))

	// --- Send request using standard NATS RequestMsg ---
	natsReply, err := h.NatsConn.RequestMsg(natsRequest, h.Config.RequestTimeout)

	// --- Handle NATS Response/Error ---
	if err != nil {
		statusCode := http.StatusInternalServerError
		respBody := "Internal server error (NATS communication)"
		natsStatus := metrics.StatusError // Default status for metrics

		if errors.Is(err, nats.ErrTimeout) { // Use errors.Is for checking specific NATS errors
			statusCode = http.StatusGatewayTimeout
			respBody = "Request timed out waiting for backend processor"
			h.Logger.Warn("NATS request timeout", "subject", subject, "timeout", h.Config.RequestTimeout)
			natsStatus = metrics.StatusTimeout
		} else {
			h.Logger.Error("NATS request error", "subject", subject, "error", err)
			// Keep natsStatus as metrics.StatusError
		}

		// Record NATS request metric
		metrics.NatsRequestsTotal.WithLabelValues(subject, natsStatus).Inc()

		// Send HTTP error response
		w.WriteHeader(statusCode)
		fmt.Fprint(w, respBody)

		// Record HTTP metrics for error cases
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return
	}

	// --- Process Successful NATS Reply ---
	h.Logger.Debug("Received NATS reply",
		"subject", natsReply.Subject, // Note: This is the reply subject, not the original request subject
		"headers", natsReply.Header,
		"body_len", len(natsReply.Data))

	// Record successful NATS request metric
	metrics.NatsRequestsTotal.WithLabelValues(subject, metrics.StatusSuccess).Inc()

	// Determine HTTP status code from response header
	statusCode := http.StatusOK // Default
	statusStr := natsReply.Header.Get("X-Response-Status-Code")
	if statusStr != "" {
		if code, err := strconv.Atoi(statusStr); err == nil && code >= 100 && code < 600 {
			statusCode = code
		} else {
			h.Logger.Warn("Invalid or missing X-Response-Status-Code header in NATS reply", "value", statusStr, "error", err)
			// Keep default OK or consider setting an internal server error? Sticking with OK is likely safer.
		}
	}

	// Copy headers from NATS reply header to HTTP response writer
	for key, values := range natsReply.Header {
		// Skip the internal status code header we just processed
		if key == "X-Response-Status-Code" || key == "Nats-Service-Error-Code" || key == "Nats-Service-Error" { // Also skip standard NATS error headers
			continue
		}
		// Skip other potential internal headers if needed (e.g., starting with "Nats-")
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Write HTTP response status and body
	w.WriteHeader(statusCode)
	if len(natsReply.Data) > 0 {
		_, writeErr := w.Write(natsReply.Data)
		if writeErr != nil {
			// Log error, but headers and status are already sent
			h.Logger.Error("Error writing HTTP response body", "error", writeErr, "status", statusCode)
		}
	}

	// Record HTTP metrics for successful case
	metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
	metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
}
