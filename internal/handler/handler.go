package handler

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/akhenakh/narun/internal/gwconfig"
	"github.com/akhenakh/narun/internal/metrics"
	"github.com/nats-io/nats.go"
)

// Helper to check NATS errors - needed for errors.Is
var _ error = nats.ErrTimeout // Ensure we have the nats error variables available

type HttpHandler struct {
	NatsConn    *nats.Conn // Use the standard NATS connection
	Config      *gwconfig.Config
	Logger      *slog.Logger
	internalMux *http.ServeMux
}

func NewHttpHandler(logger *slog.Logger, nc *nats.Conn, cfg *gwconfig.Config) *HttpHandler {
	handler := &HttpHandler{
		NatsConn:    nc,
		Config:      cfg,
		Logger:      logger,
		internalMux: http.NewServeMux(), // Initialize the internal mux
	}

	// Register routes with the internal mux
	for i := range cfg.Routes {
		// Need to capture the loop variable correctly for the closure
		route := cfg.Routes[i]
		if route.Type == gwconfig.RouteTypeHTTP {
			// http.ServeMux handles prefix matching automatically if path ends with "/"
			pattern := route.Path // Use the path directly from config
			logger.Debug("Registering HTTP route with internal mux", "pattern", pattern, "nats_service", route.Service)
			handler.internalMux.HandleFunc(pattern, handler.makeRouteHandler(&route)) // Pass pointer to route
		}
	}

	return handler
}

// ServeHTTP now simply delegates to the internal mux.
func (h *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.internalMux.ServeHTTP(w, r)
}

// makeRouteHandler creates an http.HandlerFunc for a specific route.
// This closure captures the specific route configuration.
func (h *HttpHandler) makeRouteHandler(routeCfg *gwconfig.RouteConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		path := r.URL.Path // Path matched by ServeMux
		method := r.Method

		logger := h.Logger.With("path", path, "method", method, "nats_service", routeCfg.Service)

		// Method Check (still useful even after mux routing)
		methodAllowed := false
		upperMethod := strings.ToUpper(method)
		for _, allowedMethod := range routeCfg.Methods {
			if upperMethod == allowedMethod { // Methods in config are already uppercase
				methodAllowed = true
				break
			}
		}
		if !methodAllowed {
			logger.Warn("Method not allowed for matched route")
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusMethodNotAllowed)).Inc()
			metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
			return
		}

		// --- NATS Request Logic (Moved from original ServeHTTP) ---

		// Read request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Error("Error reading request body", "error", err)
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
			metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
			return
		}
		defer r.Body.Close()

		// Create NATS Request Message with Headers
		subject := routeCfg.Service // Directly use the service from the captured route config
		natsRequest := nats.NewMsg(subject)
		natsRequest.Data = body
		natsRequest.Header = make(nats.Header)

		// Copy original HTTP headers to NATS headers
		for key, values := range r.Header {
			for _, value := range values {
				natsRequest.Header.Add(key, value)
			}
		}

		// Add custom headers needed for reconstruction on the consumer side
		natsRequest.Header.Set("X-Original-Method", method)
		natsRequest.Header.Set("X-Original-Path", path)            // Use the actual request path
		natsRequest.Header.Set("X-Original-Query", r.URL.RawQuery) // Pass query params
		natsRequest.Header.Set("X-Original-Host", r.Host)
		natsRequest.Header.Set("X-Original-RemoteAddr", r.RemoteAddr)

		logger.Debug("Sending NATS request",
			"subject", natsRequest.Subject,
			"headers", natsRequest.Header,
			"body_len", len(natsRequest.Data))

		// Send request using standard NATS RequestMsg
		natsReply, err := h.NatsConn.RequestMsg(natsRequest, h.Config.RequestTimeout)

		// Handle NATS Response/Error
		if err != nil {
			statusCode := http.StatusInternalServerError
			respBody := "Internal server error (NATS communication)"
			natsStatus := metrics.StatusError

			if errors.Is(err, nats.ErrTimeout) {
				statusCode = http.StatusGatewayTimeout
				respBody = "Request timed out waiting for backend processor"
				logger.Warn("NATS request timeout", "subject", subject, "timeout", h.Config.RequestTimeout)
				natsStatus = metrics.StatusTimeout
			} else if errors.Is(err, nats.ErrNoResponders) {
				// Added check for NoResponders
				statusCode = http.StatusServiceUnavailable
				respBody = "No backend service available"
				logger.Warn("NATS no responders", "subject", subject)
				natsStatus = metrics.StatusError // Treat as error for gateway metric
			} else {
				logger.Error("NATS request error", "subject", subject, "error", err)
			}

			metrics.NatsRequestsTotal.WithLabelValues(subject, natsStatus).Inc()
			http.Error(w, respBody, statusCode) // Use http.Error for simplicity
			metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
			metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
			return
		}

		// Process Successful NATS Reply
		logger.Debug("Received NATS reply",
			"subject", natsReply.Subject,
			"headers", natsReply.Header,
			"body_len", len(natsReply.Data))

		metrics.NatsRequestsTotal.WithLabelValues(subject, metrics.StatusSuccess).Inc()

		// Determine HTTP status code from response header
		statusCode := http.StatusOK
		statusStr := natsReply.Header.Get("X-Response-Status-Code")
		if statusStr != "" {
			if code, err := strconv.Atoi(statusStr); err == nil && code >= 100 && code < 600 {
				statusCode = code
			} else {
				logger.Warn("Invalid or missing X-Response-Status-Code header in NATS reply", "value", statusStr, "error", err)
			}
		}

		// Copy headers from NATS reply header to HTTP response writer
		respHeader := w.Header() // Get header map before writing status
		for key, values := range natsReply.Header {
			if key == "X-Response-Status-Code" || key == "Nats-Service-Error-Code" || key == "Nats-Service-Error" {
				continue
			}
			// Assign the slice directly is more efficient for http.Header
			respHeader[key] = values
		}

		// Write HTTP response status and body
		w.WriteHeader(statusCode)
		if len(natsReply.Data) > 0 {
			_, writeErr := w.Write(natsReply.Data)
			if writeErr != nil {
				logger.Error("Error writing HTTP response body", "error", writeErr, "status", statusCode)
				// Cannot change status code now, but log it
			}
		}

		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
	}
}
