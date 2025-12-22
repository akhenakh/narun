package handler

import (
	"bytes"
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
	"github.com/openpcc/bhttp"
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
		path := r.URL.Path
		method := r.Method
		subject := routeCfg.Service

		logger := h.Logger.With("path", path, "method", method, "nats_service", subject)

		// Method Check
		methodAllowed := false
		upperMethod := strings.ToUpper(method)
		for _, allowedMethod := range routeCfg.Methods {
			if upperMethod == allowedMethod {
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

		// BHTTP Request Encoding
		logger.Debug("Encoding request to BHTTP format")

		// BHTTP encoder expects a client-side request, where RequestURI is not set.
		// The incoming server-side request has it set, so we clear it.
		// The URL field is already parsed and is what the encoder will use.
		r.RequestURI = ""

		encoder := &bhttp.RequestEncoder{}
		bhttpMsg, err := encoder.EncodeRequest(r)
		if err != nil {
			logger.Error("Error encoding request to BHTTP", "error", err)
			http.Error(w, "Failed to encode request", http.StatusInternalServerError)
			metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
			metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
			return
		}

		bhttpPayload, err := io.ReadAll(bhttpMsg)
		if err != nil {
			logger.Error("Error reading encoded BHTTP message", "error", err)
			http.Error(w, "Failed to read encoded request", http.StatusInternalServerError)
			metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
			metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
			return
		}

		// Create NATS Request Message with BHTTP payload
		natsRequest := nats.NewMsg(subject)
		natsRequest.Data = bhttpPayload
		natsRequest.Header = make(nats.Header)
		natsRequest.Header.Set("Content-Type", "application/bhttp")

		logger.Debug("Sending NATS request", "subject", subject, "body_len", len(natsRequest.Data))

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
				statusCode = http.StatusServiceUnavailable
				respBody = "No backend service available"
				logger.Warn("NATS no responders", "subject", subject)
				natsStatus = metrics.StatusError
			} else {
				logger.Error("NATS request error", "subject", subject, "error", err)
			}

			metrics.NatsRequestsTotal.WithLabelValues(subject, natsStatus).Inc()
			http.Error(w, respBody, statusCode)
			metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
			metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
			return
		}

		// Process Successful NATS Reply
		logger.Debug("Received NATS reply", "reply_subject", natsReply.Subject, "body_len", len(natsReply.Data))
		metrics.NatsRequestsTotal.WithLabelValues(subject, metrics.StatusSuccess).Inc()

		// BHTTP Response Decoding
		logger.Debug("Decoding BHTTP response from NATS reply")
		decoder := &bhttp.ResponseDecoder{}
		bhttpResp, err := decoder.DecodeResponse(r.Context(), bytes.NewReader(natsReply.Data))
		if err != nil {
			logger.Error("Error decoding BHTTP response from NATS reply", "error", err)
			http.Error(w, "Failed to decode backend response", http.StatusBadGateway)
			metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusBadGateway)).Inc()
			metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
			return
		}
		defer bhttpResp.Body.Close()

		// Copy headers from decoded BHTTP response to HTTP response writer
		respHeader := w.Header()
		for key, values := range bhttpResp.Header {
			respHeader[key] = values
		}
		// Copy trailers if any
		for key, values := range bhttpResp.Trailer {
			respHeader[key] = values
		}

		// Write HTTP response status and body
		w.WriteHeader(bhttpResp.StatusCode)
		if bhttpResp.ContentLength != 0 {
			_, writeErr := io.Copy(w, bhttpResp.Body)
			if writeErr != nil {
				logger.Error("Error writing HTTP response body", "error", writeErr, "status", bhttpResp.StatusCode)
			}
		}

		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(bhttpResp.StatusCode)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
	}
}
