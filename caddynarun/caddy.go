package caddynarun

import (
	"errors" // Import errors package
	"fmt"
	"io"

	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/akhenakh/narun/internal/metrics"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap" // Caddy uses Zap logger
)

func init() {
	caddy.RegisterModule(&Handler{})
	httpcaddyfile.RegisterHandlerDirective("narun", parseCaddyfile)
}

// Handler implements the Caddy HTTP handler for narun integration using NATS Micro.
type Handler struct {
	// Configuration
	NatsURL        string         `json:"nats_url,omitempty"`
	RequestTimeout caddy.Duration `json:"request_timeout,omitempty"`
	Service        string         `json:"service,omitempty"` // ADDED: Target NATS service name

	// Internal state
	logger  *zap.Logger
	nc      *nats.Conn
	timeout time.Duration

	initOnce sync.Once // Ensures NATS connection happens only once per instance lifecycle
	initErr  error     // Stores error from initOnce execution
}

// CaddyModule returns the Caddy module information.
func (*Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.narun",
		New: func() caddy.Module { return new(Handler) },
	}
}

// Provision sets up the handler instance. Connects to NATS, validates config.
func (h *Handler) Provision(ctx caddy.Context) error {
	h.logger = ctx.Logger(h)

	// Validate and normalize config
	if h.NatsURL == "" {
		h.NatsURL = nats.DefaultURL
	}
	if h.RequestTimeout <= 0 {
		h.RequestTimeout = caddy.Duration(15 * time.Second)
	}
	h.timeout = time.Duration(h.RequestTimeout)

	// Validate Service Name
	if strings.TrimSpace(h.Service) == "" {
		return fmt.Errorf("the 'service' directive specifying the target NATS service name is required within the narun block")
	}
	if strings.ContainsAny(h.Service, "*> ") {
		return fmt.Errorf("invalid 'service' name '%s': contains invalid characters (*, >, space)", h.Service)
	}
	// Log the provisioned service
	h.logger.Debug("provisioned narun handler",
		zap.String("target_service", h.Service),
		zap.Duration("timeout", h.timeout),
		zap.String("nats_url", h.NatsURL),
	)

	// Connect to NATS (only once per instance)
	h.initOnce.Do(func() {
		h.logger.Info("Attempting to connect to NATS", zap.String("url", h.NatsURL))
		// Use standard NATS connection options if needed, similar to narun/cmd/narun/main.go
		conn, err := nats.Connect(h.NatsURL,
			nats.Name("Caddy Narun Plugin"),
			nats.Timeout(10*time.Second),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
			nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
				h.logger.Error("NATS client disconnected", zap.Error(err))
			}),
			nats.ReconnectHandler(func(nc *nats.Conn) {
				h.logger.Info("NATS client reconnected", zap.String("url", nc.ConnectedUrl()))
			}),
		)
		if err != nil {
			h.initErr = fmt.Errorf("failed to connect to NATS at %s: %w", h.NatsURL, err)
			h.logger.Error("NATS connection failed", zap.String("url", h.NatsURL), zap.Error(h.initErr))
			return
		}
		h.nc = conn
		h.logger.Info("Connected to NATS", zap.String("url", h.nc.ConnectedUrl()))
	})

	if h.initErr != nil {
		return h.initErr
	}
	if h.nc == nil || !h.nc.IsConnected() {
		return fmt.Errorf("NATS connection is not active after initialization attempt")
	}

	return nil
}

// Validate ensures the handler is correctly configured.
func (h *Handler) Validate() error {
	if strings.TrimSpace(h.Service) == "" {
		return fmt.Errorf("target NATS 'service' name is required")
	}
	// Note: NATS connection health is implicitly checked in ServeHTTP before use
	return nil
}

// Cleanup closes the NATS connection.
func (h *Handler) Cleanup() error {
	h.logger.Debug("Cleaning up narun handler")
	if h.nc != nil && h.nc.IsConnected() { // Check IsConnected before closing
		h.nc.Close()
		h.logger.Info("NATS connection closed")
	}
	return nil
}

// ServeHTTP handles the incoming HTTP request, routing it via NATS Micro.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	// Check NATS connection
	if h.nc == nil || !h.nc.IsConnected() {
		h.logger.Error("NATS connection not available for request handling",
			zap.String("path", r.URL.Path),
			zap.String("method", r.Method))
		return caddyhttp.Error(http.StatusServiceUnavailable, fmt.Errorf("NATS connection unavailable"))
	}

	// Directly use the configured Service name as the target NATS subject
	natsSubject := h.Service
	startTime := time.Now()

	h.logger.Debug("Matched Caddy route, proxying via NATS Micro",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),          // Log actual request path
		zap.String("target_service", h.Service), // Log configured service
		zap.String("nats_subject", natsSubject),
	)

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error("Error reading request body", zap.Error(err), zap.String("path", r.URL.Path))
		return caddyhttp.Error(http.StatusInternalServerError, fmt.Errorf("failed to read request body: %w", err))
	}
	defer r.Body.Close()

	// Prepare NATS Request Message with Headers
	natsRequest := nats.NewMsg(natsSubject)
	natsRequest.Data = body
	natsRequest.Header = make(nats.Header)

	// Copy original HTTP headers
	for key, values := range r.Header {
		for _, value := range values {
			natsRequest.Header.Add(key, value)
		}
	}
	// Add custom X-Original-* headers
	natsRequest.Header.Set("X-Original-Method", r.Method)
	natsRequest.Header.Set("X-Original-Path", r.URL.Path) // Pass the actual path
	natsRequest.Header.Set("X-Original-Query", r.URL.RawQuery)
	natsRequest.Header.Set("X-Original-Host", r.Host)
	natsRequest.Header.Set("X-Original-RemoteAddr", r.RemoteAddr)

	// Send NATS Request using RequestMsg
	natsReply, err := h.nc.RequestMsg(natsRequest, h.timeout)

	// Handle NATS Response/Error
	if err != nil {
		statusCode := http.StatusInternalServerError
		errMsg := "internal server error (NATS communication)"
		natsStatus := metrics.StatusError // For metrics

		if errors.Is(err, nats.ErrTimeout) {
			statusCode = http.StatusGatewayTimeout
			errMsg = "request timed out waiting for backend processor"
			natsStatus = metrics.StatusTimeout
			h.logger.Warn("NATS request timeout", zap.String("subject", natsSubject), zap.Duration("timeout", h.timeout))
		} else if errors.Is(err, nats.ErrNoResponders) {
			// Added check for NoResponders
			statusCode = http.StatusServiceUnavailable
			errMsg = "no backend service available"
			natsStatus = metrics.StatusError // Treat as error for gateway metric
			h.logger.Warn("NATS no responders", zap.String("subject", natsSubject))
		} else {
			natsStatus = metrics.StatusError
			h.logger.Error("NATS request error", zap.String("subject", natsSubject), zap.Error(err))
		}

		// Record NATS metric for the attempt
		metrics.NatsRequestsTotal.WithLabelValues(natsSubject, natsStatus).Inc()
		// Use Caddy's error handling
		return caddyhttp.Error(statusCode, fmt.Errorf(errMsg+": %w", err))
	}

	// Process Successful NATS Reply
	metrics.NatsRequestsTotal.WithLabelValues(natsSubject, metrics.StatusSuccess).Inc() // Record successful NATS request
	h.logger.Debug("Received NATS reply",
		zap.String("reply_subject", natsReply.Subject),
		zap.Int("reply_size", len(natsReply.Data)),
		zap.Any("reply_headers", natsReply.Header),
	)

	// Determine HTTP status code from response header
	statusCode := http.StatusOK
	statusStr := natsReply.Header.Get("X-Response-Status-Code")
	if statusStr != "" {
		if code, err := strconv.Atoi(statusStr); err == nil && code >= 100 && code < 600 {
			statusCode = code
		} else {
			h.logger.Warn("Invalid or missing X-Response-Status-Code header in NATS reply", zap.String("value", statusStr), zap.Error(err))
		}
	}

	// Copy headers from NATS reply to HTTP response writer
	respHeader := w.Header()
	for key, values := range natsReply.Header {
		if key == "X-Response-Status-Code" || key == "Nats-Service-Error-Code" || key == "Nats-Service-Error" {
			continue
		}
		respHeader[key] = values // Assign slice directly
	}

	// Write HTTP status and body
	w.WriteHeader(statusCode)
	if len(natsReply.Data) > 0 {
		_, writeErr := w.Write(natsReply.Data)
		if writeErr != nil {
			h.logger.Error("Error writing response body", zap.Error(writeErr), zap.Int("status", statusCode))
			// Status/headers already sent. Caddy might handle logging this.
			// return writeErr // Optionally return error to Caddy
		}
	}

	h.logger.Debug("Finished proxying request via NATS Micro",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.Int("status", statusCode),
		zap.Duration("duration", time.Since(startTime)),
	)

	return nil // Indicate success to Caddy
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // consume directive name ("narun")
	if d.NextArg() {
		// Allow optional argument after directive? E.g., `narun <service_name>`
		// For now, require options within block for consistency.
		return d.ArgErr()
	}

	for d.NextBlock(0) {
		option := d.Val()
		switch option {
		case "nats_url":
			if !d.AllArgs(&h.NatsURL) {
				return d.ArgErr()
			}
		case "request_timeout":
			var timeoutStr string
			if !d.AllArgs(&timeoutStr) {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(timeoutStr)
			if err != nil {
				return d.Errf("parsing request_timeout '%s': %v", timeoutStr, err)
			}
			h.RequestTimeout = caddy.Duration(dur)
		case "service":
			if !d.AllArgs(&h.Service) {
				return d.ArgErr()
			}
			// Basic validation within parsing
			if strings.TrimSpace(h.Service) == "" {
				return d.Err("service name cannot be empty")
			}
		default:
			return d.Errf("unrecognized narun option: %s", option)
		}
	}

	// Validation after parsing block
	if h.Service == "" {
		return d.Err("the 'service' directive is required within the narun block")
	}

	return nil
}

// parseCaddyfile unmarshals tokens from h into a new Middleware.
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var m Handler
	err := m.UnmarshalCaddyfile(h.Dispenser)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// Interface guards (remain the same)
var (
	_ caddy.Module                = (*Handler)(nil)
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
