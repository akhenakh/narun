package caddynarun

import (
	"bytes"
	"errors" // Import errors package
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/akhenakh/narun/internal/metrics"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/nats-io/nats.go"
	"github.com/openpcc/bhttp"
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
	NKeySeedFile   string         `json:"nkey_seed_file,omitempty"` // Optional NKey seed
	RequestTimeout caddy.Duration `json:"request_timeout,omitempty"`
	Service        string         `json:"service,omitempty"` // Target NATS service name

	// Internal state
	logger  *zap.Logger
	nc      *nats.Conn
	timeout time.Duration

	initOnce sync.Once // Ensures NATS connection happens only once per instance lifecycle
	initErr  error     // Stores error from initOnce execution if the *very first* connect attempt fails
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
	h.logger = ctx.Logger(h) // Caddy will provide an appropriately scoped logger

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

	h.logger.Debug("Provisioning narun handler",
		zap.String("service", h.Service),
		zap.Duration("timeout", h.timeout),
		zap.String("nats_url", h.NatsURL),
	)

	// Connect to NATS (only once per instance)
	h.initOnce.Do(func() {
		clientName := fmt.Sprintf("Caddy Narun Plugin (%s)", h.Service)
		// NATS client names have a max length, ensure we don't exceed it.
		// MaxClientNameLen is not exported directly, but typical limits are around 256.
		// Using a safe common value like 128 or checking nats.MaxClientNameLen if available (it's not exported).
		// For simplicity, we'll assume service names are reasonably short.
		// If you expect very long service names, add a truncation mechanism here.
		if len(clientName) > 128 { // Arbitrary safe limit if nats.MaxClientNameLen not usable
			clientName = clientName[:128]
		}

		natsOpts := []nats.Option{
			nats.Name(clientName),
			nats.Timeout(10 * time.Second),             // Timeout for the initial Connect() call itself
			nats.RetryOnFailedConnect(true),            // CRITICAL: Allows returning a conn for background retries
			nats.MaxReconnects(-1),                     // Retry forever
			nats.ReconnectWait(300 * time.Millisecond), // Time to wait between reconnect attempts
			nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
				logFields := []zap.Field{zap.String("service", h.Service)}
				if err != nil {
					logFields = append(logFields, zap.Error(err))
				}
				if nc != nil && nc.ConnectedUrl() != "" { // nc might be nil if error is very early
					logFields = append(logFields, zap.String("disconnected_from_url", nc.ConnectedUrl()))
				}
				h.logger.Error("NATS client disconnected", logFields...)
			}),
			nats.ReconnectHandler(func(nc *nats.Conn) {
				h.logger.Info("NATS client reconnected", zap.String("url", nc.ConnectedUrl()), zap.String("service", h.Service))
			}),
			nats.ClosedHandler(func(nc *nats.Conn) {
				h.logger.Info("NATS connection permanently closed", zap.String("service", h.Service))
			}),
			nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
				errMsg := "NATS async error"
				logFields := []zap.Field{zap.Error(err), zap.String("service", h.Service)}
				if sub != nil {
					errMsg = fmt.Sprintf("NATS async error for subscription '%s'", sub.Subject)
					logFields = append(logFields, zap.String("subscription_subject", sub.Subject))
				}
				if nc != nil {
					logFields = append(logFields, zap.String("client_status", nc.Status().String()))
				}
				h.logger.Error(errMsg, logFields...)
			}),
		}

		if h.NKeySeedFile != "" {
			opt, err := nats.NkeyOptionFromSeed(h.NKeySeedFile)
			if err != nil {
				h.logger.Error("Failed to load nkey seed", zap.String("file", h.NKeySeedFile), zap.Error(err))
				// We can't return error from initOnce, but connection will likely fail or we can set initErr
				h.initErr = fmt.Errorf("failed to load nkey seed: %w", err)
				return
			}
			natsOpts = append(natsOpts, opt)
		}

		h.logger.Info("Attempting initial NATS connection",
			zap.String("url", h.NatsURL),
			zap.String("service", h.Service),
			zap.String("nats_client_name", clientName),
		)

		// Ensure RetryOnFailedConnect is true, so nats.Connect returns a conn object
		// that will attempt to reconnect in the background even if the first attempt fails.
		conn, err := nats.Connect(h.NatsURL, natsOpts...)

		// 'conn' should be non-nil if RetryOnFailedConnect is true, even if 'err' is non-nil.
		// 'err' here signifies that the *initial* attempt within nats.Connect (respecting its Timeout) failed.
		// The 'conn' object will then handle background retries.
		h.nc = conn

		if err != nil {
			// This error means the first connect attempt (within the 10s nats.Timeout) failed.
			// The client (h.nc) is now trying to reconnect in the background.
			h.initErr = fmt.Errorf("initial NATS connection attempt failed for service '%s' at %s: %w. Client will retry in background", h.Service, h.NatsURL, err)
			h.logger.Warn("Initial NATS connection failed, client will attempt to reconnect in background",
				zap.String("url", h.NatsURL),
				zap.String("service", h.Service),
				zap.Error(h.initErr), // Log the specific error from the first attempt
			)
		} else {
			h.initErr = nil // Clear any previous error
			h.logger.Info("Initial NATS connection successful", zap.String("url", h.nc.ConnectedUrl()), zap.String("service", h.Service))
		}
	})

	// After initOnce.Do:
	// - h.nc is set (to a NATS conn object that might be disconnected but trying to reconnect).
	// - h.initErr might be set if the *very first* connection attempt failed.
	// We do *not* return h.initErr from Provision, as that would stop Caddy.
	// The ServeHTTP method will correctly handle the case where h.nc is not connected.

	if h.initErr != nil {
		// This is just an informational log at the Provision level now.
		h.logger.Warn("NATS handler for service provisioned, but NATS not yet connected. Will retry.",
			zap.String("service", h.Service),
			zap.String("initial_connect_error_summary", h.initErr.Error()),
		)
	}

	return nil
}

// Validate ensures the handler is correctly configured.
func (h *Handler) Validate() error {
	if strings.TrimSpace(h.Service) == "" {
		return fmt.Errorf("target NATS 'service' name is required")
	}
	// NATS connection health is not validated here; it's handled at runtime by ServeHTTP.
	return nil
}

// Cleanup closes the NATS connection.
func (h *Handler) Cleanup() error {
	h.logger.Debug("Cleaning up narun handler", zap.String("service", h.Service))
	if h.nc != nil { // Check if h.nc was initialized
		if !h.nc.IsClosed() { // Only close if not already closed
			h.nc.Close() // This is a permanent close. No more reconnects.
			h.logger.Info("NATS connection closed via Cleanup", zap.String("service", h.Service))
		} else {
			h.logger.Debug("NATS connection already closed prior to Cleanup", zap.String("service", h.Service))
		}
	}
	return nil
}

// ServeHTTP handles the incoming HTTP request, routing it via NATS Micro.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	if h.nc == nil {
		h.logger.Error("NATS connection object is nil, cannot handle request",
			zap.String("path", r.URL.Path),
			zap.String("method", r.Method),
			zap.String("service", h.Service))
		return caddyhttp.Error(http.StatusServiceUnavailable, fmt.Errorf("NATS service '%s' misconfigured or NATS client failed to initialize", h.Service))
	}

	if !h.nc.IsConnected() {
		statusStr := "unknown"
		if h.nc != nil {
			statusStr = h.nc.Status().String()
		}
		h.logger.Warn("NATS connection not currently active for request handling",
			zap.String("path", r.URL.Path),
			zap.String("method", r.Method),
			zap.String("service", h.Service),
			zap.String("nats_client_status", statusStr),
			zap.String("nats_server_url_last_connected", h.nc.ConnectedUrl()),
		)
		return caddyhttp.Error(http.StatusServiceUnavailable, fmt.Errorf("NATS service '%s' temporarily unavailable", h.Service))
	}

	natsSubject := h.Service
	startTime := time.Now()

	h.logger.Debug("Matched Caddy route, proxying via BHTTP over NATS Micro",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("target_service", h.Service),
		zap.String("nats_subject", natsSubject),
	)

	// BHTTP encoder expects a client-side request, where RequestURI is not set.
	// The incoming server-side request has it set, so we clear it.
	r.RequestURI = ""

	//  BHTTP Request Encoding
	encoder := &bhttp.RequestEncoder{}
	bhttpMsg, err := encoder.EncodeRequest(r)
	if err != nil {
		h.logger.Error("Error encoding request to BHTTP", zap.Error(err), zap.String("path", r.URL.Path), zap.String("service", h.Service))
		return caddyhttp.Error(http.StatusInternalServerError, fmt.Errorf("failed to encode request: %w", err))
	}

	bhttpPayload, err := io.ReadAll(bhttpMsg)
	if err != nil {
		h.logger.Error("Error reading encoded BHTTP message", zap.Error(err), zap.String("path", r.URL.Path), zap.String("service", h.Service))
		return caddyhttp.Error(http.StatusInternalServerError, fmt.Errorf("failed to read encoded request: %w", err))
	}

	// Prepare NATS Request Message with BHTTP payload
	natsRequest := nats.NewMsg(natsSubject)
	natsRequest.Data = bhttpPayload
	natsRequest.Header = make(nats.Header)
	natsRequest.Header.Set("Content-Type", "application/bhttp")

	// Send NATS Request using RequestMsg
	natsReply, err := h.nc.RequestMsg(natsRequest, h.timeout)

	// Handle NATS Response/Error
	if err != nil {
		statusCode := http.StatusInternalServerError
		errMsg := "internal server error (NATS communication)"
		natsStatus := metrics.StatusError

		if errors.Is(err, nats.ErrTimeout) {
			statusCode = http.StatusGatewayTimeout
			errMsg = "request timed out waiting for backend processor"
			natsStatus = metrics.StatusTimeout
			h.logger.Warn("NATS request timeout", zap.String("subject", natsSubject), zap.Duration("timeout", h.timeout), zap.String("service", h.Service))
		} else if errors.Is(err, nats.ErrNoResponders) {
			statusCode = http.StatusServiceUnavailable
			errMsg = "no backend service available"
			natsStatus = metrics.StatusError
			h.logger.Warn("NATS no responders", zap.String("subject", natsSubject), zap.String("service", h.Service))
		} else {
			natsStatus = metrics.StatusError
			h.logger.Error("NATS request error", zap.String("subject", natsSubject), zap.Error(err), zap.String("service", h.Service))
		}

		metrics.NatsRequestsTotal.WithLabelValues(natsSubject, natsStatus).Inc()
		return caddyhttp.Error(statusCode, fmt.Errorf(errMsg+": %w", err))
	}

	// Process Successful NATS Reply
	metrics.NatsRequestsTotal.WithLabelValues(natsSubject, metrics.StatusSuccess).Inc()
	h.logger.Debug("Received NATS reply",
		zap.String("reply_subject", natsReply.Subject),
		zap.Int("reply_size", len(natsReply.Data)),
		zap.String("service", h.Service),
	)

	//  BHTTP Response Decoding
	decoder := &bhttp.ResponseDecoder{}
	bhttpResp, err := decoder.DecodeResponse(r.Context(), bytes.NewReader(natsReply.Data))
	if err != nil {
		h.logger.Error("Error decoding BHTTP response from NATS reply", zap.Error(err), zap.String("service", h.Service))
		return caddyhttp.Error(http.StatusBadGateway, fmt.Errorf("failed to decode backend response: %w", err))
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

	// Write HTTP status and body
	w.WriteHeader(bhttpResp.StatusCode)
	if bhttpResp.ContentLength != 0 {
		_, writeErr := io.Copy(w, bhttpResp.Body)
		if writeErr != nil {
			h.logger.Error("Error writing response body", zap.Error(writeErr), zap.Int("status", bhttpResp.StatusCode), zap.String("service", h.Service))
		}
	}

	h.logger.Debug("Finished proxying request via BHTTP over NATS Micro",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.Int("status", bhttpResp.StatusCode),
		zap.Duration("duration", time.Since(startTime)),
		zap.String("service", h.Service),
	)

	return nil
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // consume directive name ("narun")
	if d.NextArg() {
		return d.ArgErr()
	}

	for d.NextBlock(0) {
		option := d.Val()
		switch option {
		case "nats_url":
			if !d.AllArgs(&h.NatsURL) {
				return d.ArgErr()
			}
		case "nkey_seed_file":
			if !d.AllArgs(&h.NKeySeedFile) {
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
			if strings.TrimSpace(h.Service) == "" {
				return d.Err("service name cannot be empty")
			}
		default:
			return d.Errf("unrecognized narun option: %s", option)
		}
	}

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

// Interface guards
var (
	_ caddy.Module                = (*Handler)(nil)
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
