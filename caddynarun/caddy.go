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
	Routes         []RouteConfig  `json:"routes,omitempty"`

	// Internal state
	logger   *zap.Logger
	nc       *nats.Conn
	timeout  time.Duration
	routeMap map[string]map[string]*RouteConfig // map[path]map[METHOD]*RouteConfig
	routesMu sync.RWMutex

	initOnce sync.Once // Ensures NATS connection happens only once per instance lifecycle
	initErr  error     // Stores error from initOnce execution
}

// RouteConfig defines a single routing rule from HTTP to NATS Micro service.
type RouteConfig struct {
	Path    string   `json:"path"`
	Methods []string `json:"methods"` // Should be uppercase
	Service string   `json:"service"` // NATS Micro service name (REQUIRED)
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
	h.logger = ctx.Logger(h) // Use Caddy's logger directly
	h.routesMu.Lock()
	defer h.routesMu.Unlock()

	// Validate and normalize config
	if h.NatsURL == "" {
		h.NatsURL = nats.DefaultURL
	}
	if h.RequestTimeout <= 0 {
		h.RequestTimeout = caddy.Duration(15 * time.Second) // Default timeout
	}
	h.timeout = time.Duration(h.RequestTimeout)

	if len(h.Routes) == 0 {
		return fmt.Errorf("at least one 'route' must be configured within the narun block")
	}

	// Build route map
	h.routeMap = make(map[string]map[string]*RouteConfig)
	for i := range h.Routes {
		route := &h.Routes[i] // Get pointer to the route config

		if strings.TrimSpace(route.Path) == "" {
			return fmt.Errorf("invalid route config at index %d: path cannot be empty", i)
		}
		if len(route.Methods) == 0 {
			return fmt.Errorf("invalid route config at index %d for path '%s': methods list cannot be empty", i, route.Path)
		}
		//  Validate Service Name
		if strings.TrimSpace(route.Service) == "" {
			return fmt.Errorf("invalid route config at index %d for path '%s': service name cannot be empty", i, route.Path)
		}
		if strings.ContainsAny(route.Service, "*> ") {
			return fmt.Errorf("invalid service name '%s' for path '%s': contains invalid characters (*, >, space)", route.Service, route.Path)
		}

		if _, ok := h.routeMap[route.Path]; !ok {
			h.routeMap[route.Path] = make(map[string]*RouteConfig)
		}

		// Normalize methods to uppercase and store
		normalizedMethods := make([]string, 0, len(route.Methods))
		for _, method := range route.Methods {
			upperMethod := strings.ToUpper(strings.TrimSpace(method))
			if upperMethod == "" {
				return fmt.Errorf("invalid empty method in route config at index %d for path '%s'", i, route.Path)
			}
			if _, ok := h.routeMap[route.Path][upperMethod]; ok {
				return fmt.Errorf("duplicate route defined for path '%s' and method '%s'", route.Path, upperMethod)
			}
			h.routeMap[route.Path][upperMethod] = route
			normalizedMethods = append(normalizedMethods, upperMethod)
			h.logger.Debug("provisioned route",
				zap.String("path", route.Path),
				zap.String("method", upperMethod),
				zap.String("service", route.Service), // Log service name
			)
		}
		route.Methods = normalizedMethods // Update route methods to normalized ones
	}

	// Connect to NATS (only once per instance) - Simplified, no JetStream needed
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
	h.routesMu.RLock()
	defer h.routesMu.RUnlock()

	if len(h.routeMap) == 0 && len(h.Routes) > 0 {
		return fmt.Errorf("routes configured but internal route map not built (provisioning error or invalid routes?)")
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
		// Use Caddy's standard error handling if possible, or write directly
		return caddyhttp.Error(http.StatusServiceUnavailable, fmt.Errorf("NATS connection unavailable"))
		// Or:
		// w.WriteHeader(http.StatusServiceUnavailable)
		// fmt.Fprint(w, "NATS connection unavailable")
		// return nil // If returning nil prevents further Caddy processing of the error
	}

	path := r.URL.Path
	method := r.Method

	// Route lookup
	h.routesMu.RLock()
	methodsForPath, pathFound := h.routeMap[path]
	var routeCfg *RouteConfig
	if pathFound {
		routeCfg = methodsForPath[strings.ToUpper(method)]
	}
	h.routesMu.RUnlock()

	if routeCfg == nil {
		// Pass to next handler if no route matches
		h.logger.Debug("Request did not match any narun routes, passing to next handler",
			zap.String("method", method),
			zap.String("path", path),
		)
		return next.ServeHTTP(w, r)
	}

	// Route Found - Handle via NATS Micro
	natsSubject := routeCfg.Service // Target subject is the service name
	startTime := time.Now()

	h.logger.Debug("Matched route, proxying via NATS Micro",
		zap.String("method", method),
		zap.String("path", path),
		zap.String("target_service", routeCfg.Service),
		zap.String("nats_subject", natsSubject),
	)

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error("Error reading request body", zap.Error(err), zap.String("path", path))
		return caddyhttp.Error(http.StatusInternalServerError, fmt.Errorf("failed to read request body: %w", err))
	}
	defer r.Body.Close()

	//  Prepare NATS Request Message with Headers
	natsRequest := nats.NewMsg(natsSubject)
	natsRequest.Data = body
	natsRequest.Header = make(nats.Header) // Use nats.Header

	// Copy original HTTP headers
	for key, values := range r.Header {
		for _, value := range values {
			natsRequest.Header.Add(key, value)
		}
	}
	// Add custom X-Original-* headers
	natsRequest.Header.Set("X-Original-Method", method)
	natsRequest.Header.Set("X-Original-Path", path)
	natsRequest.Header.Set("X-Original-Query", r.URL.RawQuery)
	natsRequest.Header.Set("X-Original-Host", r.Host)
	natsRequest.Header.Set("X-Original-RemoteAddr", r.RemoteAddr)

	//  Send NATS Request using RequestMsg
	natsReply, err := h.nc.RequestMsg(natsRequest, h.timeout)

	// Handle NATS Response/Error
	if err != nil {
		statusCode := http.StatusInternalServerError
		errMsg := "internal server error (NATS communication)"
		if errors.Is(err, nats.ErrTimeout) {
			statusCode = http.StatusGatewayTimeout
			errMsg = "request timed out waiting for backend processor"
			h.logger.Warn("NATS request timeout", zap.String("subject", natsSubject), zap.Duration("timeout", h.timeout))
		} else {
			h.logger.Error("NATS request error", zap.String("subject", natsSubject), zap.Error(err))
			// Consider logging more details about the error if it's not timeout
		}
		// Use Caddy's error handling
		return caddyhttp.Error(statusCode, fmt.Errorf(errMsg+": %w", err))
	}

	//  Process Successful NATS Reply
	h.logger.Debug("Received NATS reply",
		zap.String("reply_subject", natsReply.Subject), // This is the inbox reply subject
		zap.Int("reply_size", len(natsReply.Data)),
		zap.Any("reply_headers", natsReply.Header), // Log received headers
	)

	// Determine HTTP status code from response header
	statusCode := http.StatusOK // Default
	statusStr := natsReply.Header.Get("X-Response-Status-Code")
	if statusStr != "" {
		if code, err := strconv.Atoi(statusStr); err == nil && code >= 100 && code < 600 {
			statusCode = code
		} else {
			h.logger.Warn("Invalid or missing X-Response-Status-Code header in NATS reply", zap.String("value", statusStr), zap.Error(err))
		}
	}

	// Copy headers from NATS reply to HTTP response writer
	respHeader := w.Header() // Get the ResponseWriter's header map
	for key, values := range natsReply.Header {
		// Skip internal/NATS headers
		if key == "X-Response-Status-Code" || key == "Nats-Service-Error-Code" || key == "Nats-Service-Error" {
			continue
		}
		// Assign the slice directly
		respHeader[key] = values
	}

	// Write HTTP status and body
	w.WriteHeader(statusCode)
	if len(natsReply.Data) > 0 {
		_, writeErr := w.Write(natsReply.Data)
		if writeErr != nil {
			// Log error, but status/headers likely sent. Caddy might handle this further.
			h.logger.Error("Error writing response body", zap.Error(writeErr), zap.Int("status", statusCode))
			// Optional: Return the error so Caddy knows writing failed
			// return writeErr
		}
	}

	h.logger.Debug("Finished proxying request via NATS Micro",
		zap.String("method", method),
		zap.String("path", path),
		zap.Int("status", statusCode),
		zap.Duration("duration", time.Since(startTime)),
	)

	return nil // Indicate success to Caddy
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // consume directive name ("narun")
	if d.NextArg() {
		return d.ArgErr()
	}

	h.Routes = []RouteConfig{} // Initialize routes for this block

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
		case "route":
			// Args: path service_name [METHOD...]
			args := d.RemainingArgs()
			if len(args) < 2 {
				return d.Errf("route requires at least path and service name: route <path> <service_name> [METHOD...]")
			}
			rc := RouteConfig{
				Path:    args[0],
				Service: args[1], // Use args[1] for service name
			}
			if len(args) > 2 {
				rc.Methods = args[2:] // Methods will be normalized (uppercased) in Provision
			} else {
				// Default to GET and POST if no methods specified? Or require explicit?
				// Let's require explicit methods for clarity. Provision will check len > 0.
				// rc.Methods = []string{"GET", "POST"} // Example default
				return d.Errf("route for path '%s' requires at least one HTTP method", rc.Path)
			}
			h.Routes = append(h.Routes, rc)
		default:
			return d.Errf("unrecognized narun option: %s", option)
		}
	}

	if len(h.Routes) == 0 {
		return d.Err("at least one 'route' directive is required within the narun block")
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
