package caddynarun

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/akhenakh/narun/internal/natsutil" // Use natsutil for JetStream setup
	"github.com/akhenakh/narun/nconsumer"         // Use nconsumer types and pooling
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/fxamacker/cbor/v2" // CBOR encoding
	"github.com/nats-io/nats.go"
	"go.uber.org/zap" // Caddy uses Zap logger
)

func init() {
	caddy.RegisterModule(&Handler{})
	httpcaddyfile.RegisterHandlerDirective("narun", parseCaddyfile)
}

// Handler implements the Caddy HTTP handler for narun integration.
type Handler struct {
	//  Configuration
	NatsURL        string         `json:"nats_url,omitempty"`
	RequestTimeout caddy.Duration `json:"request_timeout,omitempty"`
	NatsStream     string         `json:"nats_stream,omitempty"`
	Routes         []RouteConfig  `json:"routes,omitempty"`

	//  Internal state
	logger   *zap.Logger
	nc       *nats.Conn
	js       nats.JetStreamContext
	timeout  time.Duration
	routeMap map[string]map[string]*RouteConfig // map[path]map[METHOD]*RouteConfig
	routesMu sync.RWMutex                       // Protects routeMap during config reloads

	initOnce sync.Once // Ensures NATS connection happens only once per instance lifecycle
	initErr  error     // Stores error from initOnce execution
}

// RouteConfig defines a single routing rule from HTTP to NATS app.
type RouteConfig struct {
	Path    string   `json:"path"`
	Methods []string `json:"methods"`
	App     string   `json:"app"`
}

// CaddyModule returns the Caddy module information.
func (*Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "http.handlers.narun",
		// This New function correctly returns a *Handler, which implements
		// the necessary interfaces (Provisioner, Validator, etc.)
		New: func() caddy.Module { return new(Handler) },
	}
}

// Provision sets up the handler instance. Connects to NATS, validates config.
func (h *Handler) Provision(ctx caddy.Context) error {
	h.logger = ctx.Logger(h) // Get Caddy's zap logger
	h.routesMu.Lock()
	defer h.routesMu.Unlock()

	// ... (rest of the config validation and route map building as before) ...
	if h.NatsURL == "" {
		h.NatsURL = nats.DefaultURL
		h.logger.Debug("using default nats url", zap.String("url", h.NatsURL))
	}
	if h.RequestTimeout <= 0 {
		h.RequestTimeout = caddy.Duration(30 * time.Second) // Default timeout
		h.logger.Debug("using default request timeout", zap.Duration("timeout", time.Duration(h.RequestTimeout)))
	}
	h.timeout = time.Duration(h.RequestTimeout)

	if strings.TrimSpace(h.NatsStream) == "" {
		return fmt.Errorf("nats_stream is required")
	}
	if strings.ContainsAny(h.NatsStream, ".*> ") {
		return fmt.Errorf("invalid nats_stream name '%s': contains invalid characters", h.NatsStream)
	}
	if len(h.Routes) == 0 {
		return fmt.Errorf("at least one 'route' must be configured")
	}

	h.routeMap = make(map[string]map[string]*RouteConfig)
	for i := range h.Routes {
		route := &h.Routes[i]
		if strings.TrimSpace(route.Path) == "" {
			return fmt.Errorf("invalid route config at index %d: path cannot be empty", i)
		}
		if len(route.Methods) == 0 {
			return fmt.Errorf("invalid route config at index %d for path '%s': methods list cannot be empty", i, route.Path)
		}
		if strings.TrimSpace(route.App) == "" {
			return fmt.Errorf("invalid route config at index %d for path '%s': app name cannot be empty", i, route.Path)
		}
		if strings.ContainsAny(route.App, ".*> ") {
			return fmt.Errorf("invalid app name '%s' for path '%s': contains invalid characters", route.App, route.Path)
		}

		if _, ok := h.routeMap[route.Path]; !ok {
			h.routeMap[route.Path] = make(map[string]*RouteConfig)
		}

		for _, method := range route.Methods {
			upperMethod := strings.ToUpper(strings.TrimSpace(method))
			if upperMethod == "" {
				return fmt.Errorf("invalid empty method in route config at index %d for path '%s'", i, route.Path)
			}
			if _, ok := h.routeMap[route.Path][upperMethod]; ok {
				return fmt.Errorf("duplicate route defined for path '%s' and method '%s'", route.Path, upperMethod)
			}
			h.routeMap[route.Path][upperMethod] = route
			h.logger.Debug("provisioned route",
				zap.String("path", route.Path),
				zap.String("method", upperMethod),
				zap.String("app", route.App),
				zap.String("nats_stream", h.NatsStream),
			)
		}
	}
	// Connect to NATS and Setup JetStream (only once per instance)
	h.initOnce.Do(func() {

		slogHandler := NewZapSlogHandler(h.logger, slog.LevelInfo)

		// Create the actual *slog.Logger instance
		slogLogger := slog.New(slogHandler)

		// Connect NATS
		conn, err := natsutil.ConnectNATS(h.NatsURL)
		if err != nil {
			h.initErr = fmt.Errorf("failed to connect to NATS at %s: %w", h.NatsURL, err)
			// Use Caddy's logger directly for initialization errors
			h.logger.Error("NATS connection failed", zap.String("url", h.NatsURL), zap.Error(h.initErr))
			return
		}
		h.nc = conn
		h.logger.Info("connected to NATS", zap.String("url", h.nc.ConnectedUrl()))

		jsCtx, err := natsutil.SetupJetStream(slogLogger, h.nc, h.NatsStream) // Now passing the correct type
		if err != nil {
			h.initErr = fmt.Errorf("failed to setup JetStream stream '%s': %w", h.NatsStream, err)
			// Use the slogLogger for logging the error, which routes back to zap
			slogLogger.Error("JetStream setup failed", "stream", h.NatsStream, "error", err)
			h.nc.Close()
			h.nc = nil
			return
		}
		h.js = jsCtx
		slogLogger.Info("JetStream context ready", "stream", h.NatsStream)
	})

	if h.initErr != nil {
		return h.initErr
	}
	if h.nc == nil || !h.nc.IsConnected() {
		return fmt.Errorf("NATS connection is not active")
	}

	return nil
}

// Validate ensures the handler is correctly configured.
func (h *Handler) Validate() error {
	h.routesMu.RLock()
	defer h.routesMu.RUnlock()

	if strings.TrimSpace(h.NatsStream) == "" {
		return fmt.Errorf("nats_stream is required")
	}
	if len(h.routeMap) == 0 && len(h.Routes) > 0 {
		// This indicates Provisioning likely failed before building the map
		return fmt.Errorf("routes configured but internal route map not built (provisioning error?)")
	}
	// Note: NATS connection health is implicitly checked in ServeHTTP

	return nil
}

// Cleanup closes the NATS connection.
func (h *Handler) Cleanup() error {
	h.logger.Debug("cleaning up narun handler")
	if h.nc != nil {
		h.nc.Close()
		h.logger.Info("NATS connection closed")
	}
	return nil
}

// ServeHTTP handles the incoming HTTP request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	// Ensure NATS connection is still valid (it might drop after provisioning)
	if h.nc == nil || !h.nc.IsConnected() {
		h.logger.Error("NATS connection not available", zap.String("path", r.URL.Path))
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("NATS connection unavailable"))
		return nil // We handled the response
	}

	path := r.URL.Path
	method := r.Method

	//  Find Route
	h.routesMu.RLock() // Read lock for accessing routeMap
	methodsForPath, pathFound := h.routeMap[path]
	var routeCfg *RouteConfig
	if pathFound {
		routeCfg = methodsForPath[method] // Method lookup expects uppercase already from Provision
	}
	h.routesMu.RUnlock() // Release lock

	if routeCfg == nil {
		// No matching route configured for this handler, pass to the next handler in the chain
		return next.ServeHTTP(w, r)
	}

	//  Route Found - Handle via NATS
	natsSubject := fmt.Sprintf("%s.%s", h.NatsStream, routeCfg.App)
	startTime := time.Now()

	h.logger.Debug("matched route, proxying via NATS",
		zap.String("method", method),
		zap.String("path", path),
		zap.String("app", routeCfg.App),
		zap.String("nats_subject", natsSubject),
	)

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error("error reading request body", zap.Error(err), zap.String("path", path))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to read request body"))
		return nil // Handled
	}
	defer r.Body.Close()

	//  Prepare NATS Request
	normalizedReq := nconsumer.GetRequestData()
	defer nconsumer.PutRequestData(normalizedReq) // Ensure it's put back

	normalizedReq.Method = method
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

	cborReqPayload, err := cbor.Marshal(normalizedReq)
	if err != nil {
		h.logger.Error("error marshalling request to CBOR", zap.Error(err), zap.String("subject", natsSubject))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal server error (encoding request)"))
		return nil // Handled
	}

	// Send NATS Request
	natsMsg, err := h.nc.Request(natsSubject, cborReqPayload, h.timeout)

	// Handle NATS Response/Error
	if err != nil {
		// TODO: Add Metrics
		statusCode := http.StatusInternalServerError
		respBody := "Internal server error (NATS communication)"
		if err == nats.ErrTimeout {
			statusCode = http.StatusGatewayTimeout
			respBody = "Request timed out waiting for backend processor"
			h.logger.Warn("NATS request timeout", zap.String("subject", natsSubject), zap.Duration("timeout", h.timeout))
		} else {
			h.logger.Error("NATS request error", zap.String("subject", natsSubject), zap.Error(err))
		}
		w.WriteHeader(statusCode)
		w.Write([]byte(respBody))
		return nil // Handled
	}

	// Process NATS Reply
	h.logger.Debug("received NATS reply", zap.String("subject", natsSubject), zap.Int("reply_size", len(natsMsg.Data)))

	normalizedResp := nconsumer.GetResponseData()
	defer nconsumer.PutResponseData(normalizedResp) // Ensure it's put back

	err = cbor.Unmarshal(natsMsg.Data, &normalizedResp)
	if err != nil {
		h.logger.Error("error unmarshalling CBOR response", zap.Error(err), zap.String("subject", natsSubject))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal server error (decoding response)"))
		return nil // Handled
	}

	// Write HTTP Response
	if normalizedResp.Headers != nil {
		for key, values := range normalizedResp.Headers {
			if key == "Content-Type" && len(values) > 0 {
				w.Header().Del(key)
			}
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
	}

	statusCode := normalizedResp.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}

	w.WriteHeader(statusCode)

	if normalizedResp.Body != nil {
		_, writeErr := w.Write(normalizedResp.Body)
		if writeErr != nil {
			h.logger.Error("error writing response body", zap.Error(writeErr), zap.Int("status", statusCode))
		}
	}

	h.logger.Debug("finished proxying request",
		zap.String("method", method),
		zap.String("path", path),
		zap.Int("status", statusCode),
		zap.Duration("duration", time.Since(startTime)),
	)

	return nil // Indicate we handled the request
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // consume directive name ("narun")
	if d.NextArg() {
		return d.ArgErr()
	}

	h.Routes = []RouteConfig{}

	for d.NextBlock(0) { // Open brace {
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
		case "nats_stream":
			if !d.AllArgs(&h.NatsStream) {
				return d.ArgErr()
			}
		case "route":
			var rc RouteConfig
			args := d.RemainingArgs()
			if len(args) < 2 {
				return d.Errf("route requires at least path and app name: route <path> <app> [METHOD...]")
			}
			rc.Path = args[0]
			rc.App = args[1]
			if len(args) > 2 {
				rc.Methods = args[2:]
			} else {
				rc.Methods = []string{} // Will be validated in Provision
			}
			h.Routes = append(h.Routes, rc)
		default:
			return d.Errf("unrecognized narun option: %s", option)
		}
	}
	// It's okay now if no routes are defined in a block, maybe it's just setting defaults?
	// Although the top-level check in Provision will still require at least one route overall.
	return nil
}

// parseCaddyfile unmarshals tokens from h into a new Middleware.
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var m Handler
	err := m.UnmarshalCaddyfile(h.Dispenser)
	if err != nil {
		return nil, err
	}
	// Methods check moved to Provision
	return &m, nil
}

// Interface guards should reflect the pointer receiver for interfaces implemented by Handler
var (
	_ caddy.Module                = (*Handler)(nil) // Check pointer type satisfies Module
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddy.Validator             = (*Handler)(nil)
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
	// Keep the ZapSlogHandler guard as is, it's a separate type
	_ slog.Handler = (*ZapSlogHandler)(nil)
)
