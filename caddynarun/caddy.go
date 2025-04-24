package caddynarun

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	v1 "github.com/akhenakh/narun/gen/go/httprpc/v1" // Import generated proto types
	"github.com/akhenakh/narun/internal/natsutil"    // Use natsutil for JetStream setup
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"                  // Caddy uses Zap logger
	"google.golang.org/protobuf/proto" // Import proto package
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
	Methods []string `json:"methods"` // Should be uppercase
	App     string   `json:"app"`
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
	h.routesMu.Lock()
	defer h.routesMu.Unlock()

	// Validate and normalize config
	if h.NatsURL == "" {
		h.NatsURL = nats.DefaultURL
	}
	if h.RequestTimeout <= 0 {
		h.RequestTimeout = caddy.Duration(30 * time.Second)
	}
	h.timeout = time.Duration(h.RequestTimeout)

	if strings.TrimSpace(h.NatsStream) == "" {
		return fmt.Errorf("nats_stream is required")
	}
	if strings.ContainsAny(h.NatsStream, ".*> ") {
		return fmt.Errorf("invalid nats_stream name '%s': contains invalid characters", h.NatsStream)
	}
	if len(h.Routes) == 0 {
		return fmt.Errorf("at least one 'route' must be configured within the narun block")
	}

	// Build route map
	h.routeMap = make(map[string]map[string]*RouteConfig)
	for i := range h.Routes {
		route := &h.Routes[i] // Get pointer to modify methods in place if needed

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
			h.routeMap[route.Path][upperMethod] = route // Point to the original route config struct
			normalizedMethods = append(normalizedMethods, upperMethod)
			h.logger.Debug("provisioned route",
				zap.String("path", route.Path),
				zap.String("method", upperMethod),
				zap.String("app", route.App),
				zap.String("nats_stream", h.NatsStream),
			)
		}
		// Update the route's methods to be uppercase for consistency (optional, but cleaner)
		route.Methods = normalizedMethods
	}

	// Connect to NATS and Setup JetStream (only once per instance)
	h.initOnce.Do(func() {
		// Create slog handler wrapping Caddy's zap logger
		// Use Info level as default, adjust if needed
		slogHandler := NewZapSlogHandler(h.logger, slog.LevelInfo)
		slogLogger := slog.New(slogHandler)

		// Connect NATS
		conn, err := natsutil.ConnectNATS(h.NatsURL) // Assuming natsutil handles defaults
		if err != nil {
			h.initErr = fmt.Errorf("failed to connect to NATS at %s: %w", h.NatsURL, err)
			h.logger.Error("NATS connection failed", zap.String("url", h.NatsURL), zap.Error(h.initErr))
			return
		}
		h.nc = conn
		h.logger.Info("connected to NATS", zap.String("url", h.nc.ConnectedUrl()))

		// Setup JetStream using the single stream name
		jsCtx, err := natsutil.SetupJetStream(slogLogger, h.nc, h.NatsStream)
		if err != nil {
			h.initErr = fmt.Errorf("failed to setup JetStream stream '%s': %w", h.NatsStream, err)
			slogLogger.Error("JetStream setup failed", "stream", h.NatsStream, "error", err)
			h.nc.Close() // Clean up connection if JS setup fails
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
		// This case should ideally be covered by initErr, but check again
		return fmt.Errorf("NATS connection is not active after initialization attempt")
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
		// This indicates Provisioning likely failed before building the map or no routes were valid
		return fmt.Errorf("routes configured but internal route map not built (provisioning error or invalid routes?)")
	}
	// Note: NATS connection health is implicitly checked in ServeHTTP before use

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
	// (Keep NATS connection check unchanged)
	if h.nc == nil || !h.nc.IsConnected() {
		h.logger.Error("NATS connection not available for request handling",
			zap.String("path", r.URL.Path),
			zap.String("method", r.Method))
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "NATS connection unavailable")
		return nil
	}

	path := r.URL.Path
	method := r.Method

	// (Keep route lookup unchanged)
	h.routesMu.RLock()
	methodsForPath, pathFound := h.routeMap[path]
	var routeCfg *RouteConfig
	if pathFound {
		routeCfg = methodsForPath[strings.ToUpper(method)]
	}
	h.routesMu.RUnlock()

	if routeCfg == nil {
		// (Keep next handler logic unchanged)
		h.logger.Debug("request did not match any narun routes, passing to next handler",
			zap.String("method", method),
			zap.String("path", path),
		)
		return next.ServeHTTP(w, r)
	}

	// Route Found - Handle via NATS
	natsSubject := fmt.Sprintf("%s.%s", h.NatsStream, routeCfg.App)
	startTime := time.Now()

	h.logger.Debug("matched route, proxying via NATS",
		zap.String("method", method),
		zap.String("path", path),
		zap.String("app", routeCfg.App),
		zap.String("nats_stream", h.NatsStream),
		zap.String("nats_subject", natsSubject),
	)

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error("error reading request body", zap.Error(err), zap.String("path", path))
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "Failed to read request body")
		return nil
	}
	defer r.Body.Close()

	// Prepare NATS Request (Protobuf) using Opaque API
	protoReq := &v1.NatsHttpRequest{}
	protoReq.SetMethod(method)
	protoReq.SetPath(path)
	protoReq.SetQuery(r.URL.RawQuery)
	protoReq.SetProto(r.Proto)
	protoReq.SetBody(body)
	protoReq.SetRemoteAddr(r.RemoteAddr)
	protoReq.SetRequestUri(r.RequestURI)
	protoReq.SetHost(r.Host)

	// Create headers map
	headers := make(map[string]*v1.HeaderValues)
	for key, values := range r.Header {
		if len(values) > 0 {
			// Create a HeaderValues instance and set its values
			headerVal := &v1.HeaderValues{}
			headerVal.SetValues(values)
			headers[key] = headerVal
		}
	}
	protoReq.SetHeaders(headers)

	protoReqPayload, err := proto.Marshal(protoReq)
	if err != nil {
		h.logger.Error("error marshalling request to Protobuf", zap.Error(err), zap.String("subject", natsSubject))
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "Internal server error (encoding request)")
		return nil
	}

	// Send NATS Request (unchanged)
	natsMsg, err := h.nc.Request(natsSubject, protoReqPayload, h.timeout)

	// Handle NATS Response/Error (unchanged)
	if err != nil {
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
		fmt.Fprint(w, respBody)
		return nil
	}

	// Process NATS Reply (Protobuf)
	h.logger.Debug("received NATS reply", zap.String("subject", natsSubject), zap.Int("reply_size", len(natsMsg.Data)))

	protoResp := &v1.NatsHttpResponse{}
	err = proto.Unmarshal(natsMsg.Data, protoResp)
	if err != nil {
		h.logger.Error("error unmarshalling Protobuf response", zap.Error(err), zap.String("subject", natsSubject))
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "Internal server error (decoding response)")
		return nil
	}

	// Write HTTP Response using getters
	respHeaders := protoResp.GetHeaders()
	for key, headerValues := range respHeaders {
		if headerValues != nil {
			for _, value := range headerValues.GetValues() {
				w.Header().Add(key, value)
			}
		}
	}

	statusCode := int(protoResp.GetStatusCode())
	if statusCode == 0 {
		statusCode = http.StatusOK
	}

	w.WriteHeader(statusCode)

	respBody := protoResp.GetBody()
	if respBody != nil {
		_, writeErr := w.Write(respBody)
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

	return nil
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next() // consume directive name ("narun")

	// Optional arguments after directive? No, expect block.
	if d.NextArg() {
		return d.ArgErr() // No args expected here, only block
	}

	h.Routes = []RouteConfig{} // Initialize routes for this block

	// Parse the block
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
		case "nats_stream":
			if !d.AllArgs(&h.NatsStream) {
				return d.ArgErr()
			}
		case "route":
			args := d.RemainingArgs() // Get all args on the line: path app [METHOD...]
			if len(args) < 2 {
				return d.Errf("route requires at least path and app name: route <path> <app> [METHOD...]")
			}
			rc := RouteConfig{
				Path: args[0],
				App:  args[1],
			}
			if len(args) > 2 {
				rc.Methods = args[2:] // Methods will be normalized (uppercased) in Provision
			} else {
				// If no methods specified, should we default?
				// Let's require explicit methods for clarity. Provision will check len > 0.
				rc.Methods = []string{}
			}
			h.Routes = append(h.Routes, rc)
		default:
			return d.Errf("unrecognized narun option: %s", option)
		}
	}

	// Basic validation after parsing the block
	if h.NatsStream == "" {
		// Stream is mandatory per handler block
		return d.Err("nats_stream is required within the narun block")
	}
	if len(h.Routes) == 0 {
		// At least one route is needed per handler block
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
	// Further validation (like method checks, NATS connection) happens in Provision
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
	// Ensure ZapSlogHandler still implements slog.Handler
	_ slog.Handler = (*ZapSlogHandler)(nil)
)
