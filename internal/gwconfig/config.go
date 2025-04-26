package gwconfig

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// RouteType distinguishes between HTTP and gRPC routes
type RouteType string

const (
	RouteTypeHTTP RouteType = "http"
	RouteTypeGRPC RouteType = "grpc"
)

// RouteConfig defines a single routing rule. Only one of Path or GrpcService should be set.
type RouteConfig struct {
	// HTTP specific
	Path    string   `yaml:"path,omitempty"`    // HTTP request path prefix/exact match
	Methods []string `yaml:"methods,omitempty"` // HTTP methods

	// gRPC specific
	GrpcService string `yaml:"grpc,omitempty"` // Fully qualified gRPC service name

	// Common
	Service string `yaml:"service"` // Target NATS Micro service name (REQUIRED for both types)

	// Internal
	Type RouteType `yaml:"-"` // Determined during load
}

type Config struct {
	NatsURL               string        `yaml:"nats_url"`
	ServerAddr            string        `yaml:"server_addr"`
	GrpcAddr              string        `yaml:"grpc_addr,omitempty"` // Optional gRPC listen address
	MetricsAddr           string        `yaml:"metrics_addr"`        // metrics server listen address
	RequestTimeoutSeconds int           `yaml:"request_timeout_seconds"`
	RequestTimeout        time.Duration `yaml:"-"` // Calculated field
	Routes                []RouteConfig `yaml:"routes"`

	// Internal mappings for faster lookups
	httpRouteMap map[string]map[string]*RouteConfig `yaml:"-"` // map[path]map[method]*RouteConfig
	grpcRouteMap map[string]*RouteConfig            `yaml:"-"` // map[fullGrpcServiceName]*RouteConfig
}

func LoadConfig(path string) (*Config, error) {
	cfg := &Config{
		// Default values
		NatsURL:               "nats://localhost:4222",
		ServerAddr:            ":8080", // Default HTTP port
		MetricsAddr:           ":9090",
		RequestTimeoutSeconds: 30,
		// GrpcAddr defaults to empty, meaning disabled unless specified
	}

	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	err = yaml.Unmarshal(yamlFile, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config YAML: %w", err)
	}

	// Validation
	if cfg.RequestTimeoutSeconds <= 0 {
		return nil, fmt.Errorf("request_timeout_seconds must be positive")
	}
	cfg.RequestTimeout = time.Duration(cfg.RequestTimeoutSeconds) * time.Second

	// Build lookup maps
	cfg.httpRouteMap = make(map[string]map[string]*RouteConfig)
	cfg.grpcRouteMap = make(map[string]*RouteConfig)

	if len(cfg.Routes) == 0 {
		return nil, fmt.Errorf("at least one route must be defined in the configuration")
	}

	hasGrpcRoutes := false
	for i := range cfg.Routes {
		route := &cfg.Routes[i] // Use pointer to the route in the slice

		// Common Validation
		if strings.TrimSpace(route.Service) == "" {
			return nil, fmt.Errorf("invalid route config at index %d: target NATS 'service' name cannot be empty", i)
		}
		if strings.ContainsAny(route.Service, "*> ") {
			return nil, fmt.Errorf("invalid target NATS 'service' name '%s' for route at index %d: contains invalid characters (*, >, space)", route.Service, i)
		}

		// Determine Route Type and Validate Specific Fields
		isHTTP := route.Path != ""
		isGRPC := route.GrpcService != ""

		if isHTTP && isGRPC {
			return nil, fmt.Errorf("invalid route config at index %d: cannot specify both 'path' and 'grpc'", i)
		}
		if !isHTTP && !isGRPC {
			return nil, fmt.Errorf("invalid route config at index %d: must specify either 'path' (for HTTP) or 'grpc' (for gRPC)", i)
		}

		if isHTTP {
			route.Type = RouteTypeHTTP
			// HTTP Specific Validation
			if strings.TrimSpace(route.Path) == "" {
				// This check is technically redundant due to isHTTP check, but kept for clarity
				return nil, fmt.Errorf("invalid HTTP route config at index %d: path cannot be empty", i)
			}
			if len(route.Methods) == 0 {
				return nil, fmt.Errorf("invalid HTTP route config at index %d for path '%s': methods list cannot be empty", i, route.Path)
			}

			// Initialize path map if it doesn't exist
			if _, ok := cfg.httpRouteMap[route.Path]; !ok {
				cfg.httpRouteMap[route.Path] = make(map[string]*RouteConfig)
			}

			// Add route for each method, checking for duplicates
			normalizedMethods := make([]string, 0, len(route.Methods))
			for _, method := range route.Methods {
				upperMethod := strings.ToUpper(strings.TrimSpace(method)) // Normalize method
				if upperMethod == "" {
					return nil, fmt.Errorf("invalid empty HTTP method in route config at index %d for path '%s'", i, route.Path)
				}
				if _, ok := cfg.httpRouteMap[route.Path][upperMethod]; ok {
					return nil, fmt.Errorf("duplicate HTTP route defined for path '%s' and method '%s'", route.Path, upperMethod)
				}
				cfg.httpRouteMap[route.Path][upperMethod] = route // Store pointer to the route config
				normalizedMethods = append(normalizedMethods, upperMethod)
			}
			route.Methods = normalizedMethods // Update with normalized methods

		} else { // isGRPC
			route.Type = RouteTypeGRPC
			hasGrpcRoutes = true
			// gRPC Specific Validation
			trimmedGrpcService := strings.TrimSpace(route.GrpcService)
			if trimmedGrpcService == "" {
				// Redundant due to isGRPC check
				return nil, fmt.Errorf("invalid gRPC route config at index %d: 'grpc' service name cannot be empty", i)
			}
			// Basic validation for gRPC service name (e.g., package.Service)
			if strings.ContainsAny(trimmedGrpcService, " /") { // Disallow spaces or slashes
				return nil, fmt.Errorf("invalid gRPC service name '%s' at index %d: contains invalid characters (space, /)", route.GrpcService, i)
			}
			route.GrpcService = trimmedGrpcService // Use trimmed version

			// Check for duplicate gRPC service mappings
			if _, ok := cfg.grpcRouteMap[route.GrpcService]; ok {
				return nil, fmt.Errorf("duplicate gRPC route defined for service '%s'", route.GrpcService)
			}
			cfg.grpcRouteMap[route.GrpcService] = route
		}
	}

	// If gRPC routes are defined, GrpcAddr must also be set
	if hasGrpcRoutes && strings.TrimSpace(cfg.GrpcAddr) == "" {
		return nil, fmt.Errorf("gRPC routes are defined, but 'grpc_addr' is missing or empty in the configuration")
	}
	// If GrpcAddr is set, but no gRPC routes, log a warning? Or allow it? Let's allow it for now.

	return cfg, nil
}

// FindHttpRoute finds a matching HTTP route config for a given path and method.
func (c *Config) FindHttpRoute(path, method string) (*RouteConfig, bool) {
	if methods, ok := c.httpRouteMap[path]; ok {
		// Method lookup expects uppercase
		if route, ok := methods[strings.ToUpper(method)]; ok {
			return route, true
		}
	}
	return nil, false
}

// FindGrpcRoute finds a matching gRPC route config for a given fully qualified gRPC service name.
func (c *Config) FindGrpcRoute(grpcServiceName string) (*RouteConfig, bool) {
	route, ok := c.grpcRouteMap[grpcServiceName]
	return route, ok
}

// GetNatsSubject returns the target NATS service name for a given route.
// This is the common function used by both HTTP and gRPC handlers
// to determine the destination NATS service.
func (c *Config) GetNatsSubject(route *RouteConfig) string {
	if route == nil {
		// Return empty string if route is nil to avoid panics
		// Calling code should handle this (e.g., log error, return 404/Unimplemented)
		return ""
	}
	// The 'service' field in RouteConfig directly holds the target NATS service name
	// for both HTTP and gRPC routes.
	return route.Service
}

// GetMicroSubject is functionally identical to GetNatsSubject in this design,
// as the 'service' field represents the target Micro service name (and subject).
// Kept for potential clarity or future differentiation if needed.
func (c *Config) GetMicroSubject(route *RouteConfig) string {
	return c.GetNatsSubject(route)
}
