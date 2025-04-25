package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type RouteConfig struct {
	Path    string   `yaml:"path"`
	Methods []string `yaml:"methods"`
	// App     string   `yaml:"app"` // REMOVED
	Service string `yaml:"service"` // NATS Micro service name (REQUIRED)
}

type Config struct {
	NatsURL               string `yaml:"nats_url"`
	ServerAddr            string `yaml:"server_addr"`
	MetricsAddr           string `yaml:"metrics_addr"`
	RequestTimeoutSeconds int    `yaml:"request_timeout_seconds"`
	// NatsStream            string        `yaml:"nats_stream"` // REMOVED
	RequestTimeout time.Duration `yaml:"-"` // Calculated field
	Routes         []RouteConfig `yaml:"routes"`

	// Internal mapping for faster lookups: map[path]map[method]*RouteConfig
	routeMap map[string]map[string]*RouteConfig `yaml:"-"`
}

func LoadConfig(path string) (*Config, error) {
	cfg := &Config{
		// Default values
		NatsURL:               "nats://localhost:4222",
		ServerAddr:            ":8080",
		MetricsAddr:           ":9090",
		RequestTimeoutSeconds: 30,
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

	// Build lookup map
	cfg.routeMap = make(map[string]map[string]*RouteConfig)

	if len(cfg.Routes) == 0 {
		return nil, fmt.Errorf("at least one route must be defined in the configuration")
	}

	for i := range cfg.Routes {
		route := &cfg.Routes[i] // Use pointer to the route in the slice

		// Validate route fields
		if strings.TrimSpace(route.Path) == "" {
			return nil, fmt.Errorf("invalid route config at index %d: path cannot be empty", i)
		}
		if len(route.Methods) == 0 {
			return nil, fmt.Errorf("invalid route config at index %d for path '%s': methods list cannot be empty", i, route.Path)
		}
		// --- Service is now REQUIRED ---
		if strings.TrimSpace(route.Service) == "" {
			return nil, fmt.Errorf("invalid route config at index %d for path '%s': service name cannot be empty", i, route.Path)
		}
		// Basic validation for service name (NATS subject component)
		// Allowing '.' for hierarchical names, but disallowing typical wildcards/spaces
		if strings.ContainsAny(route.Service, "*> ") {
			return nil, fmt.Errorf("invalid service name '%s' for path '%s': contains invalid characters (*, >, space)", route.Service, route.Path)
		}

		// Initialize path map if it doesn't exist
		if _, ok := cfg.routeMap[route.Path]; !ok {
			cfg.routeMap[route.Path] = make(map[string]*RouteConfig)
		}

		// Add route for each method, checking for duplicates
		for _, method := range route.Methods {
			upperMethod := strings.ToUpper(strings.TrimSpace(method)) // Normalize method
			if upperMethod == "" {
				return nil, fmt.Errorf("invalid empty method in route config at index %d for path '%s'", i, route.Path)
			}
			if _, ok := cfg.routeMap[route.Path][upperMethod]; ok {
				return nil, fmt.Errorf("duplicate route defined for path '%s' and method '%s'", route.Path, upperMethod)
			}
			cfg.routeMap[route.Path][upperMethod] = route // Store pointer to the route config
		}
	}

	return cfg, nil
}

// FindRoute finds a matching route config for a given path and method.
// Method should be UPPERCASE.
func (c *Config) FindRoute(path, method string) (*RouteConfig, bool) {
	if methods, ok := c.routeMap[path]; ok {
		// Method lookup expects uppercase
		if route, ok := methods[strings.ToUpper(method)]; ok {
			return route, true
		}
	}
	return nil, false
}

// GetMicroSubject returns the NATS Micro service name (which acts as the subject).
func (c *Config) GetMicroSubject(route *RouteConfig) string {
	if route == nil {
		return ""
	}
	// The 'service' field directly defines the target service name/subject.
	return route.Service
}

// GetNatsSubject - Keep this method name for now for compatibility in handler.go,
// but it now just returns the service name. Consider renaming later if appropriate.
func (c *Config) GetNatsSubject(route *RouteConfig) string {
	return c.GetMicroSubject(route)
}
