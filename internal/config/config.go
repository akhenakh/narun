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
	App     string   `yaml:"app"`
}

type Config struct {
	NatsURL               string        `yaml:"nats_url"`
	ServerAddr            string        `yaml:"server_addr"`
	MetricsAddr           string        `yaml:"metrics_addr"`
	RequestTimeoutSeconds int           `yaml:"request_timeout_seconds"`
	NatsStream            string        `yaml:"nats_stream"` // Global stream name
	RequestTimeout        time.Duration `yaml:"-"`           // Calculated field
	Routes                []RouteConfig `yaml:"routes"`

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
		NatsStream:            "DEFAULT_STREAM", // Default stream if not specified
	}

	yamlFile, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	err = yaml.Unmarshal(yamlFile, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config YAML: %w", err)
	}

	// --- Validation ---
	if cfg.RequestTimeoutSeconds <= 0 {
		return nil, fmt.Errorf("request_timeout_seconds must be positive")
	}
	cfg.RequestTimeout = time.Duration(cfg.RequestTimeoutSeconds) * time.Second

	if strings.TrimSpace(cfg.NatsStream) == "" {
		return nil, fmt.Errorf("nats_stream cannot be empty")
	}
	// Basic validation for NATS stream name (adjust regex/rules as needed)
	if strings.ContainsAny(cfg.NatsStream, ".*> ") {
		return nil, fmt.Errorf("invalid nats_stream name '%s': contains invalid characters", cfg.NatsStream)
	}

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
			// Default to allow any method if none specified? Or require explicit methods?
			// Let's require explicit methods for clarity.
			// Alternatively, you could default to ["GET"] or ["GET", "POST"] etc.
			return nil, fmt.Errorf("invalid route config at index %d for path '%s': methods list cannot be empty (consider using ['GET', 'POST', 'PUT', 'DELETE', ...])", i, route.Path)
		}
		if strings.TrimSpace(route.App) == "" {
			return nil, fmt.Errorf("invalid route config at index %d for path '%s': app name cannot be empty", i, route.Path)
		}
		// Basic validation for app name (part of subject)
		if strings.ContainsAny(route.App, ".*> ") {
			return nil, fmt.Errorf("invalid app name '%s' for path '%s': contains invalid characters", route.App, route.Path)
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

// GetNatsSubject derives the NATS subject for a given route.
func (c *Config) GetNatsSubject(route *RouteConfig) string {
	if route == nil {
		return ""
	}
	// Pattern: <STREAM>.<APP>
	return fmt.Sprintf("%s.%s", c.NatsStream, route.App)
}

// GetStreamName returns the globally configured stream name.
func (c *Config) GetStreamName() string {
	return c.NatsStream
}

// GetStreamNames returns the single global stream name in a slice.
// Used for compatibility with SetupJetStream which might handle multiple streams in future.
func (c *Config) GetStreamNames() []string {
	if c.NatsStream == "" {
		return []string{} // Should not happen with validation, but safe
	}
	return []string{c.NatsStream}
}
