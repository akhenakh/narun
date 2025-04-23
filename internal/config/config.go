package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type RouteConfig struct {
	Path        string `yaml:"path"`
	Method      string `yaml:"method"`
	NatsStream  string `yaml:"nats_stream"`
	NatsSubject string `yaml:"nats_subject"`
}

type Config struct {
	NatsURL               string        `yaml:"nats_url"`
	ServerAddr            string        `yaml:"server_addr"`
	MetricsAddr           string        `yaml:"metrics_addr"`
	RequestTimeoutSeconds int           `yaml:"request_timeout_seconds"`
	RequestTimeout        time.Duration `yaml:"-"` // Calculated field
	Routes                []RouteConfig `yaml:"routes"`

	// Internal mapping for faster lookups
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

	if cfg.RequestTimeoutSeconds <= 0 {
		return nil, fmt.Errorf("request_timeout_seconds must be positive")
	}
	cfg.RequestTimeout = time.Duration(cfg.RequestTimeoutSeconds) * time.Second

	// Build lookup map
	cfg.routeMap = make(map[string]map[string]*RouteConfig)
	streamSet := make(map[string]struct{}) // To collect unique stream names

	for i := range cfg.Routes {
		route := &cfg.Routes[i] // Get pointer to modify map correctly
		if route.Path == "" || route.Method == "" || route.NatsStream == "" || route.NatsSubject == "" {
			return nil, fmt.Errorf("invalid route config at index %d: missing fields", i)
		}
		if _, ok := cfg.routeMap[route.Path]; !ok {
			cfg.routeMap[route.Path] = make(map[string]*RouteConfig)
		}
		if _, ok := cfg.routeMap[route.Path][route.Method]; ok {
			return nil, fmt.Errorf("duplicate route defined for path %s and method %s", route.Path, route.Method)
		}
		cfg.routeMap[route.Path][route.Method] = route
		streamSet[route.NatsStream] = struct{}{}
	}

	// Optional: Log loaded config details
	fmt.Printf("Loaded config: %+v\n", cfg)
	fmt.Printf("Unique NATS streams configured: %v\n", streamSet)

	return cfg, nil
}

// FindRoute finds a matching route config for a given path and method.
func (c *Config) FindRoute(path, method string) (*RouteConfig, bool) {
	if methods, ok := c.routeMap[path]; ok {
		if route, ok := methods[method]; ok {
			return route, true
		}
	}
	return nil, false
}

// GetStreamNames returns a list of unique stream names from the config.
func (c *Config) GetStreamNames() []string {
	streamSet := make(map[string]struct{})
	for _, route := range c.Routes {
		streamSet[route.NatsStream] = struct{}{}
	}
	streams := make([]string, 0, len(streamSet))
	for stream := range streamSet {
		streams = append(streams, stream)
	}
	return streams
}
