package nconsumer

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/akhenakh/narun/internal/metrics"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// Options configure the NATS Micro service
type Options struct {
	NATSURL       string
	ServiceName   string        // Optional: Override service name (defaults to AppName)
	Description   string        // Optional: Service description
	Version       string        // Optional: Service version (default: "0.1.0")
	MaxConcurrent int           // Optional: Max concurrent requests
	NATSOptions   []nats.Option // Optional: Additional NATS options
	Logger        *slog.Logger  // Logger to use
}

// ListenAndServe runs a NATS Micro service with the given handler
func ListenAndServe(opts Options, handler http.Handler) error {
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	if opts.NATSURL == "" {
		opts.NATSURL = nats.DefaultURL
	}

	if opts.Version == "" {
		opts.Version = "0.1.0"
	}

	// ServiceName is  required
	if opts.ServiceName == "" {
		return fmt.Errorf("ServiceName is required in Options")
	}
	// Validate service name (similar to config validation)
	if strings.ContainsAny(opts.ServiceName, "*> ") {
		return fmt.Errorf("invalid ServiceName '%s': contains invalid characters (*, >, space)", opts.ServiceName)
	}
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = runtime.NumCPU()
	}

	// Setup NATS connection
	natsOpts := []nats.Option{
		nats.Name(fmt.Sprintf("NATS Micro Service - %s", opts.ServiceName)), // Use ServiceName here
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			logger.Error("NATS client disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Debug("NATS client reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			logger.Debug("NATS client connection closed")
		}),
	}

	natsOpts = append(natsOpts, opts.NATSOptions...)

	nc, err := nats.Connect(opts.NATSURL, natsOpts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS at %s: %w", opts.NATSURL, err)
	}
	defer nc.Close()

	logger.Info("Connected to NATS", "url", nc.ConnectedUrl())

	// Create NATS Micro service
	config := micro.Config{
		Name:        opts.ServiceName, // Use ServiceName for micro registration
		Version:     opts.Version,
		Description: opts.Description,
		// Default QueueGroup is fine, usually derived from service name implicitly
	}

	// Create the HTTP handler adapter
	// Create the HTTP handler adapter
	httpHandlerAdapter := func(req micro.Request) {
		startTime := time.Now()
		logger.Debug("Processing request",
			"subject", req.Subject(),
			"reply", req.Reply())

		// Extract HTTP info from headers
		// req.Headers() returns micro.Headers (which is based on nats.Header/http.Header)
		// Using Get() is fine.
		incomingHeaders := req.Headers()
		method := incomingHeaders.Get("X-Original-Method")
		if method == "" {
			method = "POST"
		}
		path := incomingHeaders.Get("X-Original-Path")
		if path == "" {
			path = "/"
		}
		query := incomingHeaders.Get("X-Original-Query")
		host := incomingHeaders.Get("X-Original-Host")
		remoteAddr := incomingHeaders.Get("X-Original-RemoteAddr")

		// Reconstruct HTTP request
		// Pass the incomingHeaders (micro.Headers) - should work as it's map[string][]string underneath
		httpReq, err := reconstructHttpRequest(context.Background(), method, path, query, host, remoteAddr, incomingHeaders, req.Data())
		if err != nil {
			logger.Error("Failed to reconstruct HTTP request", "error", err)

			//  Corrected Internal Error Handling
			// Create headers using nats.Header type directly to use Set method
			errorHeaders := make(nats.Header)
			errorHeaders.Set("Nats-Service-Error-Code", strconv.Itoa(http.StatusInternalServerError))
			errorHeaders.Set("Nats-Service-Error", "Failed to reconstruct HTTP request")

			// Pass the nats.Header by casting it to micro.Headers for the option
			respondErr := req.Respond(nil, micro.WithHeaders(micro.Headers(errorHeaders)))
			if respondErr != nil {
				logger.Error("Failed to send error response", "error", respondErr)
			}
			return
		}

		// Create response writer shim (Unchanged)
		responseShim := newResponseWriterShim()

		// Process with http.Handler (Unchanged)
		handler.ServeHTTP(responseShim, httpReq)

		// Corrected Success Response Handling

		// Create headers for the response using nats.Header type directly
		replyHeaders := make(nats.Header)

		// Copy headers from the shim (which is http.Header/nats.Header compatible)
		for key, values := range responseShim.Header() {
			// Direct assignment works as underlying types match
			replyHeaders[key] = values
		}

		// Add the status code header using the Set method from nats.Header
		replyHeaders.Set("X-Response-Status-Code", fmt.Sprintf("%d", responseShim.statusCode))

		// Send the response using Respond with body and headers
		// Cast the nats.Header to micro.Headers for the option
		err = req.Respond(responseShim.body.Bytes(), micro.WithHeaders(micro.Headers(replyHeaders)))
		if err != nil {
			logger.Error("Failed to send NATS response", "error", err)
		}

		//  Record metrics (Unchanged)
		duration := time.Since(startTime).Seconds()
		metrics.RequestProcessingTime.WithLabelValues(
			opts.ServiceName, // Use ServiceName for metrics label
			"default",        // TODO: use endpoint name or keep simple? Using "default" for now.
			fmt.Sprintf("%d", responseShim.statusCode),
		).Observe(duration)

		logger.Debug("Request processed",
			"subject", req.Subject(),
			"status", responseShim.statusCode,
			"duration_ms", duration*1000)
	}

	// Create the service
	svc, err := micro.AddService(nc, config)
	if err != nil {
		return fmt.Errorf("failed to create NATS Micro service: %w", err)
	}

	// Define the subject based on stream and app name
	subject := opts.ServiceName
	endpointName := "handler" // Give the endpoint a logical name

	// Add endpoint for the main handler using ServiceName as the subject
	err = svc.AddEndpoint(endpointName, // A name for this specific endpoint within the service
		micro.HandlerFunc(httpHandlerAdapter),
		micro.WithEndpointSubject(subject), // Listen on the ServiceName directly
	)

	if err != nil {
		// Stop the service if adding the endpoint fails
		stopErr := svc.Stop()
		if stopErr != nil {
			logger.Error("Failed to stop service after endpoint add failure", "service", opts.ServiceName, "error", stopErr)
		}
		return fmt.Errorf("failed to add endpoint '%s' with subject '%s' to service '%s': %w", endpointName, subject, opts.ServiceName, err)
	}

	logger.Info("NATS Micro service started",
		"name", opts.ServiceName,
		"endpoint", endpointName,
		"subject", subject,
		"version", opts.Version)

	// Handle shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down service...", "name", opts.ServiceName)
	// Use context for graceful stop if needed, but Stop() is generally sufficient
	stopErr := svc.Stop()
	if stopErr != nil {
		logger.Error("Error stopping service", "name", opts.ServiceName, "error", stopErr)
		return stopErr // Return the stop error
	}
	logger.Info("Service stopped cleanly", "name", opts.ServiceName)

	return nil
}

// reconstructHttpRequest creates an HTTP request from components
func reconstructHttpRequest(ctx context.Context, method, path, query, host, remoteAddr string, headers micro.Headers, body []byte) (*http.Request, error) {
	var url string
	if query != "" {
		url = fmt.Sprintf("%s?%s", path, query)
	} else {
		url = path
	}

	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}

	// Set headers from micro.Headers (map[string][]string)
	// http.Header is also map[string][]string
	for key, values := range headers {
		// Skip X-Original-* headers as they're for internal use by the consumer logic
		if strings.HasPrefix(key, "X-Original-") {
			continue
		}
		// Directly assign the slice, http.Header handles it correctly.
		req.Header[key] = values
	}

	if host != "" {
		req.Host = host
	}

	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}

	// Set Content-Length based on body size if not already present
	// This is often helpful for downstream handlers
	if req.Header.Get("Content-Length") == "" && len(body) > 0 {
		req.Header.Set("Content-Length", strconv.Itoa(len(body)))
		req.ContentLength = int64(len(body))
	} else if len(body) == 0 {
		req.ContentLength = 0
	}

	return req, nil
}
