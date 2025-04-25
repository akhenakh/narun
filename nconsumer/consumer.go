package nconsumer

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync" // Import sync package
	"syscall"
	"time"

	"maps"

	"github.com/akhenakh/narun/internal/metrics"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// Pool for responseWriterShim objects
var shimPool = sync.Pool{
	New: func() interface{} {
		// Called when pool is empty
		return &responseWriterShim{
			header: make(http.Header), // Initialize header map
			// body will be acquired from bufferPool when needed/reset
		}
	},
}

// Pool for NATS Header maps used in replies
var headerPool = sync.Pool{
	New: func() any {
		return make(nats.Header)
	},
}

// Helper function to clear a nats.Header map before putting it back in the pool
func clearHeader(h nats.Header) {
	for k := range h {
		delete(h, k)
	}
}

// Options configure the NATS Micro service
type Options struct {
	NATSURL       string
	ServiceName   string // Required: The NATS Micro service name
	Description   string
	Version       string
	MaxConcurrent int // Max concurrent requests to process (uses semaphore)
	NATSOptions   []nats.Option
	Logger        *slog.Logger
}

// ListenAndServe runs a NATS Micro service with the given handler
func ListenAndServe(opts Options, handler http.Handler) error {
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}
	if opts.ServiceName == "" {
		return fmt.Errorf("ServiceName is required in Options")
	}
	if strings.ContainsAny(opts.ServiceName, "*> ") {
		return fmt.Errorf("invalid ServiceName '%s': contains invalid characters (*, >, space)", opts.ServiceName)
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
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = runtime.NumCPU() * 2
	} // Default concurrency

	//  Concurrency Control Semaphore
	concurrencySem := make(chan struct{}, opts.MaxConcurrent)
	logger.Debug("Concurrency limit set", "max", opts.MaxConcurrent)

	//  (NATS connection setup remains the same)
	natsOpts := []nats.Option{
		nats.Name(fmt.Sprintf("NATS Micro Service - %s", opts.ServiceName)),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) { logger.Error("NATS client disconnected", "error", err) }),
		nats.ReconnectHandler(func(nc *nats.Conn) { logger.Debug("NATS client reconnected", "url", nc.ConnectedUrl()) }),
		nats.ClosedHandler(func(nc *nats.Conn) { logger.Debug("NATS client connection closed") }),
	}
	natsOpts = append(natsOpts, opts.NATSOptions...)
	nc, err := nats.Connect(opts.NATSURL, natsOpts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS at %s: %w", opts.NATSURL, err)
	}
	defer nc.Close()
	logger.Info("Connected to NATS", "url", nc.ConnectedUrl())

	config := micro.Config{
		Name:        opts.ServiceName,
		Version:     opts.Version,
		Description: opts.Description,
	}

	httpHandlerAdapter := func(req micro.Request) {
		// --- Acquire Concurrency Semaphore ---
		concurrencySem <- struct{}{}
		metrics.WorkerCount.WithLabelValues(opts.ServiceName, "default").Inc() // Increment active worker gauge
		defer func() {
			<-concurrencySem                                                       // Release semaphore when done
			metrics.WorkerCount.WithLabelValues(opts.ServiceName, "default").Dec() // Decrement active worker gauge
		}()

		startTime := time.Now()
		logger.Debug("Processing request", "subject", req.Subject()) // Simplified log

		// Get Pooled Objects
		responseShim := shimPool.Get().(*responseWriterShim)
		// Ensure the shim gets a buffer from the pool upon retrieval/reset
		responseShim.body = bufferPool.Get().(*bytes.Buffer)
		// Reset shim state *before* use (alternative to resetting before Put)
		responseShim.Reset()
		// Get the body buffer again after reset cleared it
		responseShim.body = bufferPool.Get().(*bytes.Buffer)

		// Ensure shim is returned to pool even on panic, and reset state
		defer func() {
			responseShim.Reset() // Calls Put on the internal buffer
			shimPool.Put(responseShim)
		}()

		// --- (Extract HTTP info - Unchanged) ---
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

		// (Reconstruct HTTP request - Unchanged, maybe pool this later if needed)
		httpReq, err := reconstructHttpRequest(context.Background(), method, path, query, host, remoteAddr, incomingHeaders, req.Data())
		if err != nil {
			logger.Error("Failed to reconstruct HTTP request", "error", err)
			// Error Handling using Pooled Headers
			errorHeaders := headerPool.Get().(nats.Header)
			defer headerPool.Put(errorHeaders) // Return headers to pool
			clearHeader(errorHeaders)          // Clear before reuse/return

			errorHeaders.Set("Nats-Service-Error-Code", strconv.Itoa(http.StatusInternalServerError))
			errorHeaders.Set("Nats-Service-Error", "Failed to reconstruct HTTP request")
			respondErr := req.Respond(nil, micro.WithHeaders(micro.Headers(errorHeaders))) // Cast needed
			if respondErr != nil {
				logger.Error("Failed to send error response", "error", respondErr)
			}
			return // Return after handling error
		}

		// Process with http.Handler
		// Catch panics from user handler to prevent crashing the consumer instance
		var handlerPanicErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					handlerPanicErr = fmt.Errorf("handler panic: %v", r)
					logger.Error("Panic recovered in user handler", "error", handlerPanicErr, "subject", req.Subject())
				}
			}()
			handler.ServeHTTP(responseShim, httpReq)
		}()

		// Success/Error Response Handling using Pooled Headers
		replyHeaders := headerPool.Get().(nats.Header)
		defer headerPool.Put(replyHeaders) // Return headers to pool
		clearHeader(replyHeaders)          // Clear before reuse/return

		// Check if handler panicked
		if handlerPanicErr != nil {
			// Send a 500 error if the handler panicked
			responseShim.statusCode = http.StatusInternalServerError // Ensure status is 500
			replyHeaders.Set("Nats-Service-Error-Code", strconv.Itoa(http.StatusInternalServerError))
			replyHeaders.Set("Nats-Service-Error", "Internal Server Error")
			// Don't copy headers from shim in case of panic
			err = req.Respond(nil, micro.WithHeaders(micro.Headers(replyHeaders))) // Send empty body
		} else {
			// Normal response: Copy headers from shim
			maps.Copy(replyHeaders, responseShim.Header())
			replyHeaders.Set("X-Response-Status-Code", fmt.Sprintf("%d", responseShim.statusCode))
			// Send response
			err = req.Respond(responseShim.body.Bytes(), micro.WithHeaders(micro.Headers(replyHeaders))) // Cast needed
		}

		if err != nil {
			logger.Error("Failed to send NATS response", "error", err)
		}

		// Record metrics (status code might be 500 due to panic)
		duration := time.Since(startTime).Seconds()
		statusStr := fmt.Sprintf("%d", responseShim.statusCode) // Use status code captured (or set to 500)
		metrics.RequestProcessingTime.WithLabelValues(
			opts.ServiceName,
			"default", // Endpoint name or label
			statusStr,
		).Observe(duration)

		logger.Debug("Request processed",
			"subject", req.Subject(),
			"status", responseShim.statusCode,
			"duration_ms", duration*1000)
	}

	svc, err := micro.AddService(nc, config)
	if err != nil {
		return fmt.Errorf("failed to create NATS Micro service %s: %w", opts.ServiceName, err)
	}

	subject := opts.ServiceName
	endpointName := "handler"
	err = svc.AddEndpoint(endpointName, micro.HandlerFunc(httpHandlerAdapter), micro.WithEndpointSubject(subject))
	if err != nil {
		stopErr := svc.Stop()
		if stopErr != nil {
			logger.Error("Failed to stop service after endpoint add failure", "service", opts.ServiceName, "error", stopErr)
		}
		return fmt.Errorf("failed to add endpoint '%s' with subject '%s' to service '%s': %w", endpointName, subject, opts.ServiceName, err)
	}
	logger.Info("NATS Micro service started", "name", opts.ServiceName, "endpoint", endpointName, "subject", subject, "version", opts.Version)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down service...", "name", opts.ServiceName)
	stopErr := svc.Stop()
	if stopErr != nil {
		logger.Error("Error stopping service", "name", opts.ServiceName, "error", stopErr)
		return stopErr
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
	for key, values := range headers {
		if strings.HasPrefix(key, "X-Original-") {
			continue
		}
		req.Header[key] = values
	}
	if host != "" {
		req.Host = host
	}
	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}
	if req.Header.Get("Content-Length") == "" && len(body) > 0 {
		req.Header.Set("Content-Length", strconv.Itoa(len(body)))
		req.ContentLength = int64(len(body))
	} else if len(body) == 0 {
		req.ContentLength = 0
	}
	return req, nil
}
