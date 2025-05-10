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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Pool for responseWriterShim objects
var shimPool = sync.Pool{
	New: func() any {
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
		// Acquire Concurrency Semaphore
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

		// Extract HTTP info
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

// ListenAndServeGRPC runs a NATS Micro service that wraps a standard gRPC service implementation.
// It uses the provided grpc.ServiceDesc to route incoming NATS requests (based on X-Original-Grpc-Method header)
// to the appropriate method on the gRPC service implementation.
func ListenAndServeGRPC(opts Options, desc *grpc.ServiceDesc, impl interface{}) error {
	if desc == nil {
		return fmt.Errorf("grpc.ServiceDesc cannot be nil")
	}
	if impl == nil {
		return fmt.Errorf("gRPC service implementation cannot be nil")
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
	logger = logger.With("nats_service", opts.ServiceName, "grpc_service", desc.ServiceName)

	if opts.NATSURL == "" {
		opts.NATSURL = nats.DefaultURL
	}
	if opts.Version == "" {
		opts.Version = "0.1.0" // Consider deriving from desc or adding to opts?
	}
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = runtime.NumCPU() * 2
	}

	// Build a map from full gRPC method name to MethodDesc for quick lookup
	methodMap := make(map[string]*grpc.MethodDesc)
	for i := range desc.Methods {
		mDesc := &desc.Methods[i]
		fullMethodName := fmt.Sprintf("/%s/%s", desc.ServiceName, mDesc.MethodName)
		methodMap[fullMethodName] = mDesc
		logger.Debug("Registering gRPC method internally", "grpc_method", fullMethodName)
	}
	// TODO: Add support for desc.Streams if needed later

	if len(methodMap) == 0 {
		logger.Warn("No gRPC methods found in ServiceDesc, service will handle no calls.")
		// Continue running, but it won't do anything useful.
	}

	// Concurrency Control Semaphore
	concurrencySem := make(chan struct{}, opts.MaxConcurrent)
	logger.Debug("Concurrency limit set", "max", opts.MaxConcurrent)

	// NATS connection setup (similar to ListenAndServe)
	natsOpts := []nats.Option{
		nats.Name(fmt.Sprintf("NATS gRPC Consumer - %s", opts.ServiceName)),
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

	// The Core NATS Micro Handler using gRPC Desc
	grpcAdapter := func(req micro.Request) {
		// Acquire semaphore
		concurrencySem <- struct{}{}
		// Use gRPC service name for metrics? Or NATS service name? Let's use NATS for consistency with HTTP side.
		metrics.WorkerCount.WithLabelValues(opts.ServiceName, "grpc").Inc()
		defer func() {
			<-concurrencySem
			metrics.WorkerCount.WithLabelValues(opts.ServiceName, "grpc").Dec()
		}()

		startTime := time.Now()
		reqLogger := logger // Create request-scoped logger if needed later

		// Get Original gRPC Method
		originalMethod := req.Headers().Get("X-Original-Grpc-Method")
		if originalMethod == "" {
			reqLogger.Error("Missing X-Original-Grpc-Method header")
			err := req.Error(strconv.Itoa(http.StatusBadRequest), "Missing required X-Original-Grpc-Method header", nil)
			if err != nil {
				reqLogger.Error("Failed to send error response for missing header", "error", err)
			}
			return
		}
		reqLogger = reqLogger.With("grpc_method", originalMethod)

		// Find Method Descriptor
		mDesc, found := methodMap[originalMethod]
		if !found {
			reqLogger.Warn("Received request for unknown or unhandled gRPC method")
			err := req.Error(strconv.Itoa(http.StatusNotImplemented), // Map to 501 Not Implemented
				fmt.Sprintf("Method %s not implemented or routed to this NATS service", originalMethod), nil)
			if err != nil {
				reqLogger.Error("Failed to send error response for unknown method", "error", err)
			}
			return
		}

		// Prepare Decoder Function
		// This function will be called by the MethodDesc.Handler
		dec := func(v interface{}) error {
			// v will be a pointer to the correct request proto type (e.g., *SayHelloRequest)
			// We need to assert it to proto.Message to use proto.Unmarshal
			protoReq, ok := v.(proto.Message)
			if !ok {
				// This should not happen if protoc generated code correctly
				reqLogger.Error("Decoder target type does not implement proto.Message", "target_type", fmt.Sprintf("%T", v))
				return status.Errorf(codes.Internal, "internal error: decoder target type %T is not a proto.Message", v)
			}
			if err := proto.Unmarshal(req.Data(), protoReq); err != nil {
				reqLogger.Warn("Failed to unmarshal NATS request data into protobuf", "error", err, "target_type", fmt.Sprintf("%T", v))
				// Return a gRPC status error that the handler can potentially catch
				return status.Errorf(codes.InvalidArgument, "cannot unmarshal request body: %v", err)
			}
			return nil
		}

		// Call the gRPC Method Handler
		// We need a context. For now, use background. Could potentially pass tracing info via headers later.
		ctx := context.Background()
		// The handler expects: service implementation, context, decoder func, interceptor (nil for now)
		// It returns: response object (interface{}), error
		resp, err := mDesc.Handler(impl, ctx, dec, nil) // No interceptor support yet

		if err != nil {
			reqLogger.Warn("gRPC handler returned an error", "error", err)
			// Check if it's a gRPC status error
			if s, ok := status.FromError(err); ok {
				// Translate gRPC status to NATS error
				// Use gRPC code as NATS error code string, and message as description
				err = req.Error(strconv.Itoa(int(s.Code())), s.Message(), nil) // No data payload for error
			} else {
				// Non-gRPC error, return a generic internal server error
				err = req.Error(strconv.Itoa(http.StatusInternalServerError), "Internal server error", nil)
			}
			// Check if sending the NATS error response failed
			if err != nil {
				reqLogger.Error("Failed to send NATS error response", "error", err)
			}
		} else {
			// Success case: Marshal the response
			protoResp, ok := resp.(proto.Message)
			if !ok {
				// This should not happen if the handler returned the correct type
				reqLogger.Error("gRPC handler returned non-proto.Message response type", "resp_type", fmt.Sprintf("%T", resp))
				err = req.Error(strconv.Itoa(http.StatusInternalServerError), "Internal error: invalid response type from handler", nil)
			} else {
				respBytes, err := proto.Marshal(protoResp)
				if err != nil {
					reqLogger.Error("Failed to marshal gRPC response", "error", err)
					err = req.Error(strconv.Itoa(http.StatusInternalServerError), "Internal error: failed to marshal response", nil)
				} else {
					// Send successful response
					err = req.Respond(respBytes)
				}
			}
			// Check if sending the NATS success response failed
			if err != nil {
				reqLogger.Error("Failed to send NATS success response", "error", err)
			}
		}

		// Record processing time metric
		duration := time.Since(startTime).Seconds()
		// Determine status code for metrics based on handler error
		statusCode := codes.OK
		if err != nil {
			if s, ok := status.FromError(err); ok {
				statusCode = s.Code()
			} else {
				statusCode = codes.Internal // Default to internal error if not a status error
			}
		}
		metrics.RequestProcessingTime.WithLabelValues(
			opts.ServiceName,
			"grpc",              // Endpoint name or label for gRPC
			statusCode.String(), // Use gRPC code string for status
		).Observe(duration)

		reqLogger.Debug("Finished processing gRPC-adapted request",
			"duration_ms", time.Since(startTime).Milliseconds(),
			"grpc_status_code", statusCode)

	}

	// Add NATS Micro Service
	serviceConfig := micro.Config{
		Name:        opts.ServiceName,
		Version:     opts.Version,
		Description: opts.Description, // Maybe use desc.ServiceName here?
		// Consider adding metadata from ServiceDesc if useful
	}
	svc, err := micro.AddService(nc, serviceConfig)
	if err != nil {
		return fmt.Errorf("failed to create NATS Micro service %s: %w", opts.ServiceName, err)
	}

	// Add the endpoint - listen on the ServiceName subject
	err = svc.AddEndpoint("grpc_handler", // Internal endpoint name
		micro.HandlerFunc(grpcAdapter),
		micro.WithEndpointSubject(opts.ServiceName), // Use NATS service name as subject
	)
	if err != nil {
		_ = svc.Stop() // Attempt cleanup
		return fmt.Errorf("failed to add endpoint '%s' to service '%s': %w", opts.ServiceName, opts.ServiceName, err)
	}

	logger.Info("NATS Micro service started, wrapping gRPC service", "nats_subject", opts.ServiceName)

	// Graceful Shutdown (same as ListenAndServe)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down service...")
	stopErr := svc.Stop()
	if stopErr != nil {
		logger.Error("Error stopping NATS Micro service", "error", stopErr)
		return stopErr // Return the error from stop
	}
	logger.Info("Service stopped cleanly.")
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
