package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http" // Used for status codes in NATS error headers
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	// Import the generated protobuf code
	hello_v1 "github.com/akhenakh/narun/consumers/cmd/grpc-hello/proto"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"google.golang.org/protobuf/proto" // Use the official protobuf package
)

// Default values
const (
	DefaultNatsURL = "nats://localhost:4222"
)

func main() {
	// --- Logger Setup ---
	logLevel := slog.LevelInfo
	if levelStr := os.Getenv("LOG_LEVEL"); levelStr != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(levelStr)); err == nil {
			logLevel = level
		}
	}
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true, // Add source code location to logs
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	//  Flags
	natsURL := flag.String("nats-url", DefaultNatsURL, "NATS server URL")
	// IMPORTANT: This service name MUST match the 'service:' value in narun's config.yaml for the gRPC route
	serviceName := flag.String("service", "grpc-hello-service", "NATS Micro service name to register")
	flag.Parse()

	if *serviceName == "" {
		logger.Error("NATS Micro service name cannot be empty. Use the -service flag.")
		os.Exit(1)
	}
	logger = logger.With("service", *serviceName) // Add service name to all logs

	// NATS Connection
	nc, err := nats.Connect(*natsURL,
		nats.Name(fmt.Sprintf("gRPC Hello Consumer - %s", *serviceName)),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) { logger.Error("NATS client disconnected", "error", err) }),
		nats.ReconnectHandler(func(nc *nats.Conn) { logger.Info("NATS client reconnected", "url", nc.ConnectedUrl()) }),
		nats.ClosedHandler(func(nc *nats.Conn) { logger.Info("NATS client connection closed") }),
	)
	if err != nil {
		logger.Error("Failed to connect to NATS", "url", *natsURL, "error", err)
		os.Exit(1)
	}
	defer nc.Close()
	logger.Info("Connected to NATS", "url", nc.ConnectedUrl())

	//  NATS Micro Service Handler
	grpcHandler := func(req micro.Request) {
		startTime := time.Now()
		logger.Debug("Processing NATS request", "subject", req.Subject())

		// Get the original gRPC method name from header
		originalMethod := req.Headers().Get("X-Original-Grpc-Method")
		if originalMethod == "" {
			logger.Error("Missing X-Original-Grpc-Method header")
			// Use req.Error instead of req.RespondError
			handlerErr := req.Error(strconv.Itoa(http.StatusBadRequest), "Missing required X-Original-Grpc-Method header", nil)
			if handlerErr != nil {
				logger.Error("Failed to send error response for missing header", "error", handlerErr)
			}
			return
		}
		logger = logger.With("grpc_method", originalMethod) // Add method to subsequent logs for this request

		var respBytes []byte
		var handlerErr error

		// Route based on the gRPC method
		switch originalMethod {
		case "/hello.v1.Greeter/SayHello":
			// Unmarshal Request for SayHello
			var helloReq hello_v1.SayHelloRequest
			if err := proto.Unmarshal(req.Data(), &helloReq); err != nil {
				logger.Error("Failed to unmarshal SayHelloRequest", "error", err)
				// Use req.Error instead of req.RespondError
				handlerErr = req.Error(strconv.Itoa(http.StatusBadRequest), "Invalid request payload", nil)
			} else {
				// Process Request
				logger.Info("Received SayHello request", "name", helloReq.GetName())
				greeting := fmt.Sprintf("Hello, %s!", helloReq.GetName())
				helloResp := &hello_v1.SayHelloResponse{Message: greeting}

				// 5. Marshal Response
				respBytes, err = proto.Marshal(helloResp)
				if err != nil {
					logger.Error("Failed to marshal SayHelloResponse", "error", err)
					// Use req.Error instead of req.RespondError
					handlerErr = req.Error(strconv.Itoa(http.StatusInternalServerError), "Failed to generate response", nil)
				} else {
					// 6. Respond with marshaled bytes
					handlerErr = req.Respond(respBytes)
				}
			}

		default:
			// Handle unknown methods for this service
			logger.Warn("Received request for unknown gRPC method")
			// Use req.Error instead of req.RespondError
			handlerErr = req.Error(strconv.Itoa(http.StatusNotImplemented), // Map to 501 Not Implemented
				fmt.Sprintf("Method %s not implemented by this service", originalMethod), nil)
		}

		// Log final status/error
		if handlerErr != nil {
			// Check if the error was already logged during req.Error/req.Respond
			// Avoid double logging if possible, but log if the send itself failed.
			// The error returned by req.Error/req.Respond indicates if the *send* failed.
			logger.Error("Failed to send NATS response/error", "error", handlerErr)
		}

		logger.Debug("Finished processing request",
			"duration_ms", time.Since(startTime).Milliseconds(),
			"success", handlerErr == nil)

	}

	// Add NATS Micro Service
	serviceConfig := micro.Config{
		Name:        *serviceName,
		Version:     "1.0.0",
		Description: "gRPC Hello World service proxied over NATS",
	}
	svc, err := micro.AddService(nc, serviceConfig)
	if err != nil {
		logger.Error("Failed to create NATS Micro service", "error", err)
		os.Exit(1)
	}

	// Add the endpoint - The subject used here is the *service name itself*
	// NATS Micro automatically uses the service name as the root subject.
	endpointName := "grpc_proxy" // Just an internal name for the endpoint config
	err = svc.AddEndpoint(endpointName, micro.HandlerFunc(grpcHandler),
		// Use the service name as the subject prefix for this endpoint group
		micro.WithEndpointSubject(*serviceName),
	)
	if err != nil {
		logger.Error("Failed to add endpoint to service", "endpoint", endpointName, "subject", *serviceName, "error", err)
		// Attempt to stop the service if endpoint add fails
		_ = svc.Stop()
		os.Exit(1)
	}

	logger.Info("gRPC-over-NATS consumer started", "nats_subject", *serviceName)

	//  Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down service...")
	stopErr := svc.Stop()
	if stopErr != nil {
		logger.Error("Error stopping NATS Micro service", "error", stopErr)
		// Exit with error status if stop fails cleanly
		os.Exit(1)
	}
	logger.Info("Service stopped cleanly.")
}
