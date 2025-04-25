package main

import (
	"context" // Import context
	"flag"
	"fmt"
	"log/slog"
	"os"

	// Import the generated protobuf code AND the ServiceDesc
	hello_v1 "github.com/akhenakh/narun/consumers/cmd/grpc-hello/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	// Import the nconsumer package
	"github.com/akhenakh/narun/nconsumer"
)

// Default values
const (
	DefaultNatsURL = "nats://localhost:4222"
)

// --- Simple gRPC Server Implementation ---
// Implement the hello_v1.GreeterServer interface
type greeterServer struct {
	// Embed the unimplemented server type for forward compatibility
	hello_v1.UnimplementedGreeterServer
	logger *slog.Logger
}

// SayHello implements the GreeterServer interface
func (s *greeterServer) SayHello(ctx context.Context, req *hello_v1.SayHelloRequest) (*hello_v1.SayHelloResponse, error) {
	s.logger.Info("SayHello RPC called via NATS", "name", req.GetName())

	// Get name from request
	name := req.GetName()
	if name == "" {
		// Return a gRPC status error
		return nil, status.Error(codes.InvalidArgument, "Name cannot be empty")
	}

	// Create and return response
	message := fmt.Sprintf("Hello, %s!", name)
	return &hello_v1.SayHelloResponse{Message: message}, nil
}

func main() {
	logLevel := slog.LevelInfo
	if levelStr := os.Getenv("LOG_LEVEL"); levelStr != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(levelStr)); err == nil {
			logLevel = level
		}
	}
	logHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: true,
	})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	natsURL := flag.String("nats-url", DefaultNatsURL, "NATS server URL")
	// This service name MUST match the 'service:' value in narun's config.yaml for the gRPC route
	serviceName := flag.String("service", "grpc-hello-service", "NATS Micro service name to register")
	flag.Parse()

	if *serviceName == "" {
		logger.Error("NATS Micro service name cannot be empty. Use the -service flag.")
		os.Exit(1)
	}
	logger = logger.With("service", *serviceName)

	// Create gRPC Service Implementation
	serverImpl := &greeterServer{
		logger: logger, // Pass logger to the implementation
	}

	//  Configure nconsumer Options
	opts := nconsumer.Options{
		NATSURL:     *natsURL,
		ServiceName: *serviceName, // NATS Micro service name
		Logger:      logger,
		Description: "gRPC Hello World service proxied over NATS", // Optional description
		Version:     "1.0.1",                                      // Optional version
		// MaxConcurrent: 0, // Use default concurrency
	}

	//  Start the NATS Consumer for gRPC
	logger.Info("Starting NATS consumer for gRPC service", "nats_service", opts.ServiceName, "grpc_service", hello_v1.Greeter_ServiceDesc.ServiceName)

	// Use the ListenAndServeGRPC function
	// Pass options, the generated ServiceDesc, and the implementation struct
	err := nconsumer.ListenAndServeGRPC(opts, &hello_v1.Greeter_ServiceDesc, serverImpl)
	if err != nil {
		logger.Error("NATS gRPC consumer failed", "error", err)
		os.Exit(1)
	}

	// Will only reach here after graceful shutdown triggered by signal
	logger.Info("NATS gRPC consumer stopped cleanly.")
}
