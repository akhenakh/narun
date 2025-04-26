package grpcgateway

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/akhenakh/narun/internal/config"
	"github.com/akhenakh/narun/internal/metrics" // Import metrics
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rawBytesHolder holds raw bytes and implements methods needed by
// grpc.RecvMsg/SendMsg's internal proto.Unmarshal/Marshal calls.
type rawBytesHolder []byte

func (r *rawBytesHolder) Reset()         { *r = nil }
func (r *rawBytesHolder) String() string { return fmt.Sprintf("rawBytesHolder<%d bytes>", len(*r)) }
func (r *rawBytesHolder) ProtoMessage()  {} // Mark as proto.Message

// Unmarshal implements the proto.Unmarshaler interface.
// This will be called by RecvMsg's internal proto.Unmarshal.
func (r *rawBytesHolder) Unmarshal(b []byte) error {
	// Reset the slice but keep underlying capacity if desired (or create new)
	// *r = make([]byte, len(b))
	// copy(*r, b)
	// Or simpler: replace the slice content efficiently
	*r = append((*r)[:0], b...)
	return nil
}

// Marshal implements the proto.Marshaler interface (needed for SendMsg).
// Note: This returns the raw underlying bytes.
func (r *rawBytesHolder) Marshal() ([]byte, error) {
	return *r, nil
}

// Size implements the proto.Sizer interface (may be needed by Marshal/SendMsg).
func (r *rawBytesHolder) Size() int {
	return len(*r)
}

// Ensure rawBytesHolder satisfies common interfaces used by proto marshaling/unmarshaling.
// We don't need the full proto.Message methods like ProtoReflect.
type protoMarshaler interface {
	Marshal() ([]byte, error)
	Size() int
}
type protoUnmarshaler interface {
	Unmarshal([]byte) error
}

var _ protoMarshaler = (*rawBytesHolder)(nil)
var _ protoUnmarshaler = (*rawBytesHolder)(nil)

type GrpcHandler struct {
	NatsConn *nats.Conn
	Config   *config.Config
	Logger   *slog.Logger
}

func NewGrpcHandler(logger *slog.Logger, nc *nats.Conn, cfg *config.Config) *GrpcHandler {
	return &GrpcHandler{
		NatsConn: nc,
		Config:   cfg,
		Logger:   logger,
	}
}

// HandleUnknownStream intercepts gRPC requests for unregistered services.
func (h *GrpcHandler) HandleUnknownStream(srv interface{}, stream grpc.ServerStream) error {
	startTime := time.Now()
	fullMethodName, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		h.Logger.Error("Could not determine method from stream")
		metrics.GrpcRequestsTotal.WithLabelValues("unknown", codes.Internal.String()).Inc()
		return status.Error(codes.Internal, "could not determine method from stream")
	}

	parts := strings.SplitN(strings.TrimPrefix(fullMethodName, "/"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		h.Logger.Warn("Invalid gRPC method format", "method", fullMethodName)
		metrics.GrpcRequestsTotal.WithLabelValues(fullMethodName, codes.InvalidArgument.String()).Inc()
		metrics.GrpcRequestDuration.WithLabelValues(fullMethodName).Observe(time.Since(startTime).Seconds())
		return status.Errorf(codes.InvalidArgument, "invalid method name format: %s", fullMethodName)
	}
	grpcServiceName := parts[0]
	routeCfg, found := h.Config.FindGrpcRoute(grpcServiceName)
	if !found {
		h.Logger.Info("No route found for gRPC service", "grpc_service", grpcServiceName, "method", fullMethodName)
		metrics.GrpcRequestsTotal.WithLabelValues(fullMethodName, codes.Unimplemented.String()).Inc()
		metrics.GrpcRequestDuration.WithLabelValues(fullMethodName).Observe(time.Since(startTime).Seconds())
		return status.Errorf(codes.Unimplemented, "service %s not implemented or routed", grpcServiceName)
	}
	natsSubject := h.Config.GetNatsSubject(routeCfg)
	h.Logger.Debug("Routing gRPC request via NATS",
		"grpc_method", fullMethodName,
		"grpc_service", grpcServiceName,
		"nats_subject", natsSubject)

	// Read Request Bytes using RecvMsg
	var reqBytes rawBytesHolder // Use our enhanced holder type
	// RecvMsg should now call reqBytes.Unmarshal(payload) internally
	err := stream.RecvMsg(&reqBytes)
	if err != nil {
		if errors.Is(err, io.EOF) {
			h.Logger.Debug("gRPC stream EOF reading request, treating as empty body or completed unary", "method", fullMethodName)
		} else {
			h.Logger.Error("Failed reading gRPC request message", "error", err, "method", fullMethodName)
			metrics.GrpcRequestsTotal.WithLabelValues(fullMethodName, codes.Internal.String()).Inc()
			metrics.GrpcRequestDuration.WithLabelValues(fullMethodName).Observe(time.Since(startTime).Seconds())
			// Don't wrap the error here, return the specific gRPC error if possible
			return fmt.Errorf("failed reading request message: %w", err)
		}
	}
	// reqBytes now contains the raw payload bytes

	// NATS Request/Reply
	natsRequest := nats.NewMsg(natsSubject)
	natsRequest.Data = []byte(reqBytes) // Use the bytes from the holder
	natsRequest.Header = make(nats.Header)
	natsRequest.Header.Set("X-Original-Grpc-Method", fullMethodName)

	h.Logger.Debug("Sending NATS request for gRPC",
		"subject", natsRequest.Subject,
		"grpc_method", fullMethodName,
		"body_len", len(natsRequest.Data))

	natsReply, err := h.NatsConn.RequestMsg(natsRequest, h.Config.RequestTimeout)
	natsStatus := metrics.StatusSuccess // Assume success

	if err != nil {
		respStatus := codes.Internal
		errMsg := "internal server error (NATS communication)"
		if errors.Is(err, nats.ErrTimeout) {
			respStatus = codes.DeadlineExceeded
			errMsg = "request timed out waiting for backend processor"
			natsStatus = metrics.StatusTimeout
			h.Logger.Warn("NATS request timeout for gRPC", "subject", natsSubject, "grpc_method", fullMethodName, "timeout", h.Config.RequestTimeout)
		} else if errors.Is(err, nats.ErrNoResponders) {
			respStatus = codes.Unavailable
			errMsg = "no backend processors available for the request"
			natsStatus = metrics.StatusError
			h.Logger.Warn("NATS no responders for gRPC", "subject", natsSubject, "grpc_method", fullMethodName)
		} else {
			natsStatus = metrics.StatusError
			h.Logger.Error("NATS request error for gRPC", "subject", natsSubject, "grpc_method", fullMethodName, "error", err)
		}
		metrics.NatsRequestsTotal.WithLabelValues(natsSubject, natsStatus).Inc()
		metrics.GrpcRequestsTotal.WithLabelValues(fullMethodName, respStatus.String()).Inc()
		metrics.GrpcRequestDuration.WithLabelValues(fullMethodName).Observe(time.Since(startTime).Seconds())
		return status.Error(respStatus, errMsg) // Return gRPC status error
	}

	// Process NATS Reply
	metrics.NatsRequestsTotal.WithLabelValues(natsSubject, natsStatus).Inc()
	h.Logger.Debug("Received NATS reply for gRPC",
		"reply_subject", natsReply.Subject,
		"grpc_method", fullMethodName,
		"body_len", len(natsReply.Data))
	// TODO: Handle potential NATS service error headers?

	//  Send Response Bytes using SendMsg
	// Assign NATS reply data directly to our holder type
	var respBytesHolder rawBytesHolder = natsReply.Data

	// SendMsg should now call respBytesHolder.Marshal() internally
	err = stream.SendMsg(&respBytesHolder) // SendMsg needs a pointer
	if err != nil {
		h.Logger.Error("Failed sending gRPC response message", "error", err, "method", fullMethodName)
		metrics.GrpcRequestsTotal.WithLabelValues(fullMethodName, codes.Internal.String()).Inc()
		metrics.GrpcRequestDuration.WithLabelValues(fullMethodName).Observe(time.Since(startTime).Seconds())
		return fmt.Errorf("failed sending response message: %w", err) // Return error to signal broken stream
	}

	metrics.GrpcRequestsTotal.WithLabelValues(fullMethodName, codes.OK.String()).Inc()
	metrics.GrpcRequestDuration.WithLabelValues(fullMethodName).Observe(time.Since(startTime).Seconds())
	h.Logger.Debug("Successfully proxied gRPC request", "method", fullMethodName, "duration_ms", time.Since(startTime).Milliseconds())

	return nil // Success
}

// Interface guard - Still checks against grpc.StreamHandler
var _ grpc.StreamHandler = (*GrpcHandler)(nil).HandleUnknownStream
