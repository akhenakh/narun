package nconsumer

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug" // Added for stack trace
	"syscall"
	"time"

	v1 "github.com/akhenakh/narun/gen/go/httprpc/v1" // Import generated proto types

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto" // Import proto package
)

// Options configure the NATS consumer listener.
type Options struct {
	NATSURL       string
	AppName       string          // Required: Application name (used to derive Subject, QueueGroup, DurableName)
	StreamName    string          // Required: The JetStream stream this app consumes from
	AckWait       time.Duration   // Optional: JetStream AckWait, defaults to 60 seconds
	NATSOptions   []nats.Option   // Optional: Additional NATS connect options
	ListenContext context.Context // Optional: External context for cancellation
	Logger        *slog.Logger    // Logger to use
}

// ListenAndServe connects to NATS, subscribes via JETSTREAM, processes requests,
// and publishes replies to a core NATS subject specified in the request.
func ListenAndServe(opts Options, handler http.Handler) error {
	// (Input validation and logger setup unchanged)
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}
	if opts.AppName == "" || opts.StreamName == "" {
		return fmt.Errorf("AppName and StreamName are required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if opts.NATSURL == "" {
		opts.NATSURL = nats.DefaultURL
	}
	if opts.AckWait <= 0 {
		opts.AckWait = 60 * time.Second // JetStream default is 30s, use 60s to be safe? Or match JS default? Let's use 60s.
	}

	derivedSubject := fmt.Sprintf("%s.%s", opts.StreamName, opts.AppName)
	derivedQueueGroup := opts.AppName
	derivedDurableName := fmt.Sprintf("%s-durable", derivedQueueGroup)

	natsOpts := []nats.Option{
		nats.Name(fmt.Sprintf("NATS Consumer - App: %s, Subject: %s", opts.AppName, derivedSubject)),
		nats.Timeout(10 * time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		// (NATS connection handlers unchanged)
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

	// --- Get JetStream Context ---
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}
	logger.Info("Obtained JetStream context")
	// (Optional stream check unchanged)
	_, err = js.StreamInfo(opts.StreamName)
	if err == nats.ErrStreamNotFound {
		logger.Warn("Stream not found by consumer, assuming gateway created it", "stream", opts.StreamName)
	} else if err != nil {
		logger.Warn("Could not get stream info", "stream", opts.StreamName, "error", err)
	}

	logger.Debug("Subscribing using JetStream QueueSubscribe", // Updated log
		"app", opts.AppName,
		"stream", opts.StreamName,
		"subject", derivedSubject,
		"queue_group", derivedQueueGroup,
		"durable", derivedDurableName)

	ctx := opts.ListenContext
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// --- Subscribe using JetStream ---
	sub, err := js.QueueSubscribe(derivedSubject, derivedQueueGroup, func(msg *nats.Msg) {
		protoReq := &v1.NatsHttpRequest{}
		if err := proto.Unmarshal(msg.Data, protoReq); err != nil {
			logger.Error("Error unmarshalling Protobuf request data", "error", err)
			nakMsg(msg, "protobuf unmarshal error", logger) // NAK the JS message
			return
		}

		// Extract reply subject
		replySubject := protoReq.GetReplySubject()
		if replySubject == "" {
			logger.Error("Received request without reply_subject", "request_id", protoReq.GetRequestId())
			nakMsg(msg, "missing reply subject", logger) // NAK the JS message
			return
		}

		// Process and publish reply
		processMsgAndPublishReply(ctx, nc, msg, handler, protoReq, replySubject, logger) // Pass nc and replySubject

	}, nats.Durable(derivedDurableName), nats.ManualAck(), nats.AckWait(opts.AckWait)) // Use JS options

	if err != nil {
		return fmt.Errorf("failed to JetStream subscribe to subject '%s' queue '%s': %w", derivedSubject, derivedQueueGroup, err)
	}

	// (Shutdown logic unchanged)
	defer func() {
		logger.Info("Unsubscribing...")
		if sub.IsValid() {
			if err := sub.Unsubscribe(); err != nil {
				logger.Error("Error during unsubscribe", "error", err)
			}
		}
	}()
	logger.Info("Consumer ready and waiting for messages...")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-quit:
		logger.Info("Received signal, shutting down...", "signal", sig)
	case <-ctx.Done():
		logger.Info("Context cancelled, shutting down...", "reason", ctx.Err())
	}
	logger.Info("Draining subscription...")
	if sub.IsValid() {
		if err := sub.Drain(); err != nil {
			logger.Error("Error draining subscription", "error", err)
		}
		logger.Info("Subscription drained")
	} else {
		logger.Info("Subscription already invalid, no draining needed.")
	}
	logger.Info("Consumer exiting")
	runtime.Goexit()
	return nil
}

// processes the message, publishes the reply to the specified subject, and ACKs the original JetStream msg
func processMsgAndPublishReply(
	ctx context.Context,
	nc *nats.Conn, // Core NATS conn for publishing reply
	originalMsg *nats.Msg, // The JetStream message to ACK/NAK
	handler http.Handler,
	protoReq *v1.NatsHttpRequest,
	replySubject string, // The core NATS subject to publish the reply to
	logger *slog.Logger,
) {
	requestID := protoReq.GetRequestId() // For logging context
	log := logger.With("request_id", requestID)
	log.Debug("Processing JetStream message", "subject", originalMsg.Subject, "reply_to", replySubject)

	httpRequest, err := reconstructHttpRequest(ctx, protoReq)
	if err != nil {
		log.Error("Error reconstructing HTTP request", "error", err)
		nakMsg(originalMsg, "request reconstruction error", log)
		return
	}

	shim := newResponseWriterShim()

	var handlerErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				handlerErr = fmt.Errorf("handler panic: %v", r)
				log.Error("Recovered from panic in handler", "panic", r, "stack", string(debug.Stack()))
			}
		}()
		handler.ServeHTTP(shim, httpRequest)
	}()

	// Create a new response protocol buffer
	protoResp := &v1.NatsHttpResponse{}

	if handlerErr != nil {
		// Using setters - good!
		protoResp.SetStatusCode(http.StatusInternalServerError)

		headers := make(map[string]*v1.HeaderValues)
		headerVal := &v1.HeaderValues{}
		headerVal.SetValues([]string{"text/plain; charset=utf-8"})
		headers["Content-Type"] = headerVal

		protoResp.SetHeaders(headers)
		protoResp.SetBody([]byte("Internal Server Error"))
	} else {
		// Delegating to shim.copyToProto
		shim.copyToProto(protoResp)
	}

	// Marshal the response (error or success)
	protoRespPayload, err := proto.Marshal(protoResp)
	if err != nil {
		log.Error("Failed to marshal response data to Protobuf", "error", err)
		// Cannot send reply, NAK original message
		nakMsg(originalMsg, "protobuf response marshal error", log)
		return
	}

	// Publish the response to the core NATS reply subject
	log.Debug("Publishing reply", "reply_subject", replySubject, "status_code", protoResp.GetStatusCode())
	err = nc.Publish(replySubject, protoRespPayload)
	if err != nil {
		log.Error("Failed to publish reply to core NATS", "reply_subject", replySubject, "error", err)
		// Failed to send reply, NAK original message
		nakMsg(originalMsg, "reply publish error", log)
		return
	}

	// Reply sent successfully, ACK the original JetStream message
	ackMsg(originalMsg, fmt.Sprintf("reply sent to %s", replySubject), log)
}

// reconstructHttpRequest remains the same
func reconstructHttpRequest(ctx context.Context, data *v1.NatsHttpRequest) (*http.Request, error) {
	reqUrl := &url.URL{
		Path:     data.GetPath(),
		RawQuery: data.GetQuery(),
		Scheme:   "http",
		Host:     data.GetHost(),
	}
	if reqUrl.Host == "" {
		reqUrl.Host = "nats-request"
	}
	bodyBytes := data.GetBody()
	bodyReader := bytes.NewReader(bodyBytes)
	req, err := http.NewRequestWithContext(ctx, data.GetMethod(), reqUrl.String(), bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create new request: %w", err)
	}
	req.Header = make(http.Header)
	for key, headerValues := range data.GetHeaders() {
		if headerValues != nil {
			vals := headerValues.GetValues()
			if len(vals) > 0 {
				copiedVals := make([]string, len(vals))
				copy(copiedVals, vals)
				req.Header[key] = copiedVals
			}
		}
	}
	protoVersion := data.GetProto()
	if protoVersion != "" {
		req.Proto = protoVersion
		req.ProtoMajor, req.ProtoMinor, _ = http.ParseHTTPVersion(protoVersion)
	} else {
		req.Proto = "HTTP/1.1"
		req.ProtoMajor = 1
		req.ProtoMinor = 1
	}
	req.RemoteAddr = data.GetRemoteAddr()
	req.RequestURI = data.GetRequestUri()
	req.Host = data.GetHost()
	req.ContentLength = int64(len(bodyBytes))
	return req, nil
}

// ackMsg acknowledges a NATS message.
func ackMsg(msg *nats.Msg, reason string, logger *slog.Logger) {
	if err := msg.Ack(); err != nil {
		logger.Error("Error acknowledging JetStream message", "reason", reason, "error", err)
	} else {
		logger.Debug("Successfully processed and acknowledged JetStream message", "reason", reason)
	}
}

// nakMsg negatively acknowledges a NATS message for redelivery.
func nakMsg(msg *nats.Msg, reason string, logger *slog.Logger) {
	logger.Warn("NAK'ing JetStream message", "reason", reason)
	// Consider NakWithDelay for repeated failures
	if err := msg.Nak(); err != nil {
		logger.Error("Error sending NAK for JetStream message", "reason", reason, "error", err)
	}
}
