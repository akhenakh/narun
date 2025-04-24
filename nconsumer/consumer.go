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
	"syscall"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/nats-io/nats.go"
)

// Options configure the NATS consumer listener.
type Options struct {
	NatsURL       string
	AppName       string          // Required: Application name (used to derive Subject, QueueGroup, DurableName)
	StreamName    string          // Required: The JetStream stream this app consumes from
	AckWait       time.Duration   // Optional: Defaults to 60 seconds
	NATSOptions   []nats.Option   // Optional: Additional NATS connect options
	ListenContext context.Context // Optional: External context for cancellation
	Logger        *slog.Logger    // Logger to use
}

// ListenAndServe connects to NATS, subscribes to the specified subject (derived from StreamName and AppName),
// and forwards requests to the provided http.Handler.
// It blocks until the context is cancelled or a termination signal is received.
func ListenAndServe(opts Options, handler http.Handler) error {
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}
	if opts.AppName == "" || opts.StreamName == "" {
		return fmt.Errorf("AppName and StreamName are required")
	}

	// Setup default logger if none provided
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// Defaults
	if opts.NatsURL == "" {
		opts.NatsURL = nats.DefaultURL
	}
	if opts.AckWait <= 0 {
		opts.AckWait = 60 * time.Second
	}

	// *** Derive NATS parameters ***
	derivedSubject := fmt.Sprintf("%s.%s", opts.StreamName, opts.AppName)
	derivedQueueGroup := opts.AppName // Use AppName as the queue group for load balancing
	derivedDurableName := fmt.Sprintf("%s-durable", derivedQueueGroup)

	natsOpts := []nats.Option{
		nats.Name(fmt.Sprintf("NATS Consumer - App: %s, Subject: %s", opts.AppName, derivedSubject)),
		nats.Timeout(10 * time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			logger.Error("NATS client disconnected, will attempt reconnect", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Debug("NATS client reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			logger.Debug("NATS client connection closed")
		}),
	}
	natsOpts = append(natsOpts, opts.NATSOptions...)

	nc, err := nats.Connect(opts.NatsURL, natsOpts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS at %s: %w", opts.NatsURL, err)
	}
	defer nc.Close()
	logger.Info("Connected to NATS", "url", nc.ConnectedUrl())

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}

	// Optional: Ensure stream exists
	_, err = js.StreamInfo(opts.StreamName)
	if err == nats.ErrStreamNotFound {
		logger.Warn("Stream not found, assuming it will be created elsewhere", "stream", opts.StreamName)
	} else if err != nil {
		logger.Warn("Could not get stream info", "stream", opts.StreamName, "error", err)
	}

	// Use Derived Values in Log
	logger.Debug("Subscribing to derived subject",
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

	// Use Derived Values in Subscription
	sub, err := js.QueueSubscribe(derivedSubject, derivedQueueGroup, func(msg *nats.Msg) {
		reqData := GetRequestData()
		defer PutRequestData(reqData)

		if err := cbor.Unmarshal(msg.Data, reqData); err != nil {
			logger.Error("Error unmarshalling CBOR request data", "error", err)
			nakMsg(msg, "unmarshal error", logger)
			return
		}
		// Pass the *pooled* reqData to processMsgInternal
		processMsgInternal(ctx, msg, handler, reqData, logger)
	}, nats.Durable(derivedDurableName), nats.ManualAck(), nats.AckWait(opts.AckWait))

	if err != nil {
		// Add derived values to error context
		return fmt.Errorf("failed to subscribe to subject '%s' with queue '%s': %w", derivedSubject, derivedQueueGroup, err)
	}
	defer func() {
		logger.Info("Unsubscribing...")
		if err := sub.Unsubscribe(); err != nil {
			logger.Error("Error during unsubscribe", "error", err)
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
	if err := sub.Drain(); err != nil {
		logger.Error("Error draining subscription", "error", err)
	}
	logger.Info("Subscription drained")
	logger.Info("Consumer exiting")
	runtime.Goexit()
	return nil // Should not be reached
}

// processMsg handles a single NATS message
func processMsgInternal(ctx context.Context, msg *nats.Msg, handler http.Handler, reqData *RequestData, logger *slog.Logger) {
	logger.Debug("Received message", "subject", msg.Subject)

	httpRequest, err := reconstructHttpRequest(ctx, *reqData)
	if err != nil {
		logger.Error("Error reconstructing HTTP request", "error", err)
		nakMsg(msg, "request reconstruction error", logger)
		return
	}

	shim := newResponseWriterShim()

	var handlerErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				handlerErr = fmt.Errorf("handler panic: %v", r)
				logger.Error("Recovered from panic in handler", "panic", r)
			}
		}()
		handler.ServeHTTP(shim, httpRequest)
	}()

	respData := GetResponseData()
	defer PutResponseData(respData)

	if handlerErr != nil {
		respData.StatusCode = http.StatusInternalServerError
		respData.Headers["Content-Type"] = []string{"text/plain; charset=utf-8"}
		respData.Body = []byte("Internal Server Error")

		if err := sendNatsResponse(msg, respData, logger); err != nil {
			logger.Error("Error sending NATS panic response", "error", err)
			nakMsg(msg, "panic response send error", logger)
		} else {
			ackMsg(msg, "panic response sent", logger)
		}
		return
	}

	shim.copyTo(respData)

	if err := sendNatsResponse(msg, respData, logger); err != nil {
		logger.Error("Error sending NATS response", "error", err)
		nakMsg(msg, "response send error", logger)
		return
	}

	ackMsg(msg, fmt.Sprintf("%s %s -> %d", reqData.Method, reqData.Path, respData.StatusCode), logger)
}

// reconstructHttpRequest creates an *http.Request from the received RequestData
func reconstructHttpRequest(ctx context.Context, data RequestData) (*http.Request, error) {
	reqUrl := &url.URL{
		Path:     data.Path,
		RawQuery: data.Query,
		Scheme:   "http",
		Host:     data.Host,
	}
	if reqUrl.Host == "" {
		reqUrl.Host = "nats-request"
	}

	bodyReader := bytes.NewReader(data.Body)

	req, err := http.NewRequestWithContext(ctx, data.Method, reqUrl.String(), bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create new request: %w", err)
	}

	req.Header = data.Headers

	if data.Proto != "" {
		req.Proto = data.Proto
		req.ProtoMajor, req.ProtoMinor, _ = http.ParseHTTPVersion(data.Proto)
	} else {
		req.Proto = "HTTP/1.1"
		req.ProtoMajor = 1
		req.ProtoMinor = 1
	}
	if data.RemoteAddr != "" {
		req.RemoteAddr = data.RemoteAddr
	}
	if data.RequestURI != "" {
		req.RequestURI = data.RequestURI
	}
	if data.Host != "" {
		req.Host = data.Host
	}
	req.ContentLength = int64(len(data.Body))

	return req, nil
}

// sendNatsResponse encodes and sends the response back via msg.Respond
func sendNatsResponse(msg *nats.Msg, respData *ResponseData, logger *slog.Logger) error {
	cborRespPayload, err := cbor.Marshal(respData)
	if err != nil {
		logger.Error("Failed to marshal response data to CBOR",
			"response", fmt.Sprintf("%+v", respData),
			"error", err)
		return fmt.Errorf("failed to marshal response data to CBOR: %w", err)
	}

	if len(cborRespPayload) == 0 {
		logger.Warn("CBOR marshaling resulted in empty payload, sending CBOR null instead",
			"response", fmt.Sprintf("%+v", respData))
		cborRespPayload = []byte{0xf6}
	}

	if err := msg.Respond(cborRespPayload); err != nil {
		logger.Error("Failed to send NATS response", "subject", msg.Subject, "error", err)
		return fmt.Errorf("failed to send NATS response: %w", err)
	}
	return nil
}

// ackMsg acknowledges a NATS message.
func ackMsg(msg *nats.Msg, reason string, logger *slog.Logger) {
	if err := msg.Ack(); err != nil {
		logger.Error("Error acknowledging message", "reason", reason, "error", err)
	} else {
		logger.Debug("Successfully processed and acknowledged message", "reason", reason)
	}
}

// nakMsg negatively acknowledges a NATS message for redelivery.
func nakMsg(msg *nats.Msg, reason string, logger *slog.Logger) {
	logger.Warn("NAK'ing message", "reason", reason)
	if err := msg.Nak(); err != nil {
		logger.Error("Error sending NAK", "reason", reason, "error", err)
	}
}
