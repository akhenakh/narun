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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	v1 "github.com/akhenakh/narun/gen/go/httprpc/v1" // Import generated proto types
	"github.com/akhenakh/narun/internal/metrics"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto" // Import proto package
)

// Options configure the NATS consumer listener.
type Options struct {
	NATSURL       string
	AppName       string          // Required: Application name (used to derive Subject, QueueGroup, DurableName)
	StreamName    string          // Required: The JetStream stream this app consumes from
	AckWait       time.Duration   // Optional: JetStream AckWait, defaults to 60 seconds
	MaxConcurrent int             // Optional: Maximum number of concurrent requests to process (default: runtime.NumCPU())
	BatchSize     int             // Optional: Number of messages to fetch in a batch (default: 10)
	PollInterval  time.Duration   // Optional: Interval between fetch attempts when no messages (default: 10ms)
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

	if opts.BatchSize <= 0 {
		opts.BatchSize = 10 // Default batch size of 10
	}
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = runtime.NumCPU()
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 10 * time.Millisecond
	}
	// Derive subject, queue group, and durable name
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
	// Create context for cancellation
	ctx := opts.ListenContext
	if ctx == nil {
		ctx = context.Background()
	}
	natsOpts = append(natsOpts, opts.NATSOptions...)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	nc, err := nats.Connect(opts.NATSURL, natsOpts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS at %s: %w", opts.NATSURL, err)
	}
	defer nc.Close()
	logger.Info("Connected to NATS", "url", nc.ConnectedUrl())

	// Create a worker pool with semaphore pattern
	sem := make(chan struct{}, opts.MaxConcurrent)
	var wg sync.WaitGroup

	//  Get JetStream Context
	js, err := nc.JetStream()

	// Create a pull subscription instead of queue subscription
	sub, err := js.PullSubscribe(
		derivedSubject,
		derivedDurableName, // Use durable name as second parameter (not queue group)
		nats.ManualAck(),
		nats.AckWait(opts.AckWait),
		nats.BindStream(opts.StreamName),
	)

	if err != nil {
		return fmt.Errorf("failed to create pull subscription: %w", err)
	}

	// Log successful subscription
	logger.Info("Created pull subscription for batch processing",
		"subject", derivedSubject,
		"queue", derivedQueueGroup,
		"durable", derivedDurableName,
		"batch_size", opts.BatchSize,
		"max_concurrent", opts.MaxConcurrent)

	// Create metrics for tracking
	var currentWorkers atomic.Int32
	var totalProcessed atomic.Int64
	var batchesProcessed atomic.Int64

	// Main processing loop
	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("Context canceled, stopping batch processing")
				return
			default:
				// Fetch a batch of messages
				msgs, err := sub.Fetch(opts.BatchSize, nats.MaxWait(opts.PollInterval))

				if err != nil {
					if err == nats.ErrTimeout || err == context.DeadlineExceeded {
						// This is normal when no messages are available - just continue
						continue
					}

					if ctx.Err() != nil {
						// Context canceled during fetch - exit cleanly
						return
					}

					logger.Error("Error fetching batch", "error", err)
					// Back off slightly on error
					time.Sleep(opts.PollInterval)
					continue
				}

				// Log batch received
				batchSize := len(msgs)
				batchID := batchesProcessed.Add(1)
				logger.Debug("Received message batch",
					"batch_id", batchID,
					"size", batchSize,
					"active_workers", currentWorkers.Load())

				// Process each message in the batch concurrently
				for i, msg := range msgs {
					// Add to waitgroup before starting goroutine
					wg.Add(1)

					// Acquire semaphore (blocks if max concurrency reached)
					sem <- struct{}{}

					// Track worker counts
					workerID := currentWorkers.Add(1)

					// Process message in goroutine
					go func(msg *nats.Msg, msgIdx int) {
						startTime := time.Now()

						// Track active worker count in Prometheus
						metrics.WorkerCount.WithLabelValues(opts.AppName, opts.StreamName).Inc()

						// Ensure cleanup happens
						defer func() {
							<-sem // Release semaphore
							currentWorkers.Add(-1)
							wg.Done()
							totalProcessed.Add(1)

							// Log completion with timing
							logger.Debug("Finished processing message",
								"batch_id", batchID,
								"msg_idx", msgIdx,
								"worker_id", workerID,
								"duration_ms", time.Since(startTime).Milliseconds())
						}()

						// Log start of processing
						logger.Debug("Processing message",
							"batch_id", batchID,
							"msg_idx", msgIdx,
							"worker_id", workerID)

						// Unmarshal and process message (same as before)
						protoReq := &v1.NatsHttpRequest{}
						if err := proto.Unmarshal(msg.Data, protoReq); err != nil {
							logger.Error("Error unmarshalling request",
								"error", err,
								"batch_id", batchID,
								"msg_idx", msgIdx)
							nakMsg(msg, "protobuf unmarshal error", logger)
							return
						}

						// Extract reply subject
						replySubject := protoReq.GetReplySubject()
						if replySubject == "" {
							logger.Error("Missing reply subject",
								"request_id", protoReq.GetRequestId(),
								"batch_id", batchID,
								"msg_idx", msgIdx)
							nakMsg(msg, "missing reply subject", logger)
							return
						}

						// Process message and publish reply
						processMsgAndPublishReply(ctx, nc, msg, handler, protoReq, replySubject, logger)

					}(msg, i)
				}
			}
		}
	}()

	// Setup signal handling for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	select {
	case sig := <-quit:
		logger.Info("Received signal, shutting down...", "signal", sig)
	case <-ctx.Done():
		logger.Info("Context cancelled, shutting down...", "reason", ctx.Err())
	}

	// Cancel context to stop batch processing
	cancel()

	// Wait for in-flight requests to complete
	logger.Info("Waiting for in-flight requests to complete...",
		"active_workers", currentWorkers.Load(),
		"total_processed", totalProcessed.Load(),
		"batches_processed", batchesProcessed.Load())

	// Wait with timeout to prevent hanging forever
	waitCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	// Wait up to 30 seconds for graceful shutdown
	select {
	case <-waitCh:
		logger.Info("All workers completed successfully")
	case <-time.After(30 * time.Second):
		logger.Warn("Timed out waiting for workers to complete")
	}

	// Clean up subscription
	if err := sub.Drain(); err != nil {
		logger.Error("Error draining subscription", "error", err)
	}

	logger.Info("Consumer exiting",
		"total_processed", totalProcessed.Load(),
		"batches_processed", batchesProcessed.Load())

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

	// Start the overall processing timer
	startTime := time.Now()
	processingStatus := "success" // Default to success, will change if errors occur

	// Defer recording the overall processing time
	defer func() {
		duration := time.Since(startTime).Seconds()
		// Record the processing time with app and stream from the message subject
		// Parse subject to extract stream and app
		parts := strings.Split(originalMsg.Subject, ".")
		stream := ""
		app := ""
		if len(parts) >= 2 {
			stream = parts[0]
			app = parts[1]
		}

		metrics.RequestProcessingTime.WithLabelValues(
			app,
			stream,
			processingStatus,
		).Observe(duration)
	}()

	log.Debug("Processing JetStream message", "subject", originalMsg.Subject, "reply_to", replySubject)

	httpRequest, err := reconstructHttpRequest(ctx, protoReq)
	if err != nil {
		log.Error("Error reconstructing HTTP request", "error", err)
		processingStatus = "error" // Set status to error

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
