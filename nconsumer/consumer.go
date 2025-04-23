package nconsumer

import (
	"bytes"
	"context"
	"fmt"
	"log"
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
	Subject       string
	StreamName    string          // Required for durable consumer setup
	QueueGroup    string          // Required for load balancing and durability
	DurableName   string          // Optional: Defaults to QueueGroup + "-durable"
	AckWait       time.Duration   // Optional: Defaults to 60 seconds
	NATSOptions   []nats.Option   // Optional: Additional NATS connect options
	ListenContext context.Context // Optional: External context for cancellation
}

// ListenAndServe connects to NATS, subscribes to the specified subject,
// and forwards requests to the provided http.Handler.
// It blocks until the context is cancelled or a termination signal is received.
func ListenAndServe(opts Options, handler http.Handler) error {
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}
	if opts.Subject == "" || opts.StreamName == "" || opts.QueueGroup == "" {
		return fmt.Errorf("Subject, StreamName, and QueueGroup are required")
	}

	// Defaults
	if opts.NatsURL == "" {
		opts.NatsURL = nats.DefaultURL
	}
	if opts.DurableName == "" {
		opts.DurableName = fmt.Sprintf("%s-durable", opts.QueueGroup)
	}
	if opts.AckWait <= 0 {
		opts.AckWait = 60 * time.Second
	}

	natsOpts := []nats.Option{
		nats.Name(fmt.Sprintf("NATS Consumer - %s", opts.Subject)),
		nats.Timeout(10 * time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("NATS client disconnected: %v. Will attempt reconnect.", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("NATS client reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Printf("NATS client connection closed.")
		}),
	}
	natsOpts = append(natsOpts, opts.NATSOptions...)

	nc, err := nats.Connect(opts.NatsURL, natsOpts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS at %s: %w", opts.NatsURL, err)
	}
	defer nc.Close()
	log.Printf("Connected to NATS at %s", nc.ConnectedUrl())

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get JetStream context: %w", err)
	}

	// Optional: Ensure stream exists (could be done by the gateway too)
	_, err = js.StreamInfo(opts.StreamName)
	if err == nats.ErrStreamNotFound {
		log.Printf("Warning: Stream %s not found. Assuming it will be created elsewhere.", opts.StreamName)
		// Optionally, you could add logic here to create it if desired.
	} else if err != nil {
		log.Printf("Warning: Could not get stream info for %s: %v", opts.StreamName, err)
	}

	log.Printf("Subscribing to subject '%s' on stream '%s' with queue group '%s' (durable: %s)",
		opts.Subject, opts.StreamName, opts.QueueGroup, opts.DurableName)

	// Determine context for operation
	ctx := opts.ListenContext
	if ctx == nil {
		ctx = context.Background() // Default if none provided
	}
	// Create a cancellable context for graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // Ensure cancellation propagates

	// --- Subscription Logic ---
	sub, err := js.QueueSubscribe(opts.Subject, opts.QueueGroup, func(msg *nats.Msg) {
		processMsg(ctx, msg, handler) // Pass context to message processor
	}, nats.Durable(opts.DurableName), nats.ManualAck(), nats.AckWait(opts.AckWait))

	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	defer func() {
		log.Println("Unsubscribing...")
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("Error during unsubscribe: %v", err)
		}
	}()

	log.Printf("Consumer ready and waiting for messages...")

	// --- Graceful Shutdown Handling ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		log.Printf("Received signal: %v. Shutting down...", sig)
	case <-ctx.Done():
		log.Printf("Context cancelled: %v. Shutting down...", ctx.Err())
	}

	log.Println("Draining subscription...")

	if err := sub.Drain(); err != nil {
		log.Printf("Error draining subscription: %v", err)
	}
	log.Println("Subscription drained.")
	// NATS connection closed by defer nc.Close()

	log.Println("Consumer exiting.")
	runtime.Goexit() // Ensure clean exit
	return nil       // Should not be reached due to Goexit
}

// processMsg handles a single NATS message
func processMsg(ctx context.Context, msg *nats.Msg, handler http.Handler) {
	log.Printf("Received message on subject %s", msg.Subject)

	// Decode the CBOR request data from the gateway
	var reqData RequestData
	if err := cbor.Unmarshal(msg.Data, &reqData); err != nil {
		log.Printf("Error unmarshalling CBOR request data: %v", err)
		nakMsg(msg, "unmarshal error")
		return
	}

	// Reconstruct the http.Request
	httpRequest, err := reconstructHttpRequest(ctx, reqData)
	if err != nil {
		log.Printf("Error reconstructing HTTP request: %v", err)
		nakMsg(msg, "request reconstruction error")
		return
	}

	// Create the response writer shim
	shim := newResponseWriterShim()

	// Execute the user's handler
	// Add panic recovery
	var handlerErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				handlerErr = fmt.Errorf("handler panic: %v", r)
				log.Printf("Recovered from panic in handler: %v", r)
				// Capture stack trace if needed
			}
		}()
		handler.ServeHTTP(shim, httpRequest)
	}()

	// Check if handler panicked
	if handlerErr != nil {
		// Decide what to send back - potentially a 500 error response
		respData := &ResponseData{
			StatusCode: http.StatusInternalServerError,
			Headers:    http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}},
			Body:       []byte("Internal Server Error"),
		}
		if err := sendNatsResponse(msg, respData); err != nil {
			log.Printf("Error sending NATS panic response: %v", err)
			nakMsg(msg, "panic response send error")
		} else {
			ackMsg(msg, "panic response sent")
		}
		return // Don't proceed further
	}

	// Get the response data from the shim
	respData := shim.getResponseData()

	// Send the response back over NATS
	if err := sendNatsResponse(msg, respData); err != nil {
		log.Printf("Error sending NATS response: %v", err)
		nakMsg(msg, "response send error")
		return
	}

	// Acknowledge the NATS message
	ackMsg(msg, fmt.Sprintf("%s %s -> %d", reqData.Method, reqData.Path, respData.StatusCode))
}

// reconstructHttpRequest creates an *http.Request from the received RequestData
func reconstructHttpRequest(ctx context.Context, data RequestData) (*http.Request, error) {
	// Rebuild URL
	reqUrl := &url.URL{
		Path:     data.Path,
		RawQuery: data.Query,
		// Scheme and Host aren't strictly necessary for routing within the handler,
		// but might be useful if the handler inspects them.
		Scheme: "http", // Assume http unless specified otherwise
		Host:   data.Host,
	}
	if reqUrl.Host == "" {
		reqUrl.Host = "nats-request" // Placeholder
	}

	// Create request body reader
	bodyReader := bytes.NewReader(data.Body)

	// Create the request object
	req, err := http.NewRequestWithContext(ctx, data.Method, reqUrl.String(), bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create new request: %w", err)
	}

	// Populate headers
	req.Header = data.Headers

	// Set optional fields
	if data.Proto != "" {
		req.Proto = data.Proto
		req.ProtoMajor, req.ProtoMinor, _ = http.ParseHTTPVersion(data.Proto)
	} else {
		req.Proto = "HTTP/1.1" // Default
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

	// Calculate ContentLength (important for handlers that check it)
	req.ContentLength = int64(len(data.Body))

	return req, nil
}

// sendNatsResponse encodes and sends the response back via msg.Respond
func sendNatsResponse(msg *nats.Msg, respData *ResponseData) error {
	cborRespPayload, err := cbor.Marshal(respData)
	if err != nil {
		return fmt.Errorf("failed to marshal response data to CBOR: %w", err)
	}

	if err := msg.Respond(cborRespPayload); err != nil {
		return fmt.Errorf("failed to send NATS response: %w", err)
	}
	return nil
}

// ackMsg acknowledges a NATS message.
func ackMsg(msg *nats.Msg, reason string) {
	if err := msg.Ack(); err != nil {
		log.Printf("Error acknowledging message (%s): %v", reason, err)
		// This is tricky, response was sent but ack failed. Might lead to duplicate processing.
	} else {
		log.Printf("Successfully processed and acknowledged message (%s)", reason)
	}
}

// nakMsg negatively acknowledges a NATS message for redelivery.
func nakMsg(msg *nats.Msg, reason string) {
	log.Printf("NAK'ing message (%s)", reason)
	// Nak to allow redelivery after default delay configured on consumer/stream
	if err := msg.Nak(); err != nil {
		log.Printf("Error sending NAK (%s): %v", reason, err)
	}
}
