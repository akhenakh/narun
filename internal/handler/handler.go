package handler

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	v1 "github.com/akhenakh/narun/gen/go/httprpc/v1"
	"github.com/akhenakh/narun/internal/config"
	"github.com/akhenakh/narun/internal/metrics"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// pendingReply tracks a pending NATS request/reply
type pendingReply struct {
	created time.Time
	doneCh  chan *nats.Msg
}

type HttpHandler struct {
	NatsConn *nats.Conn
	JsCtx    nats.JetStreamContext
	Config   *config.Config
	Logger   *slog.Logger

	// Reply management
	replyPrefix    string
	replySub       *nats.Subscription
	replyMu        sync.RWMutex
	pendingReplies map[string]*pendingReply
}

func NewHttpHandler(logger *slog.Logger, nc *nats.Conn, js nats.JetStreamContext, cfg *config.Config) *HttpHandler {
	h := &HttpHandler{
		NatsConn:       nc,
		JsCtx:          js,
		Config:         cfg,
		Logger:         logger,
		replyPrefix:    nats.NewInbox(),
		pendingReplies: make(map[string]*pendingReply),
	}

	// Create a single wildcard subscription for all replies
	var err error
	h.replySub, err = h.NatsConn.SubscribeSync(h.replyPrefix + ".*")
	if err != nil {
		logger.Error("Failed to create reply subscription", "error", err)
		// Continue anyway, we'll create individual subscriptions as fallback
	} else {
		logger.Info("Created shared reply subscription", "subject", h.replyPrefix+".*")

		// Start a goroutine to process replies
		go h.processReplies()
	}

	// Start cleanup goroutine
	go h.cleanupExpiredRequests()

	return h
}

// processReplies continuously processes incoming replies from the shared subscription
func (h *HttpHandler) processReplies() {
	for {
		msg, err := h.replySub.NextMsg(60 * time.Second) // Poll with timeout
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue // Just a timeout, keep processing
			}

			if errors.Is(err, nats.ErrBadSubscription) || errors.Is(err, nats.ErrConnectionClosed) {
				h.Logger.Error("Reply subscription terminated", "error", err)
				return // Exit if subscription is invalid
			}

			h.Logger.Warn("Error receiving from reply subscription", "error", err)
			continue
		}

		// Extract request ID from subject
		subject := msg.Subject
		if len(subject) <= len(h.replyPrefix)+1 { // +1 for the dot
			h.Logger.Warn("Received reply with invalid subject", "subject", subject)
			continue
		}

		requestID := subject[len(h.replyPrefix)+1:] // Skip prefix and dot

		// Find and signal the pending reply
		h.replyMu.Lock()
		pendingReq, exists := h.pendingReplies[requestID]
		if exists {
			delete(h.pendingReplies, requestID) // Remove from map
			h.replyMu.Unlock()

			// Send the message to the waiting goroutine
			select {
			case pendingReq.doneCh <- msg:
				// Successfully delivered
			default:
				h.Logger.Warn("Reply channel was closed", "requestID", requestID)
			}
		} else {
			h.replyMu.Unlock()
			h.Logger.Warn("Received reply for unknown request", "requestID", requestID)
		}
	}
}

// cleanupExpiredRequests periodically removes timed-out requests
func (h *HttpHandler) cleanupExpiredRequests() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		timeout := h.Config.RequestTimeout
		cutoff := time.Now().Add(-timeout)

		h.replyMu.Lock()
		for id, req := range h.pendingReplies {
			if req.created.Before(cutoff) {
				close(req.doneCh)
				delete(h.pendingReplies, id)
				h.Logger.Debug("Cleaned up expired request", "requestID", id)
			}
		}
		h.replyMu.Unlock()
	}
}

func (h *HttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	path := r.URL.Path
	method := r.Method

	routeCfg, found := h.Config.FindRoute(path, method)
	if !found {
		h.Logger.Info("No route found", "method", method, "path", path)
		http.NotFound(w, r)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusNotFound)).Inc()
		return
	}

	natsSubject := h.Config.GetNatsSubject(routeCfg)
	if natsSubject == "" {
		h.Logger.Error("Could not derive NATS subject", "method", method, "path", path, "app", routeCfg.App)
		http.Error(w, "Internal server error (configuration)", http.StatusInternalServerError)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
		return
	}

	requestID := uuid.NewString()

	// Use shared subscription if available
	var replySubject string
	var doneCh chan *nats.Msg
	var replySub *nats.Subscription
	var err error

	if h.replySub != nil && h.replySub.IsValid() {
		// Use the shared subscription approach
		replySubject = fmt.Sprintf("%s.%s", h.replyPrefix, requestID)
		doneCh = make(chan *nats.Msg, 1)

		// Register this request in our pending map
		h.replyMu.Lock()
		h.pendingReplies[requestID] = &pendingReply{
			created: time.Now(),
			doneCh:  doneCh,
		}
		h.replyMu.Unlock()
	} else {
		// Fallback to individual subscription if shared subscription failed
		replySubject = nats.NewInbox()
		replySub, err = h.NatsConn.SubscribeSync(replySubject)
		if err != nil {
			h.Logger.Error("Error subscribing to reply subject", "reply_subject", replySubject, "request_id", requestID, "error", err)
			http.Error(w, "Internal server error (NATS setup)", http.StatusInternalServerError)
			metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
			metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
			return
		}
		defer replySub.Unsubscribe()
		_ = replySub.AutoUnsubscribe(1)
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.Logger.Error("Error reading request body", "method", method, "path", path, "nats_subject", natsSubject, "request_id", requestID, "error", err)
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())

		// Clean up if using shared subscription
		if doneCh != nil {
			h.replyMu.Lock()
			delete(h.pendingReplies, requestID)
			close(doneCh)
			h.replyMu.Unlock()
		}
		return
	}
	defer r.Body.Close()

	// Prepare Protobuf Request using Setters
	protoReq := &v1.NatsHttpRequest{}
	protoReq.SetMethod(method)
	protoReq.SetPath(path)
	protoReq.SetQuery(r.URL.RawQuery)
	protoReq.SetProto(r.Proto)
	protoReq.SetBody(body)
	protoReq.SetRemoteAddr(r.RemoteAddr)
	protoReq.SetRequestUri(r.RequestURI)
	protoReq.SetHost(r.Host)
	protoReq.SetReplySubject(replySubject)
	protoReq.SetRequestId(requestID)

	// Populate headers
	headers := make(map[string]*v1.HeaderValues)
	for key, values := range r.Header {
		if len(values) > 0 {
			hv := new(v1.HeaderValues)
			hv.SetValues(values)
			headers[key] = hv
		}
	}
	protoReq.SetHeaders(headers)

	protoReqPayload, err := proto.Marshal(protoReq)
	if err != nil {
		h.Logger.Error("Error marshalling request to Protobuf", "request_id", requestID, "error", err)
		http.Error(w, "Internal server error (encoding)", http.StatusInternalServerError)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())

		// Clean up if using shared subscription
		if doneCh != nil {
			h.replyMu.Lock()
			delete(h.pendingReplies, requestID)
			close(doneCh)
			h.replyMu.Unlock()
		}
		return
	}

	h.Logger.Debug("Publishing JetStream request", "subject", natsSubject, "reply_subject", replySubject, "request_id", requestID)
	pubAck, err := h.JsCtx.Publish(natsSubject, protoReqPayload)

	var statusCode int
	var natsStatus string

	if err != nil {
		h.Logger.Error("JetStream publish error", "subject", natsSubject, "request_id", requestID, "error", err)
		http.Error(w, "Internal server error (NATS publish)", http.StatusInternalServerError)
		statusCode = http.StatusInternalServerError
		natsStatus = metrics.StatusError

		// Clean up if using shared subscription
		if doneCh != nil {
			h.replyMu.Lock()
			delete(h.pendingReplies, requestID)
			close(doneCh)
			h.replyMu.Unlock()
		}
	} else {
		h.Logger.Debug("JetStream publish acknowledged", "stream", pubAck.Stream, "seq", pubAck.Sequence, "request_id", requestID)

		// Get reply - different approaches based on subscription type
		var natsMsg *nats.Msg
		var err error

		if doneCh != nil {
			// Using shared subscription - wait on channel
			h.Logger.Debug("Waiting for reply via shared subscription", "request_id", requestID, "timeout", h.Config.RequestTimeout)
			select {
			case natsMsg = <-doneCh:
				// Got a reply
			case <-time.After(h.Config.RequestTimeout):
				err = nats.ErrTimeout

				// Clean up on timeout
				h.replyMu.Lock()
				delete(h.pendingReplies, requestID)
				close(doneCh)
				h.replyMu.Unlock()
			}
		} else {
			// Using individual subscription
			h.Logger.Debug("Waiting for reply via individual subscription", "reply_subject", replySubject, "request_id", requestID, "timeout", h.Config.RequestTimeout)
			natsMsg, err = replySub.NextMsg(h.Config.RequestTimeout)
		}

		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				h.Logger.Warn("NATS reply timeout", "reply_subject", replySubject, "request_id", requestID)
				http.Error(w, "Request timed out waiting for backend processor", http.StatusGatewayTimeout)
				statusCode = http.StatusGatewayTimeout
				natsStatus = metrics.StatusTimeout
			} else {
				h.Logger.Error("NATS reply subscription error", "reply_subject", replySubject, "request_id", requestID, "error", err)
				http.Error(w, "Internal server error (NATS reply)", http.StatusInternalServerError)
				statusCode = http.StatusInternalServerError
				natsStatus = metrics.StatusError
			}
		} else {
			natsStatus = metrics.StatusSuccess
			h.Logger.Debug("Received reply", "reply_subject", replySubject, "request_id", requestID)

			protoResp := &v1.NatsHttpResponse{}
			err = proto.Unmarshal(natsMsg.Data, protoResp)
			if err != nil {
				h.Logger.Error("Error unmarshalling Protobuf response", "reply_subject", replySubject, "request_id", requestID, "error", err)
				http.Error(w, "Internal server error (decoding response)", http.StatusInternalServerError)
				statusCode = http.StatusInternalServerError
			} else {
				respHeaders := protoResp.GetHeaders()
				for key, headerValues := range respHeaders {
					if headerValues != nil {
						for _, value := range headerValues.GetValues() {
							w.Header().Add(key, value)
						}
					}
				}

				statusCode = int(protoResp.GetStatusCode())
				if statusCode == 0 {
					statusCode = http.StatusOK
				}
				w.WriteHeader(statusCode)
				respBody := protoResp.GetBody()
				if respBody != nil {
					_, writeErr := w.Write(respBody)
					if writeErr != nil {
						h.Logger.Error("Error writing response body", "request_id", requestID, "error", writeErr)
					}
				}
			}
		}
	}

	metrics.NatsRequestsTotal.WithLabelValues(natsSubject, natsStatus).Inc()
	metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(statusCode)).Inc()
	metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
}
