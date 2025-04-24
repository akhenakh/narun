package handler

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv" // For status code label
	"time"

	v1 "github.com/akhenakh/narun/gen/go/httprpc/v1" // Import generated proto types
	"github.com/akhenakh/narun/internal/config"
	"github.com/akhenakh/narun/internal/metrics"

	"github.com/google/uuid" // For request ID
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto" // Import proto package
)

type HttpHandler struct {
	NatsConn *nats.Conn
	JsCtx    nats.JetStreamContext // Add JetStream context
	Config   *config.Config
	Logger   *slog.Logger
}

func NewHttpHandler(logger *slog.Logger, nc *nats.Conn, js nats.JetStreamContext, cfg *config.Config) *HttpHandler {
	return &HttpHandler{
		NatsConn: nc,
		JsCtx:    js,
		Config:   cfg,
		Logger:   logger,
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

	// Create a single subscription with a wildcard pattern and use request IDs:
	// In handler initialization:
	h.replyPrefix = nats.NewInbox()
	h.replySub, err = h.NatsConn.SubscribeSync(h.replyPrefix + ".*")

	requestID := uuid.NewString()
	replySubject := nats.NewInbox()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.Logger.Error("Error reading request body", "method", method, "path", path, "nats_subject", natsSubject, "request_id", requestID, "error", err) // Added requestID
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return
	}
	defer r.Body.Close()

	replySub, err := h.NatsConn.SubscribeSync(replySubject)
	if err != nil {
		h.Logger.Error("Error subscribing to reply subject", "reply_subject", replySubject, "request_id", requestID, "error", err)
		http.Error(w, "Internal server error (NATS setup)", http.StatusInternalServerError)
		metrics.HttpRequestsTotal.WithLabelValues(method, path, strconv.Itoa(http.StatusInternalServerError)).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, path).Observe(time.Since(startTime).Seconds())
		return
	}
	defer replySub.Unsubscribe()
	_ = replySub.AutoUnsubscribe(1)

	// --- Prepare Protobuf Request using Setters ---
	protoReq := &v1.NatsHttpRequest{}
	protoReq.SetMethod(method)
	protoReq.SetPath(path)
	protoReq.SetQuery(r.URL.RawQuery)
	protoReq.SetProto(r.Proto)
	protoReq.SetBody(body) // Assumes body was read successfully
	protoReq.SetRemoteAddr(r.RemoteAddr)
	protoReq.SetRequestUri(r.RequestURI)
	protoReq.SetHost(r.Host)
	protoReq.SetReplySubject(replySubject)
	protoReq.SetRequestId(requestID)

	// Populate headers (map access is okay)
	headers := make(map[string]*v1.HeaderValues)

	// Populate headers
	for key, values := range r.Header {
		if len(values) > 0 {
			// Create a new HeaderValues instance
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
	} else {
		h.Logger.Debug("JetStream publish acknowledged", "stream", pubAck.Stream, "seq", pubAck.Sequence, "request_id", requestID)

		h.Logger.Debug("Waiting for reply", "reply_subject", replySubject, "request_id", requestID, "timeout", h.Config.RequestTimeout)
		natsMsg, err := replySub.NextMsg(h.Config.RequestTimeout)

		if err != nil {
			if errors.Is(err, nats.ErrTimeout) { // Use errors.Is for checking NATS errors
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
