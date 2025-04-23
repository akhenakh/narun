package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HttpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_gateway_requests_total",
			Help: "Total number of HTTP requests received.",
		},
		[]string{"method", "path", "status_code"}, // Add status_code label
	)

	HttpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_gateway_request_duration_seconds",
			Help:    "Histogram of HTTP request latencies.",
			Buckets: prometheus.DefBuckets, // Default buckets
		},
		[]string{"method", "path"},
	)

	NatsRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_gateway_nats_requests_total",
			Help: "Total number of NATS requests sent.",
		},
		[]string{"subject", "status"}, // Status: success, timeout, error
	)
)

// Status values for NatsRequestsTotal
const (
	StatusSuccess = "success"
	StatusTimeout = "timeout"
	StatusError   = "error"
)
