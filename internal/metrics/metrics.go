package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// --- HTTP Metrics ---
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

	// --- gRPC Metrics ---
	GrpcRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_gateway_requests_total",
			Help: "Total number of gRPC requests received.",
		},
		[]string{"grpc_method", "grpc_code"}, // Use gRPC method and code
	)

	GrpcRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_gateway_request_duration_seconds",
			Help:    "Histogram of gRPC request latencies.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"grpc_method"}, // Label by full gRPC method
	)

	// --- NATS Metrics (Common for both HTTP and gRPC gateways) ---
	NatsRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_nats_requests_total", // Renamed for clarity
			Help: "Total number of NATS requests sent by the gateway.",
		},
		[]string{"subject", "status"}, // Status: success, timeout, error
	)

	// --- Consumer Metrics (Unchanged) ---
	WorkerCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "narun_worker_active_count",
			Help: "Current number of active worker goroutines processing messages.",
		},
		[]string{"app", "stream"}, // Note: 'app'/'stream' might be less relevant for Micro, consider service_name?
	)

	RequestProcessingTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "narun_request_processing_seconds",
			Help:    "Time taken to process a request in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		// Consider standardizing labels here if possible, e.g., "service", "endpoint", "status"
		[]string{"app", "stream", "status"}, // Or service_name, endpoint, status for Micro?
	)
)

// Status values for NatsRequestsTotal
const (
	StatusSuccess = "success"
	StatusTimeout = "timeout"
	StatusError   = "error"
)
