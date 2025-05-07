package metrics

import (
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTP Metrics
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

	// gRPC Metrics
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

	// NATS Metrics (Common for both HTTP and gRPC gateways)
	NatsRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_nats_requests_total", // Renamed for clarity
			Help: "Total number of NATS requests sent by the gateway.",
		},
		[]string{"subject", "status"}, // Status: success, timeout, error
	)

	// Consumer Metrics
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

// Node Runner Instance Metrics
var (
	NarunNodeRunnerInstanceUp = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "narun_node_runner_instance_up",
			Help: "Indicates if a narun instance is currently considered up (1 for up, 0 for down).",
		},
		[]string{"app_name", "instance_id", "node_id"},
	)

	NarunNodeRunnerInstanceRestartsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "narun_node_runner_instance_restarts_total",
			Help: "Total number of times a narun instance has been restarted.",
		},
		[]string{"app_name", "instance_id", "node_id"},
	)

	NarunNodeRunnerInstanceMemoryMaxRSSBytes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "narun_node_runner_instance_memory_max_rss_bytes",
			Help: "Maximum resident set size (RSS) in bytes used by the instance's last run. On Linux, this is based on kilobytes from rusage.",
		},
		[]string{"app_name", "instance_id", "node_id"},
	)

	NarunNodeRunnerInstanceCPUUserSecondsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "narun_node_runner_instance_cpu_user_seconds_total",
			Help: "Total user CPU time in seconds consumed by the narun instance.",
		},
		[]string{"app_name", "instance_id", "node_id"},
	)

	NarunNodeRunnerInstanceCPUSystemSecondsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "narun_node_runner_instance_cpu_system_seconds_total",
			Help: "Total system CPU time in seconds consumed by the narun instance.",
		},
		[]string{"app_name", "instance_id", "node_id"},
	)

	NarunNodeRunnerInstanceInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "narun_node_runner_instance_info",
			Help: "Information about a narun instance. Value is always 1.",
		},
		[]string{"app_name", "instance_id", "node_id", "spec_tag", "spec_mode", "binary_path"},
	)

	NarunNodeRunnerInstanceLastExitCode = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "narun_node_runner_instance_last_exit_code",
			Help: "The exit code of the narun instance from its last termination.",
		},
		[]string{"app_name", "instance_id", "node_id"},
	)
)

// TimevalToSeconds Helper to convert syscall.Timeval to float64 seconds.
func TimevalToSeconds(tv syscall.Timeval) float64 {
	return float64(tv.Sec) + float64(tv.Usec)/1e6
}
