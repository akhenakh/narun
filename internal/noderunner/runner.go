package noderunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/akhenakh/narun/internal/crypto"
	"github.com/akhenakh/narun/internal/metrics"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var runnerVersion = "0.2.1-dev" // Incremented version

// PrometheusTarget represents the JSON structure for HTTP SD.
type PrometheusTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

// NodeRunner manages application processes on a single node.
type NodeRunner struct {
	nodeID        string
	nc            *nats.Conn
	js            jetstream.JetStream
	kvAppConfigs  jetstream.KeyValue
	kvNodeStates  jetstream.KeyValue
	appBinaries   jetstream.ObjectStore
	fileStore     jetstream.ObjectStore // Add handle for file store
	state         *AppStateManager      // Updated state manager
	logger        *slog.Logger
	kvSecrets     jetstream.KeyValue // KV store for encrypted secrets
	masterKey     []byte             // Decoded master key
	dataDir       string
	version       string
	startTime     time.Time
	localOS       string // Detected OS
	localArch     string // Detected Arch
	globalCtx     context.Context
	globalCancel  context.CancelFunc
	shutdownWg    sync.WaitGroup
	metricsAddr   string       // Address for Prometheus metrics endpoint
	metricsServer *http.Server // HTTP server for metrics
	adminAddr     string       // Address for admin UI HTTP endpoint
	adminServer   *http.Server // HTTP server for admin UI
}

// NewNodeRunner creates and initializes a new NodeRunner.
func NewNodeRunner(nodeID, natsURL, nkeyFile, dataDir, masterKeyBase64, metricsAddr, adminAddr string, logger *slog.Logger) (*NodeRunner, error) {
	if nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("nodeID is required and could not get hostname: %w", err)
		}
		nodeID = hostname
	}
	if dataDir == "" {
		return nil, fmt.Errorf("data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", dataDir, err)
	}
	dataDirAbs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for data directory %s: %w", dataDir, err)
	}

	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	logger = logger.With("node_id", nodeID, "component", "node-runner")

	// Detect Local Platform
	localOS := runtime.GOOS
	localArch := runtime.GOARCH
	logger.Debug("Detected local platform", "os", localOS, "arch", localArch)

	var masterKey []byte
	if masterKeyBase64 != "" {
		masterKey, err = crypto.DeriveKeyFromBase64(masterKeyBase64)
		if err != nil {
			return nil, fmt.Errorf("invalid master key provided: %w", err)
		}
		logger.Info("Master key loaded successfully.")
	} else {
		logger.Warn("No master key provided (-master-key flag or NARUN_MASTER_KEY env). Secret decryption will not be possible.")
		// Allow running without a key, but secret resolution will fail
	}

	// NATS options
	natsOpts := []nats.Option{
		nats.Name(fmt.Sprintf("node-runner-%s", nodeID)),
		nats.ReconnectWait(2 * time.Second),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				logger.Error("NATS disconnected", "error", err)
			} else {
				logger.Info("NATS disconnected gracefully")
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			logger.Info("NATS connection closed")
		}),
	}

	if nkeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(nkeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load nkey seed from %s: %w", nkeyFile, err)
		}
		natsOpts = append(natsOpts, opt)
	}

	// NATS connection
	nc, err := nats.Connect(natsURL, natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS at %s: %w", natsURL, err)
	}
	logger.Debug("Connected to NATS", "url", natsURL)

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}
	logger.Debug("JetStream context created")

	// Bind to KV Stores
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer setupCancel()
	kvAppConfigs, err := js.CreateOrUpdateKeyValue(setupCtx, jetstream.KeyValueConfig{
		Bucket:      AppConfigKVBucket,
		Description: "Stores application service specifications for node runners.",
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to bind/create KV store '%s': %w", AppConfigKVBucket, err)
	}
	logger.Debug("Bound to App Config KV store", "bucket", AppConfigKVBucket)

	kvNodeStates, err := js.CreateOrUpdateKeyValue(setupCtx, jetstream.KeyValueConfig{
		Bucket: NodeStateKVBucket, Description: "Stores node runner states and heartbeats.", TTL: NodeStateKVTTL, History: 1,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to bind/create KV store '%s': %w", NodeStateKVBucket, err)
	}
	logger.Debug("Bound to Node State KV store", "bucket", NodeStateKVBucket, "ttl", NodeStateKVTTL)

	// Bind to Secret KV Store
	kvSecrets, err := js.CreateOrUpdateKeyValue(setupCtx, jetstream.KeyValueConfig{
		Bucket:      SecretKVBucket,
		Description: "Stores encrypted application secrets.",
		History:     1, // Only keep latest version of secret
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to bind/create KV store '%s': %w", SecretKVBucket, err)
	}
	logger.Debug("Bound to Secret KV store", "bucket", SecretKVBucket)

	// Bind to File Object Store
	fileStore, err := js.CreateOrUpdateObjectStore(setupCtx, jetstream.ObjectStoreConfig{
		Bucket: FileOSBucket, Description: "Shared files for Narun applications",
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to bind/create Object store '%s': %w", FileOSBucket, err)
	}
	logger.Info("Bound to File store", "bucket", FileOSBucket)

	// Bind to Object Store
	osBucket, err := js.CreateOrUpdateObjectStore(setupCtx, jetstream.ObjectStoreConfig{
		Bucket: AppBinariesOSBucket, Description: "Stores application binaries.",
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to bind/create Object store '%s': %w", AppBinariesOSBucket, err)
	}
	logger.Info("Bound to Object store", "bucket", AppBinariesOSBucket)

	gCtx, gCancel := context.WithCancel(context.Background())
	runnerStartTime := time.Now()

	runner := &NodeRunner{
		nodeID:       nodeID,
		nc:           nc,
		js:           js,
		kvAppConfigs: kvAppConfigs,
		kvNodeStates: kvNodeStates,
		kvSecrets:    kvSecrets, // Store handle
		masterKey:    masterKey, // Store decoded key
		fileStore:    fileStore, // Store handle for file store
		appBinaries:  osBucket,
		state:        NewAppStateManager(),
		logger:       logger,
		dataDir:      dataDirAbs,
		version:      runnerVersion,
		startTime:    runnerStartTime,
		localOS:      localOS,   // Store detected OS
		localArch:    localArch, //  Store detected Arch
		metricsAddr:  metricsAddr,
		adminAddr:    adminAddr, // Store adminAddr
		globalCtx:    gCtx,
		globalCancel: gCancel,
	}

	return runner, nil
}

// Run starts the node runner's main loop.
func (nr *NodeRunner) Run() error {
	nr.logger.Info("Starting node runner", "version", nr.version, "os", nr.localOS, "arch", nr.localArch)
	defer nr.logger.Info("Node runner stopped")

	// Start Metrics Server if address is configured
	if nr.metricsAddr != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		metricsMux.HandleFunc("/discovery", nr.handleDiscovery)
		nr.metricsServer = &http.Server{
			Addr:              nr.metricsAddr,
			Handler:           metricsMux,
			ReadHeaderTimeout: 5 * time.Second, // Example timeout
		}
		nr.shutdownWg.Add(1) // Add to wait group for metrics server
		go func() {
			defer nr.shutdownWg.Done()
			nr.logger.Info("Starting Prometheus metrics server", "address", nr.metricsAddr)
			if err := nr.metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				nr.logger.Error("Prometheus metrics server failed", "error", err)
				nr.globalCancel() // Critical failure, stop the runner
			} else {
				nr.logger.Info("Prometheus metrics server shut down")
			}
		}()
	}

	// Start Admin UI Server if address is configured
	if nr.adminAddr != "" {
		adminMux := http.NewServeMux()
		adminMux.HandleFunc("/", nr.handleAdminUI) // Root handler for admin UI
		nr.adminServer = &http.Server{
			Addr:              nr.adminAddr,
			Handler:           adminMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		nr.shutdownWg.Add(1) // Add to wait group for admin server
		go func() {
			defer nr.shutdownWg.Done()
			nr.logger.Info("Starting admin UI server", "address", nr.adminAddr)
			if err := nr.adminServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				nr.logger.Error("Admin UI server failed", "error", err)
				nr.globalCancel() // Critical failure, stop the runner
			} else {
				nr.logger.Info("Admin UI server shut down")
			}
		}()
	}

	// Initial Registration and Heartbeat Loop
	if err := nr.updateNodeState("running"); err != nil {
		nr.logger.Error("Initial node state registration failed", "error", err)
	} else {
		nr.logger.Info("Node registered successfully")
	}
	nr.shutdownWg.Add(1)
	go nr.heartbeatLoop()
	nr.logger.Info("Heartbeat loop started", "interval", NodeHeartbeatInterval)

	// Initial sync
	syncCtx, syncCancel := context.WithTimeout(nr.globalCtx, 60*time.Second)
	if err := nr.syncAllApps(syncCtx); err != nil {
		nr.logger.Error("Failed initial app synchronization", "error", err)
	} else {
		nr.logger.Info("Initial app synchronization complete")
	}
	syncCancel()

	// Watch for configuration changes
	watcher, err := nr.kvAppConfigs.WatchAll(nr.globalCtx) // Watch all operations, including deletes
	if err != nil {
		if nr.globalCtx.Err() != nil {
			nr.logger.Info("Failed to start KV watcher due to shutdown signal")
			return nr.globalCtx.Err()
		}
		return fmt.Errorf("failed to start KV watcher on '%s': %w", AppConfigKVBucket, err)
	}
	defer watcher.Stop() // Ensure watcher is stopped on exit from Run
	nr.logger.Info("Started watching for app configuration changes", "bucket", AppConfigKVBucket)

	// Watcher Loop
	nr.shutdownWg.Add(1)
	go func() {
		defer nr.shutdownWg.Done()
		for {
			select {
			case <-nr.globalCtx.Done():
				nr.logger.Info("Configuration watcher stopping due to context cancellation.")
				if err := watcher.Stop(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) && !errors.Is(err, context.Canceled) {
					nr.logger.Warn("Error stopping KV watcher during shutdown", "error", err)
				}
				return
			case entry, ok := <-watcher.Updates():
				if !ok {
					nr.logger.Warn("KV Watcher updates channel closed.")
					if nr.globalCtx.Err() == nil { // Only error if not already shutting down
						nr.logger.Error("Watcher closed unexpectedly while runner context is still active.")
						nr.globalCancel() // Trigger shutdown if watcher fails unexpectedly
					} else {
						nr.logger.Info("Watcher closed during shutdown.")
					}
					return
				}
				if entry == nil {
					nr.logger.Debug("Received nil update from watcher")
					continue
				}
				nr.handleAppConfigUpdate(nr.globalCtx, entry) // Pass context
			}
		}
	}()

	// Wait for shutdown signal
	<-nr.globalCtx.Done()
	nr.logger.Info("Shutdown signal received, initiating shutdown...")

	// Update node state
	if err := nr.updateNodeState("shutting_down"); err != nil {
		nr.logger.Warn("Failed to update node state to shutting_down", "error", err)
	}

	// Initiate shutdown of managed apps
	nr.shutdownAllAppInstances()

	// Stop watcher (if not already stopped by goroutine exit)
	// This needs to be idempotent or check globalCtx.Done()
	if nr.globalCtx.Err() == nil { // If not already stopped by context cancellation in the goroutine
		if err := watcher.Stop(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) && !errors.Is(err, context.Canceled) {
			nr.logger.Warn("Error stopping watcher during explicit shutdown phase", "error", err)
		}
	}

	// Shutdown metrics server
	if nr.metricsServer != nil {
		nr.logger.Info("Shutting down Prometheus metrics server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := nr.metricsServer.Shutdown(shutdownCtx); err != nil {
			nr.logger.Error("Error shutting down Prometheus metrics server", "error", err)
		} else {
			nr.logger.Info("Prometheus metrics server gracefully stopped.")
		}
	}

	// Shutdown admin UI server
	if nr.adminServer != nil {
		nr.logger.Info("Shutting down admin UI server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := nr.adminServer.Shutdown(shutdownCtx); err != nil {
			nr.logger.Error("Error shutting down admin UI server", "error", err)
		} else {
			nr.logger.Info("Admin UI server gracefully stopped.")
		}
	}

	// Wait for background goroutines (heartbeat, watcher, metrics server, admin server if running)
	nr.logger.Debug("Waiting for background goroutines to finish...")
	nr.shutdownWg.Wait()
	nr.logger.Debug("Background goroutines finished.")

	// Delete node state
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deleteCancel()
	nr.logger.Info("Deleting node state from KV store", "key", nr.nodeID)
	if err := nr.kvNodeStates.Delete(deleteCtx, nr.nodeID); err != nil {
		nr.logger.Error("Failed to delete node state from KV", "key", nr.nodeID, "error", err)
	}

	// Drain NATS connection
	if nr.nc != nil && !nr.nc.IsClosed() {
		nr.logger.Info("Draining NATS connection...")
		drainTimeout := 10 * time.Second
		drainDone := make(chan error, 1)
		go func() { drainDone <- nr.nc.Drain() }()
		select {
		case err := <-drainDone:
			if err != nil {
				nr.logger.Error("Error during NATS connection drain", "error", err)
				nr.nc.Close()
			} else {
				nr.logger.Info("NATS connection drained successfully.")
			}
		case <-time.After(drainTimeout):
			nr.logger.Warn("NATS connection drain timed out. Forcing close.", "timeout", drainTimeout)
			nr.nc.Close()
		}
		nr.logger.Info("NATS connection closed.")
	}

	// Return context error if any
	if err := nr.globalCtx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("runner stopped due to context error: %w", err)
	}
	return nil
}

// handleAdminUI serves the admin interface HTML.
func (nr *NodeRunner) handleAdminUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Basic HTML structure and styling
	fmt.Fprintf(w, `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="10">
    <title>Narun Node Runner Admin - %s</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol"; margin: 20px; background-color: #f8f9fa; color: #212529; line-height: 1.6; }
        h1, h2 { color: #007bff; border-bottom: 2px solid #007bff; padding-bottom: 0.3em; }
        table { width: 100%%; border-collapse: collapse; margin-bottom: 25px; background-color: #fff; box-shadow: 0 2px 15px rgba(0,0,0,0.05); border-radius: 5px; overflow: hidden; }
        th, td { border: 1px solid #dee2e6; padding: 12px 15px; text-align: left; }
        th { background-color: #007bff; color: white; font-weight: 600; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        tr:hover { background-color: #e9ecef; }
        .status-running { color: #28a745; font-weight: bold; }
        .status-starting { color: #ffc107; font-weight: bold; }
        .status-stopping { color: #fd7e14; font-weight: bold; }
        .status-stopped { color: #6c757d; }
        .status-failed, .status-crashed { color: #dc3545; font-weight: bold; }
        .container { max-width: 1400px; margin: auto; padding: 0 15px; }
        .header-info { background-color: #e9ecef; padding: 20px; margin-bottom: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.03); }
        .header-info p { margin: 0.5em 0; }
        .footer { text-align: center; margin-top: 30px; font-size: 0.9em; color: #6c757d; }
		.code { font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, Courier, monospace; background-color: #eef; padding: 2px 4px; border-radius: 3px; font-size: 0.9em; }
    </style>
</head>
<body>
<div class="container">
    <h1>Narun Node Runner Admin</h1>
    <div class="header-info">
        <p><strong>Node ID:</strong> <span class="code">%s</span></p>
        <p><strong>Runner Version:</strong> <span class="code">%s</span></p>
        <p><strong>Runner Started:</strong> %s (Uptime: %s)</p>
        <p><strong>Current Time:</strong> %s</p>
    </div>
`, html.EscapeString(nr.nodeID), html.EscapeString(nr.nodeID), html.EscapeString(nr.version),
		nr.startTime.Format(time.RFC1123), time.Since(nr.startTime).Truncate(time.Second).String(),
		time.Now().Format(time.RFC1123))

	allAppInfos := nr.state.GetAllAppInfos()
	if len(allAppInfos) == 0 {
		fmt.Fprintf(w, "<p>No applications are currently managed by this node.</p>")
	}

	appNames := make([]string, 0, len(allAppInfos))
	for name := range allAppInfos {
		appNames = append(appNames, name)
	}
	sort.Strings(appNames) // Sort app names for consistent display

	// Temporary struct to hold a snapshot of instance data for safe rendering
	type uiInstanceView struct {
		InstanceID   string
		RunID        string
		Status       AppStatus
		StatusClass  string
		PID          string
		StartTime    string
		Uptime       string
		RestartCount int
		ExitCode     string
		BinaryPath   string
	}

	for _, appName := range appNames {
		appInfo := allAppInfos[appName]
		appInfo.mu.RLock() // Read lock for accessing this app's info

		fmt.Fprintf(w, "<h2>Application: <span class=\"code\">%s</span></h2>", html.EscapeString(appName))

		if appInfo.spec == nil {
			fmt.Fprintf(w, "<p><em>No specification loaded for this application (possibly deleted or in error state).</em></p>")
			appInfo.mu.RUnlock()
			continue
		}

		fmt.Fprintf(w, "<p><strong>Tag:</strong> <span class=\"code\">%s</span> | <strong>Mode:</strong> <span class=\"code\">%s</span> | <strong>Config Hash:</strong> <span class=\"code\">%s</span></p>",
			html.EscapeString(appInfo.spec.Tag), html.EscapeString(appInfo.spec.Mode), html.EscapeString(appInfo.configHash))

		instanceViews := make([]uiInstanceView, 0, len(appInfo.instances))
		if len(appInfo.instances) == 0 {
			fmt.Fprintf(w, "<p><em>No instances running for this application on this node.</em></p>")
		} else {
			// Create a snapshot of instance data under lock
			tempSortedInstances := make([]*ManagedApp, len(appInfo.instances))
			copy(tempSortedInstances, appInfo.instances)
			// Sort the copy of instances by ID for consistent display
			sort.Slice(tempSortedInstances, func(i, j int) bool {
				return tempSortedInstances[i].InstanceID < tempSortedInstances[j].InstanceID
			})

			for _, instance := range tempSortedInstances {
				pidStr := "-"
				if instance.Pid > 0 {
					pidStr = fmt.Sprintf("%d", instance.Pid)
				}
				startTimeStr := "-"
				uptimeStr := "-"
				if !instance.StartTime.IsZero() {
					startTimeStr = instance.StartTime.Format(time.RFC1123)
					if instance.Status == StatusRunning || instance.Status == StatusStarting {
						uptimeStr = time.Since(instance.StartTime).Truncate(time.Second).String()
					}
				}
				exitCodeStr := "-"
				if instance.lastExitCode != nil {
					exitCodeStr = fmt.Sprintf("%d", *instance.lastExitCode)
				}
				instanceViews = append(instanceViews, uiInstanceView{
					InstanceID:   instance.InstanceID,
					RunID:        instance.RunID,
					Status:       instance.Status,
					StatusClass:  "status-" + strings.ToLower(string(instance.Status)),
					PID:          pidStr,
					StartTime:    startTimeStr,
					Uptime:       uptimeStr,
					RestartCount: instance.restartCount,
					ExitCode:     exitCodeStr,
					BinaryPath:   instance.BinaryPath,
				})
			}
		}
		appInfo.mu.RUnlock() // Unlock after all data for this app is copied

		// Render from the snapshot (instanceViews)
		if len(instanceViews) > 0 {
			fmt.Fprintf(w, `
    <table>
        <thead>
            <tr>
                <th>Instance ID</th>
                <th>Run ID</th>
                <th>Status</th>
                <th>PID</th>
                <th>Started</th>
                <th>Uptime</th>
                <th>Restarts</th>
                <th>Exit Code</th>
                <th>Binary Path</th>
            </tr>
        </thead>
        <tbody>`)
			for _, view := range instanceViews {
				fmt.Fprintf(w, `
            <tr>
                <td><span class="code">%s</span></td>
                <td><span class="code">%s</span></td>
                <td class="%s">%s</td>
                <td>%s</td>
                <td>%s</td>
                <td>%s</td>
                <td>%d</td>
                <td>%s</td>
                <td><span class="code">%s</span></td>
            </tr>`,
					html.EscapeString(view.InstanceID), html.EscapeString(view.RunID),
					view.StatusClass, html.EscapeString(string(view.Status)),
					view.PID, view.StartTime, view.Uptime, view.RestartCount, view.ExitCode, html.EscapeString(view.BinaryPath))
			}
			fmt.Fprintf(w, `</tbody></table>`)
		}
	}

	fmt.Fprintf(w, `
    <div class="footer">
        <p>Narun Node Runner Admin UI &copy; %d</p>
    </div>
</div>
</body>
</html>`, time.Now().Year())
}

// heartbeatLoop periodically updates the node's state in the KV store.
func (nr *NodeRunner) heartbeatLoop() {
	defer nr.shutdownWg.Done()
	ticker := time.NewTicker(NodeHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := nr.updateNodeState("running"); err != nil {
				nr.logger.Warn("Failed to send heartbeat update", "error", err)
			} else {
				nr.logger.Debug("Heartbeat sent successfully")
			}
		case <-nr.globalCtx.Done():
			nr.logger.Info("Heartbeat loop stopping due to context cancellation.")
			return
		}
	}
}

// handleDiscovery serves the targets list for Prometheus HTTP SD
func (nr *NodeRunner) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	targets := []PrometheusTarget{}

	// Get snapshot of apps
	allAppInfos := nr.state.GetAllAppInfos()

	// Attempt to get Host IP (simple approach, might need config if multi-homed)
	hostIP := "127.0.0.1"
	if conn, err := net.Dial("udp", "8.8.8.8:80"); err == nil {
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		hostIP = localAddr.IP.String()
		conn.Close()
	}

	for appName, info := range allAppInfos {
		info.mu.RLock()
		spec := info.spec

		// Skip if spec missing or metrics disabled
		if spec == nil || spec.Metrics.Port == 0 {
			info.mu.RUnlock()
			continue
		}

		for _, inst := range info.instances {
			if inst.Status == StatusRunning && inst.HostMetricsPort > 0 {
				target := PrometheusTarget{
					Targets: []string{fmt.Sprintf("%s:%d", hostIP, inst.HostMetricsPort)},
					Labels: map[string]string{
						"job":               "narun-workload",
						"narun_app":         appName,
						"narun_instance_id": inst.InstanceID,
						"narun_node_id":     nr.nodeID,
						"narun_tag":         spec.Tag,
						"__metrics_path__":  spec.Metrics.Path, // Prometheus uses this to scrape specific path
					},
				}
				targets = append(targets, target)
			}
		}
		info.mu.RUnlock()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(targets)
}

// updateNodeState constructs the current node state and puts it into the KV store.
func (nr *NodeRunner) updateNodeState(status string) error {
	allAppInfos := nr.state.GetAllAppInfos()
	managedInstanceIDs := make([]string, 0)
	for _, info := range allAppInfos {
		info.mu.RLock()
		for _, instance := range info.instances {
			managedInstanceIDs = append(managedInstanceIDs, instance.InstanceID)
		}
		info.mu.RUnlock()
	}
	sort.Strings(managedInstanceIDs)

	state := NodeState{
		NodeID:           nr.nodeID,
		LastSeen:         time.Now(),
		Version:          nr.version,
		StartTime:        nr.startTime,
		ManagedInstances: managedInstanceIDs,
		Status:           status,
		OS:               nr.localOS,   // Include OS
		Arch:             nr.localArch, // Include Arch
	}

	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal node state: %w", err)
	}

	putCtx, putCancel := context.WithTimeout(context.Background(), NodeHeartbeatInterval/2)
	defer putCancel()
	_, err = nr.kvNodeStates.Put(putCtx, nr.nodeID, stateData)
	if err != nil {
		return fmt.Errorf("failed to put node state to KV '%s': %w", NodeStateKVBucket, err)
	}
	return nil
}

// syncAllApps gets all current configurations and ensures apps are running.
func (nr *NodeRunner) syncAllApps(ctx context.Context) error {
	nr.logger.Info("Performing initial synchronization of all applications...")
	keysLister, err := nr.kvAppConfigs.ListKeys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			nr.logger.Info("No existing app configurations found in KV store.")
			return nil
		}
		return fmt.Errorf("failed to start listing keys from KV store '%s': %w", AppConfigKVBucket, err)
	}
	defer keysLister.Stop()

	// Keep track of keys seen during sync
	keysSeen := make(map[string]bool)
	syncWg := sync.WaitGroup{}
	errChan := make(chan error, 1) // Buffer of 1 to catch first error

	keysChan := keysLister.Keys()

syncLoop:
	for {
		select {
		case <-ctx.Done():
			nr.logger.Warn("Initial synchronization canceled or timed out.", "error", ctx.Err())
			keysLister.Stop() // Ensure lister is stopped on timeout/cancel
			// Wait for any already running goroutines before returning
			syncWg.Wait()
			close(errChan) // Close errChan after all goroutines are done
			// Drain errChan to prevent goroutine leak if error was sent
			for range errChan {
			}
			return ctx.Err()
		case key, ok := <-keysChan:
			if !ok { // Channel closed
				break syncLoop
			}
			if key == "" {
				continue
			}
			keysSeen[key] = true // Mark key as present in KV

			syncWg.Add(1)
			go func(k string) {
				defer syncWg.Done()
				entryCtx, entryCancel := context.WithTimeout(ctx, 20*time.Second) // Shorter timeout for individual GET
				defer entryCancel()

				entry, getErr := nr.kvAppConfigs.Get(entryCtx, k)
				if getErr != nil {
					if errors.Is(getErr, jetstream.ErrKeyNotFound) {
						// This case should be rare if ListKeys is consistent, but handle defensively
						nr.logger.Warn("App config key disappeared during sync, treating as deleted", "key", k)
						nr.handleAppConfigUpdate(entryCtx, &simulatedDeleteEntry{key: k, bucket: AppConfigKVBucket})
					} else {
						nr.logger.Error("Failed to get KV entry during sync", "key", k, "error", getErr)
						select {
						case errChan <- fmt.Errorf("sync failed getting key %s: %w", k, getErr):
						default: // errChan might be full or closed
						}
					}
					return
				}
				nr.handleAppConfigUpdate(entryCtx, entry) // Process the update
			}(key)
		}
	}

	syncWg.Wait()
	close(errChan) // Close errChan after all processing goroutines are done

	if firstErr := <-errChan; firstErr != nil { // Read the first error if any
		return firstErr // Return the first processing error encountered
	}

	// Pruning Phase
	nr.logger.Info("Pruning locally managed apps not found in KV store...")
	allLocalApps := nr.state.GetAllAppInfos()
	for appName, appInfo := range allLocalApps {
		if !keysSeen[appName] { // If this app wasn't in the KV keys list...
			nr.logger.Info("Pruning app no longer in KV config", "app", appName)
			// Lock the appInfo for stopping and deleting
			appInfo.mu.Lock()
			currentInstances := append([]*ManagedApp{}, appInfo.instances...) // Copy for iteration
			nr.stopAllInstancesForApp(ctx, appInfo, nr.logger.With("app", appName))
			// Clear state within appInfo
			appInfo.spec = nil
			appInfo.configHash = ""
			appInfo.instances = make([]*ManagedApp, 0)

			// Cleanup metrics for all instances of this pruned app
			for _, inst := range currentInstances {
				nr.cleanupInstanceMetrics(inst)
			}
			appInfo.mu.Unlock()
			// Remove the app entry from the state manager entirely
			nr.state.DeleteApp(appName)
		}
	}
	nr.logger.Info("App pruning complete.")

	return nil
}

// handleAppConfigUpdate processes a single KV entry update (PUT or DELETE).
func (nr *NodeRunner) handleAppConfigUpdate(ctx context.Context, entry jetstream.KeyValueEntry) {
	if entry.Bucket() != AppConfigKVBucket {
		nr.logger.Warn("Received KV update from unexpected bucket", "bucket", entry.Bucket())
		return
	}
	appName := entry.Key()
	// Create logger early
	logger := nr.logger.With("app", appName)
	if entry.Operation() != jetstream.KeyValueDelete && entry.Operation() != jetstream.KeyValuePurge { // Avoid logging revision for deletes
		logger = logger.With("kv_revision", entry.Revision())
	}
	logger = logger.With("kv_operation", entry.Operation().String())

	appInfo := nr.state.GetAppInfo(appName) // Gets or creates appInfo struct
	appInfo.mu.Lock()                       // Lock the specific app's state for processing
	defer appInfo.mu.Unlock()

	switch entry.Operation() {
	case jetstream.KeyValuePut:
		logger.Info("Processing configuration update")
		spec, err := ParseServiceSpec(entry.Value())
		if err != nil {
			logger.Error("Failed to parse service spec from KV", "error", err)
			instancesToCleanup := append([]*ManagedApp{}, appInfo.instances...)
			nr.stopAllInstancesForApp(ctx, appInfo, logger)
			for _, inst := range instancesToCleanup {
				nr.cleanupInstanceMetrics(inst)
			}
			appInfo.spec = nil
			appInfo.configHash = ""
			return
		}
		if spec.Name != appName {
			logger.Error("Service name in spec does not match KV key", "spec_name", spec.Name, "key", appName)
			instancesToCleanup := append([]*ManagedApp{}, appInfo.instances...)
			nr.stopAllInstancesForApp(ctx, appInfo, logger)
			for _, inst := range instancesToCleanup {
				nr.cleanupInstanceMetrics(inst)
			}
			appInfo.spec = nil
			appInfo.configHash = ""
			return
		}

		newConfigHash, err := calculateSpecHash(spec)
		if err != nil {
			logger.Error("Failed to hash service spec", "error", err)
			instancesToCleanup := append([]*ManagedApp{}, appInfo.instances...)
			nr.stopAllInstancesForApp(ctx, appInfo, logger)
			for _, inst := range instancesToCleanup {
				nr.cleanupInstanceMetrics(inst)
			}
			appInfo.spec = nil
			appInfo.configHash = ""
			return
		}

		// Update spec and hash in appInfo
		configHashChanged := appInfo.configHash != newConfigHash
		appInfo.spec = spec // Store the parsed spec
		appInfo.configHash = newConfigHash

		// Determine target replicas for *this* node
		targetReplicas := spec.FindTargetReplicas(nr.nodeID)
		currentReplicas := len(appInfo.instances)
		logger.Info("Reconciling instances", "target", targetReplicas, "current", currentReplicas, "hash_changed", configHashChanged)

		if configHashChanged {
			logger.Info("Configuration hash changed, restarting all instances")
			instancesToCleanup := append([]*ManagedApp{}, appInfo.instances...)
			nr.stopAllInstancesForApp(ctx, appInfo, logger) // This stops them
			// Metrics are cleaned up by monitorAppInstance's defer logic after intentional stop or by explicit cleanup below
			for _, inst := range instancesToCleanup {
				// Explicitly cleanup metrics for instances stopped due to config change,
				// as monitorAppInstance might not trigger full cleanup if intentionalStop is true.
				nr.cleanupInstanceMetrics(inst)
			}
			appInfo.instances = make([]*ManagedApp, 0, targetReplicas)
			currentReplicas = 0 // Reset current count
		}

		//  Adjust Replicas
		if targetReplicas > currentReplicas {
			needed := targetReplicas - currentReplicas
			logger.Info("Scaling up instances", "needed", needed)
			for i := 0; i < needed; i++ {
				replicaIndex := findNextReplicaIndex(appInfo.instances, targetReplicas)
				if replicaIndex == -1 {
					logger.Error("Could not find available replica index slot", "target", targetReplicas, "current_count", len(appInfo.instances))
					break
				}
				// Pass Tag implicitly via appInfo.spec to startAppInstance
				if err := nr.startAppInstance(ctx, appInfo, replicaIndex); err != nil {
					logger.Error("Failed to start new instance during scale up", "replica_index", replicaIndex, "error", err)
				} else {
					time.Sleep(100 * time.Millisecond)
				}
			}
		} else if targetReplicas < currentReplicas {
			excess := currentReplicas - targetReplicas
			logger.Info("Scaling down instances", "excess", excess)
			sortedInstances := append([]*ManagedApp{}, appInfo.instances...)
			sort.Slice(sortedInstances, func(i, j int) bool {
				idxI := extractReplicaIndex(sortedInstances[i].InstanceID)
				idxJ := extractReplicaIndex(sortedInstances[j].InstanceID)
				return idxI > idxJ // Sort descending by index to stop higher indices first
			})

			instancesToStop := make([]*ManagedApp, 0, excess)
			if excess <= len(sortedInstances) {
				instancesToStop = sortedInstances[:excess]
			} else {
				logger.Warn("Excess count greater than sorted instances, stopping all.", "excess", excess, "count", len(sortedInstances))
				instancesToStop = sortedInstances
			}

			remainingInstancesMap := make(map[string]*ManagedApp)
			for _, inst := range appInfo.instances {
				remainingInstancesMap[inst.InstanceID] = inst
			}

			for _, instToStop := range instancesToStop {
				logger.Info("Stopping excess instance", "instance_id", instToStop.InstanceID)
				// Stop synchronously for scale down to ensure metrics are cleaned up properly after stop
				if err := nr.stopAppInstance(instToStop, true); err != nil { // Intentional stop
					logger.Error("Error stopping excess instance", "instance_id", instToStop.InstanceID, "error", err)
				}
				// The monitorAppInstance will run its defer block. If it's an intentional stop,
				// it will set status to Stopped. We then explicitly clean up metrics.
				nr.cleanupInstanceMetrics(instToStop)
				delete(remainingInstancesMap, instToStop.InstanceID)
			}
			// Rebuild appInfo.instances from remainingInstancesMap
			newInstancesSlice := make([]*ManagedApp, 0, len(remainingInstancesMap))
			for _, inst := range remainingInstancesMap {
				newInstancesSlice = append(newInstancesSlice, inst)
			}
			appInfo.instances = newInstancesSlice // Update the main slice
		} else {
			// target == current AND hash didn't change
			if targetReplicas == 0 {
				logger.Info("Target replica count is 0. Ensuring no instances are running.")
				if len(appInfo.instances) > 0 {
					instancesToCleanup := append([]*ManagedApp{}, appInfo.instances...)
					nr.stopAllInstancesForApp(ctx, appInfo, logger)
					for _, inst := range instancesToCleanup {
						nr.cleanupInstanceMetrics(inst)
					}
					appInfo.instances = make([]*ManagedApp, 0)
				}
			} else {
				logger.Info("Instance count matches target and config hash unchanged. No action needed.")
			}
		}

	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		logger.Info("Processing configuration delete/purge request")
		instancesToCleanup := append([]*ManagedApp{}, appInfo.instances...)
		nr.stopAllInstancesForApp(ctx, appInfo, logger) // Stop all instances
		for _, inst := range instancesToCleanup {
			nr.cleanupInstanceMetrics(inst)
		}
		appInfo.spec = nil // Clear spec
		appInfo.configHash = ""
		appInfo.instances = make([]*ManagedApp, 0) // Clear instances slice
		// Delete the app entry from the state manager itself upon delete/purge
		nr.state.DeleteApp(appName) // Call this *after* lock is released by defer

	default:
		logger.Warn("Ignoring unknown KV operation")
	}
}

// fetchAndPlaceFile downloads a file from the file OS bucket and places it
// at the target path within the instance's working directory.
// It verifies hashes to avoid unnecessary downloads.
func (nr *NodeRunner) fetchAndPlaceFile(ctx context.Context, objectName, instanceWorkDir, targetRelativePath string, logger *slog.Logger) error {
	logger = logger.With("mount_object", objectName, "mount_path", targetRelativePath)
	logger.Info("Fetching and placing file mount")

	// Get Object Info
	objInfo, getInfoErr := nr.fileStore.GetInfo(ctx, objectName)
	if getInfoErr != nil {
		if errors.Is(getInfoErr, jetstream.ErrObjectNotFound) {
			return fmt.Errorf("source file object '%s' not found in object store '%s': %w", objectName, FileOSBucket, getInfoErr)
		}
		return fmt.Errorf("failed to get info for file object '%s': %w", objectName, getInfoErr)
	}

	expectedDigest := objInfo.Digest
	if expectedDigest == "" {
		return fmt.Errorf("file object '%s' info is missing digest", objectName)
	}

	// Determine Local Path
	// Ensure targetRelativePath is clean and doesn't escape instanceWorkDir
	cleanRelativePath := filepath.Clean(targetRelativePath)
	if strings.HasPrefix(cleanRelativePath, "..") || filepath.IsAbs(cleanRelativePath) {
		return fmt.Errorf("invalid target mount path '%s': must be relative and within the work directory", targetRelativePath)
	}
	localPath := filepath.Join(instanceWorkDir, cleanRelativePath)
	localDir := filepath.Dir(localPath)

	// Check Local File and Hash
	_, statErr := os.Stat(localPath)
	if statErr == nil {
		// File exists, verify hash
		hashMatches, verifyErr := verifyLocalFileHash(localPath, expectedDigest)
		if verifyErr == nil && hashMatches {
			logger.Info("Local file mount exists and hash verified.", "path", localPath)
			return nil // Success, local copy is up-to-date
		}
		logger.Warn("Local file mount exists but hash verification failed or errored. Re-downloading.", "path", localPath, "verify_err", verifyErr)
	} else if !os.IsNotExist(statErr) {
		// Error stating the file other than "not found"
		return fmt.Errorf("failed to stat local file mount path %s: %w", localPath, statErr)
	} else {
		logger.Info("Local file mount not found.", "path", localPath)
	}

	// Download Needed
	logger.Info("Downloading file mount object", "object", objectName, "local_path", localPath)
	if err := os.MkdirAll(localDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory for mount path %s: %w", localDir, err)
	}

	// Use unique temp file name within the target directory
	tempLocalPath := localPath + ".tmp." + fmt.Sprintf("%d", time.Now().UnixNano())
	outFile, err := os.Create(tempLocalPath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file %s: %w", tempLocalPath, err)
	}

	var finalErr error // To capture error for defer cleanup
	defer func() {
		if outFile != nil {
			outFile.Close() // Ensure closed before potential remove
		}
		// Remove temp file if an error occurred during download/verify/rename
		if finalErr != nil {
			logger.Warn("Removing temporary mount file due to error", "path", tempLocalPath, "error", finalErr)
			if remErr := os.Remove(tempLocalPath); remErr != nil && !errors.Is(remErr, os.ErrNotExist) {
				logger.Error("Failed to remove temporary download file", "path", tempLocalPath, "error", remErr)
			}
		}
	}()

	// Get object reader
	objResult, err := nr.fileStore.Get(ctx, objectName)
	if err != nil {
		finalErr = fmt.Errorf("failed to get object reader for %s: %w", objectName, err)
		return finalErr
	}
	defer objResult.Close()

	// Copy data
	bytesCopied, err := io.Copy(outFile, objResult)
	if err != nil {
		finalErr = fmt.Errorf("failed to copy object data to %s: %w", tempLocalPath, err)
		return finalErr
	}
	logger.Debug("File mount data copied", "temp_path", tempLocalPath, "bytes_copied", bytesCopied)

	// Sync and close file before verification/rename
	if syncErr := outFile.Sync(); syncErr != nil {
		logger.Error("Failed to sync temporary file", "path", tempLocalPath, "error", syncErr)
	}
	if closeErr := outFile.Close(); closeErr != nil {
		outFile = nil // Prevent double close in defer
		finalErr = fmt.Errorf("failed to close temporary file %s: %w", tempLocalPath, closeErr)
		return finalErr
	}
	outFile = nil // Mark as closed for defer

	// Verify Checksum
	hashMatches, verifyErr := verifyLocalFileHash(tempLocalPath, expectedDigest)
	if verifyErr != nil || !hashMatches {
		errMsg := fmt.Sprintf("hash verification failed (err: %v, match: %v)", verifyErr, hashMatches)
		logger.Error(errMsg, "path", tempLocalPath, "expected_digest", expectedDigest)
		finalErr = fmt.Errorf("hash verification failed for %s: %s", tempLocalPath, errMsg)
		return finalErr
	}

	// Atomic Rename
	logger.Debug("Verification successful, renaming mount file", "from", tempLocalPath, "to", localPath)
	if err = os.Rename(tempLocalPath, localPath); err != nil {
		finalErr = fmt.Errorf("failed to finalize file mount %s: %w", localPath, err)
		_ = os.Remove(tempLocalPath) // Try removing temp file
		return finalErr
	}

	logger.Info("File mount placed successfully", "path", localPath)
	return nil // Success
}

// Helper to stop all running/starting instances for a specific app
func (nr *NodeRunner) stopAllInstancesForApp(ctx context.Context, appInfo *appInfo, logger *slog.Logger) {
	logger.Info("Stopping all instances for app")
	// appInfo is already locked by caller
	instancesToStop := append([]*ManagedApp{}, appInfo.instances...)

	var wg sync.WaitGroup
	for _, instance := range instancesToStop {
		if instance.Status == StatusRunning || instance.Status == StatusStarting || instance.Status == StatusStopping {
			wg.Add(1)
			go func(inst *ManagedApp) {
				defer wg.Done()
				if err := nr.stopAppInstance(inst, true); err != nil {
					logger.Error("Error stopping instance during cleanup/delete", "instance_id", inst.InstanceID, "error", err)
				} else {
					logger.Debug("Instance stopped during cleanup/delete", "instance_id", inst.InstanceID)
				}
			}(instance)
		}
	}
	wg.Wait()
	logger.Info("Finished stopping all instances for app")
}

// shutdownAllAppInstances iterates through all apps and stops their instances.
func (nr *NodeRunner) shutdownAllAppInstances() {
	nr.logger.Info("Stopping all managed application instances...")
	allAppInfos := nr.state.GetAllAppInfos() // Get snapshot of all app states

	var wg sync.WaitGroup
	for appName, info := range allAppInfos {
		info.mu.Lock() // Lock specific appInfo for modification
		instancesToStop := append([]*ManagedApp{}, info.instances...)
		info.instances = make([]*ManagedApp, 0) // Clear instances from state *before* stopping
		info.mu.Unlock()                        // Unlock after copying and clearing

		for _, instance := range instancesToStop {
			wg.Add(1)
			go func(appName string, inst *ManagedApp) { // Capture vars
				defer wg.Done()
				logger := nr.logger.With("app", appName, "instance_id", inst.InstanceID)
				logger.Info("Initiating shutdown")
				if err := nr.stopAppInstance(inst, true); err != nil { // Intentional stop
					logger.Error("Error during instance shutdown", "error", err)
				} else {
					logger.Info("Instance shutdown completed")
				}
				// Metrics cleanup will be handled by monitorAppInstance's defer logic
				// when it exits due to intentional stop (runner shutting down implies shouldCleanupDisk=true).
			}(appName, instance) // Pass instance pointer
		}
	}
	wg.Wait()
	nr.logger.Info("Finished stopping all managed application instances.")
}

// Stop signals the NodeRunner to shut down gracefully.
func (nr *NodeRunner) Stop() {
	nr.logger.Info("Received external stop signal")
	select {
	case <-nr.globalCtx.Done():
	default:
		nr.globalCancel()
	}
}

//  Helper functions for replica index management

func findNextReplicaIndex(currentInstances []*ManagedApp, targetReplicas int) int {
	usedIndices := make(map[int]bool)
	for _, inst := range currentInstances {
		idx := extractReplicaIndex(inst.InstanceID)
		if idx >= 0 {
			usedIndices[idx] = true
		}
	}
	for i := 0; i < targetReplicas; i++ {
		if !usedIndices[i] {
			return i
		}
	}
	return -1
}

func extractReplicaIndex(instanceID string) int {
	lastDash := strings.LastIndex(instanceID, "-")
	if lastDash == -1 || lastDash == len(instanceID)-1 {
		return -1
	}
	indexStr := instanceID[lastDash+1:]
	var index int
	_, err := fmt.Sscan(indexStr, &index)
	if err != nil {
		return -1
	}
	return index
}

// Helper for simulating delete entry during sync
type simulatedDeleteEntry struct {
	jetstream.KeyValueEntry
	key    string
	bucket string
}

func (s *simulatedDeleteEntry) Key() string                     { return s.key }
func (s *simulatedDeleteEntry) Value() []byte                   { return nil }
func (s *simulatedDeleteEntry) Revision() uint64                { return 0 }
func (s *simulatedDeleteEntry) Created() time.Time              { return time.Time{} }
func (s *simulatedDeleteEntry) Delta() uint64                   { return 0 }
func (s *simulatedDeleteEntry) Operation() jetstream.KeyValueOp { return jetstream.KeyValueDelete }
func (s *simulatedDeleteEntry) Bucket() string                  { return s.bucket }

// cleanupInstanceMetrics ensures metrics for a specific run are finalized.
// With RunID, this no longer deletes metrics but ensures 'up' is 0.
// This is likely redundant as monitorAppInstance already handles setting 'up' to 0.
func (nr *NodeRunner) cleanupInstanceMetrics(instance *ManagedApp) {
	if instance == nil {
		nr.logger.Warn("cleanupInstanceMetrics called with nil instance")
		return
	}
	appName := InstanceIDToAppName(instance.InstanceID)
	instanceID := instance.InstanceID
	runID := instance.RunID // Get the specific run ID
	nodeID := nr.nodeID

	// Ensure the 'up' metric for this specific run is 0.
	// This should be handled by monitorAppInstance on termination.
	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nodeID, runID).Set(0)

	// No deletion of other metrics (Info, Memory, CPU, ExitCode for this runID)
	// They remain as a historical record for this run.
	nr.logger.Debug("Metrics finalized for instance run (no deletion)", "instance_id", instanceID, "run_id", runID)
}
