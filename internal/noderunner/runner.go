package noderunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
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
	"github.com/robfig/cron/v3"
)

var runnerVersion = "0.2.4-dev" // Incremented version: Cron scheduling added

// NodeRunner manages application processes on a single node.
type NodeRunner struct {
	nodeID        string                // Unique identifier for this node runner instance, defaults to hostname.
	nc            *nats.Conn            // NATS connection instance.
	js            jetstream.JetStream   // JetStream context for interacting with NATS JetStream.
	kvAppConfigs  jetstream.KeyValue    // JetStream KeyValue store for application configurations (ServiceSpec).
	kvNodeStates  jetstream.KeyValue    // JetStream KeyValue store for storing this node's state and heartbeats.
	appBinaries   jetstream.ObjectStore // JetStream ObjectStore for application binaries.
	fileStore     jetstream.ObjectStore // JetStream ObjectStore for shared files mounted by applications.
	state         *AppStateManager      // Manages the runtime state of all applications and their instances on this node.
	logger        *slog.Logger          // Structured logger for the node runner.
	kvSecrets     jetstream.KeyValue    // JetStream KeyValue store for encrypted application secrets.
	masterKey     []byte                // Decoded AES-256 master key used for encrypting/decrypting secrets.
	dataDir       string                // Root directory for storing node runner data, including downloaded binaries and instance work directories.
	version       string                // Version of the node runner software.
	startTime     time.Time             // Timestamp when this node runner instance started.
	localOS       string                // Operating system of the node where the runner is executing (e.g., "linux", "darwin").
	localArch     string                // Architecture of the node where the runner is executing (e.g., "amd64", "arm64").
	globalCtx     context.Context       // Global context for the node runner, used for managing shutdown signals.
	globalCancel  context.CancelFunc    // Function to cancel the global context, triggering a shutdown.
	shutdownWg    sync.WaitGroup        // WaitGroup to ensure all goroutines (heartbeat, watcher, servers) complete during shutdown.
	metricsAddr   string                // Network address (e.g., ":9100") for the Prometheus metrics HTTP endpoint. Empty to disable.
	metricsServer *http.Server          // HTTP server instance for exposing Prometheus metrics.
	adminAddr     string                // Network address (e.g., ":9101") for the admin UI HTTP endpoint. Empty to disable.
	adminServer   *http.Server          // HTTP server instance for the admin UI.
	cronScheduler *cron.Cron            // Scheduler for managing cron job executions based on ServiceSpecs.
}

// NewNodeRunner creates and initializes a new NodeRunner.
func NewNodeRunner(nodeID, natsURL, dataDir, masterKeyBase64, metricsAddr, adminAddr string, logger *slog.Logger) (*NodeRunner, error) {
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
	}

	nc, err := nats.Connect(natsURL,
		nats.Name(fmt.Sprintf("node-runner-%s", nodeID)),
		nats.ReconnectWait(2*time.Second),
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
	)
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

	kvSecrets, err := js.CreateOrUpdateKeyValue(setupCtx, jetstream.KeyValueConfig{
		Bucket:      SecretKVBucket,
		Description: "Stores encrypted application secrets.",
		History:     1,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to bind/create KV store '%s': %w", SecretKVBucket, err)
	}
	logger.Debug("Bound to Secret KV store", "bucket", SecretKVBucket)

	fileStore, err := js.CreateOrUpdateObjectStore(setupCtx, jetstream.ObjectStoreConfig{
		Bucket: FileOSBucket, Description: "Shared files for Narun applications",
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to bind/create Object store '%s': %w", FileOSBucket, err)
	}
	logger.Info("Bound to File store", "bucket", FileOSBucket)

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

	cronLogger := CronSlogLogger{logger: logger.With("component", "cron")}
	cronScheduler := cron.New(cron.WithLogger(cronLogger))

	runner := &NodeRunner{
		nodeID:        nodeID,
		nc:            nc,
		js:            js,
		kvAppConfigs:  kvAppConfigs,
		kvNodeStates:  kvNodeStates,
		kvSecrets:     kvSecrets,
		masterKey:     masterKey,
		fileStore:     fileStore,
		appBinaries:   osBucket,
		state:         NewAppStateManager(),
		logger:        logger,
		dataDir:       dataDirAbs,
		version:       runnerVersion,
		startTime:     runnerStartTime,
		localOS:       localOS,
		localArch:     localArch,
		metricsAddr:   metricsAddr,
		adminAddr:     adminAddr,
		globalCtx:     gCtx,
		globalCancel:  gCancel,
		cronScheduler: cronScheduler,
	}

	return runner, nil
}

func (nr *NodeRunner) Run() error {
	nr.logger.Info("Starting node runner", "version", nr.version, "os", nr.localOS, "arch", nr.localArch)
	defer nr.logger.Info("Node runner stopped")

	if nr.metricsAddr != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		nr.metricsServer = &http.Server{
			Addr:              nr.metricsAddr,
			Handler:           metricsMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		nr.shutdownWg.Add(1)
		go func() {
			defer nr.shutdownWg.Done()
			nr.logger.Info("Starting Prometheus metrics server", "address", nr.metricsAddr)
			if err := nr.metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				nr.logger.Error("Prometheus metrics server failed", "error", err)
				nr.globalCancel()
			} else {
				nr.logger.Info("Prometheus metrics server shut down")
			}
		}()
	}

	if nr.adminAddr != "" {
		adminMux := http.NewServeMux()
		adminMux.HandleFunc("/", nr.handleAdminUI)
		nr.adminServer = &http.Server{
			Addr:              nr.adminAddr,
			Handler:           adminMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		nr.shutdownWg.Add(1)
		go func() {
			defer nr.shutdownWg.Done()
			nr.logger.Info("Starting admin UI server", "address", nr.adminAddr)
			if err := nr.adminServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				nr.logger.Error("Admin UI server failed", "error", err)
				nr.globalCancel()
			} else {
				nr.logger.Info("Admin UI server shut down")
			}
		}()
	}

	if err := nr.updateNodeState("running"); err != nil {
		nr.logger.Error("Initial node state registration failed", "error", err)
	} else {
		nr.logger.Info("Node registered successfully")
	}
	nr.shutdownWg.Add(1)
	go nr.heartbeatLoop()
	nr.logger.Info("Heartbeat loop started", "interval", NodeHeartbeatInterval)

	nr.cronScheduler.Start()
	nr.logger.Info("Cron scheduler started")
	defer func() {
		stopCtx := nr.cronScheduler.Stop()
		nr.logger.Info("Cron scheduler stop initiated...")
		select {
		case <-stopCtx.Done():
			nr.logger.Info("Cron scheduler stopped cleanly.")
		case <-time.After(5 * time.Second):
			nr.logger.Warn("Cron scheduler stop timed out.")
		}
	}()

	syncCtx, syncCancel := context.WithTimeout(nr.globalCtx, 60*time.Second)
	if err := nr.syncAllApps(syncCtx); err != nil {
		nr.logger.Error("Failed initial app synchronization", "error", err)
	} else {
		nr.logger.Info("Initial app synchronization complete")
	}
	syncCancel()

	watcher, err := nr.kvAppConfigs.WatchAll(nr.globalCtx)
	if err != nil {
		if nr.globalCtx.Err() != nil {
			nr.logger.Info("Failed to start KV watcher due to shutdown signal")
			return nr.globalCtx.Err()
		}
		return fmt.Errorf("failed to start KV watcher on '%s': %w", AppConfigKVBucket, err)
	}
	defer watcher.Stop()
	nr.logger.Info("Started watching for app configuration changes", "bucket", AppConfigKVBucket)

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
					if nr.globalCtx.Err() == nil {
						nr.logger.Error("Watcher closed unexpectedly while runner context is still active.")
						nr.globalCancel()
					} else {
						nr.logger.Info("Watcher closed during shutdown.")
					}
					return
				}
				if entry == nil {
					nr.logger.Debug("Received nil update from watcher")
					continue
				}
				nr.handleAppConfigUpdate(nr.globalCtx, entry)
			}
		}
	}()

	<-nr.globalCtx.Done()
	nr.logger.Info("Shutdown signal received, initiating shutdown...")

	if err := nr.updateNodeState("shutting_down"); err != nil {
		nr.logger.Warn("Failed to update node state to shutting_down", "error", err)
	}

	nr.shutdownAllAppInstances()

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

	nr.logger.Debug("Waiting for background goroutines to finish...")
	nr.shutdownWg.Wait()
	nr.logger.Debug("Background goroutines finished.")

	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deleteCancel()
	nr.logger.Info("Deleting node state from KV store", "key", nr.nodeID)
	if err := nr.kvNodeStates.Delete(deleteCtx, nr.nodeID); err != nil {
		nr.logger.Error("Failed to delete node state from KV", "key", nr.nodeID, "error", err)
	}

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

	if err := nr.globalCtx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("runner stopped due to context error: %w", err)
	}
	return nil
}

func (nr *NodeRunner) handleAdminUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	fmt.Fprintf(w, `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="10">
    <title>Narun Node Runner Admin - %s</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol"; margin: 20px; background-color: #f8f9fa; color: #212529; line-height: 1.6; }
        h1, h2, h3 { color: #007bff; border-bottom: 2px solid #007bff; padding-bottom: 0.3em; }
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
		.cron-info { font-style: italic; color: #555; font-size: 0.9em; }
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
	sort.Strings(appNames)

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
		IsCronJob    bool
	}

	for _, appName := range appNames {
		appInfo := allAppInfos[appName]
		appInfo.mu.RLock()

		fmt.Fprintf(w, "<h2>Application: <span class=\"code\">%s</span></h2>", html.EscapeString(appName))

		if appInfo.spec == nil {
			fmt.Fprintf(w, "<p><em>No specification loaded for this application (possibly deleted or in error state).</em></p>")
			appInfo.mu.RUnlock()
			continue
		}

		fmt.Fprintf(w, "<p><strong>Tag:</strong> <span class=\"code\">%s</span> | <strong>Mode:</strong> <span class=\"code\">%s</span> | <strong>Config Hash:</strong> <span class=\"code\">%s</span></p>",
			html.EscapeString(appInfo.spec.Tag), html.EscapeString(appInfo.spec.Mode), html.EscapeString(appInfo.configHash))

		if appInfo.spec.CronSchedule != "" {
			fmt.Fprintf(w, "<p class=\"cron-info\"><strong>Cron Schedule:</strong> <span class=\"code\">%s</span> (Retries: %d, Timeout: %ds, Concurrent: %t)</p>",
				html.EscapeString(appInfo.spec.CronSchedule), appInfo.spec.CronMaxRetries, appInfo.spec.CronTimeoutSeconds, appInfo.spec.CronAllowConcurrent)
		}

		instanceViews := make([]uiInstanceView, 0, len(appInfo.instances))
		if len(appInfo.instances) == 0 {
			fmt.Fprintf(w, "<p><em>No instances running for this application on this node.</em></p>")
		} else {
			tempSortedInstances := make([]*ManagedApp, len(appInfo.instances))
			copy(tempSortedInstances, appInfo.instances)
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
					IsCronJob:    instance.IsCronJobRun,
				})
			}
		}
		appInfo.mu.RUnlock()

		if len(instanceViews) > 0 {
			fmt.Fprintf(w, `
    <table>
        <thead>
            <tr>
                <th>Instance ID</th>
                <th>Run ID</th>
                <th>Status</th>
                <th>Type</th>
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
				instanceType := "Replica"
				if view.IsCronJob {
					instanceType = "Cron Job"
				}
				fmt.Fprintf(w, `
            <tr>
                <td><span class="code">%s</span></td>
                <td><span class="code">%s</span></td>
                <td class="%s">%s</td>
                <td>%s</td>
                <td>%s</td>
                <td>%s</td>
                <td>%s</td>
                <td>%d</td>
                <td>%s</td>
                <td><span class="code">%s</span></td>
            </tr>`,
					html.EscapeString(view.InstanceID), html.EscapeString(view.RunID),
					view.StatusClass, html.EscapeString(string(view.Status)),
					instanceType, view.PID, view.StartTime, view.Uptime, view.RestartCount, view.ExitCode, html.EscapeString(view.BinaryPath))
			}
			fmt.Fprintf(w, `</tbody></table>`)
		}
	}
	fmt.Fprintf(w, "<h3>Active Cron Jobs on this Node</h3>")
	cronEntries := nr.cronScheduler.Entries()
	if len(cronEntries) == 0 {
		fmt.Fprintf(w, "<p><em>No active cron jobs scheduled on this node.</em></p>")
	} else {
		fmt.Fprintf(w, `
    <table>
        <thead>
            <tr>
                <th>Cron Job ID (Internal)</th>
                <th>App Name (Est.)</th>
                <th>Next 5 Runs (Approx)</th>
                <th>Next Run (Exact)</th>
                <th>Prev Run (Exact)</th>
            </tr>
        </thead>
        <tbody>`)
		for _, entry := range cronEntries {
			appNameGuess := "Unknown"
			if jobWrapper, ok := entry.Job.(cronJobWrapper); ok {
				jobWrapper.appInfo.mu.RLock()
				if jobWrapper.appInfo.spec != nil {
					appNameGuess = jobWrapper.appInfo.spec.Name
				}
				jobWrapper.appInfo.mu.RUnlock()
			}
			var scheduleDetails strings.Builder
			if entry.Schedule != nil {
				nextTimes := make([]string, 0, 5)
				currentTime := time.Now()
				for i := 0; i < 5; i++ {
					currentTime = entry.Schedule.Next(currentTime)
					if currentTime.IsZero() {
						break
					}
					nextTimes = append(nextTimes, currentTime.Format("Jan 02 15:04 MST"))
				}
				if len(nextTimes) > 0 {
					scheduleDetails.WriteString(strings.Join(nextTimes, ", "))
				} else {
					scheduleDetails.WriteString("(no further scheduled runs)")
				}
			} else {
				scheduleDetails.WriteString("(invalid schedule)")
			}

			fmt.Fprintf(w, `
            <tr>
                <td>%d</td>
                <td><span class="code">%s</span></td>
                <td>%s</td>
                <td>%s</td>
                <td>%s</td>
            </tr>`,
				entry.ID,
				html.EscapeString(appNameGuess),
				html.EscapeString(scheduleDetails.String()),
				entry.Next.Format(time.RFC1123),
				entry.Prev.Format(time.RFC1123))
		}
		fmt.Fprintf(w, `</tbody></table>`)
	}

	fmt.Fprintf(w, `
    <div class="footer">
        <p>Narun Node Runner Admin UI © %d</p>
    </div>
</div>
</body>
</html>`, time.Now().Year())
}

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

	cronJobEntryDescs := make([]string, 0)
	if nr.cronScheduler != nil {
		for _, entry := range nr.cronScheduler.Entries() {
			appName := "unknown"
			if jobWrapper, ok := entry.Job.(cronJobWrapper); ok {
				jobWrapper.appInfo.mu.RLock()
				if jobWrapper.appInfo.spec != nil {
					appName = jobWrapper.appInfo.spec.Name
				}
				jobWrapper.appInfo.mu.RUnlock()
			}
			cronJobEntryDescs = append(cronJobEntryDescs, fmt.Sprintf("%s (ID: %d)", appName, entry.ID))
		}
	}
	sort.Strings(cronJobEntryDescs)

	state := NodeState{
		NodeID:           nr.nodeID,
		LastSeen:         time.Now(),
		Version:          nr.version,
		StartTime:        nr.startTime,
		ManagedInstances: managedInstanceIDs,
		Status:           status,
		OS:               nr.localOS,
		Arch:             nr.localArch,
		CronJobIDs:       cronJobEntryDescs,
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

	keysSeen := make(map[string]bool)
	syncWg := sync.WaitGroup{}
	errChan := make(chan error, 1)

	keysChan := keysLister.Keys()

syncLoop:
	for {
		select {
		case <-ctx.Done():
			nr.logger.Warn("Initial synchronization canceled or timed out.", "error", ctx.Err())
			keysLister.Stop()
			syncWg.Wait()
			close(errChan)
			for range errChan {
			}
			return ctx.Err()
		case key, ok := <-keysChan:
			if !ok {
				break syncLoop
			}
			if key == "" {
				continue
			}
			keysSeen[key] = true

			syncWg.Add(1)
			go func(k string) {
				defer syncWg.Done()
				entryCtx, entryCancel := context.WithTimeout(ctx, 20*time.Second)
				defer entryCancel()

				entry, getErr := nr.kvAppConfigs.Get(entryCtx, k)
				if getErr != nil {
					if errors.Is(getErr, jetstream.ErrKeyNotFound) {
						nr.logger.Warn("App config key disappeared during sync, treating as deleted", "key", k)
						nr.handleAppConfigUpdate(entryCtx, &simulatedDeleteEntry{key: k, bucket: AppConfigKVBucket})
					} else {
						nr.logger.Error("Failed to get KV entry during sync", "key", k, "error", getErr)
						select {
						case errChan <- fmt.Errorf("sync failed getting key %s: %w", k, getErr):
						default:
						}
					}
					return
				}
				nr.handleAppConfigUpdate(entryCtx, entry)
			}(key)
		}
	}

	syncWg.Wait()
	close(errChan)

	if firstErr := <-errChan; firstErr != nil {
		return firstErr
	}

	nr.logger.Info("Pruning locally managed apps not found in KV store...")
	allLocalApps := nr.state.GetAllAppInfos()
	for appName, appInfo := range allLocalApps {
		if !keysSeen[appName] {
			nr.logger.Info("Pruning app no longer in KV config", "app", appName)
			appInfo.mu.Lock()
			nr.cleanupAppInstancesAndCron(ctx, appInfo, nr.logger.With("app", appName))
			appInfo.mu.Unlock()
			nr.state.DeleteApp(appName)
		}
	}
	nr.logger.Info("App pruning complete.")

	return nil
}

func (nr *NodeRunner) handleAppConfigUpdate(ctx context.Context, entry jetstream.KeyValueEntry) {
	if entry.Bucket() != AppConfigKVBucket {
		nr.logger.Warn("Received KV update from unexpected bucket", "bucket", entry.Bucket())
		return
	}
	appName := entry.Key()
	logger := nr.logger.With("app", appName)
	if entry.Operation() != jetstream.KeyValueDelete && entry.Operation() != jetstream.KeyValuePurge {
		logger = logger.With("kv_revision", entry.Revision())
	}
	logger = logger.With("kv_operation", entry.Operation().String())

	appInfo := nr.state.GetAppInfo(appName)
	appInfo.mu.Lock()

	switch entry.Operation() {
	case jetstream.KeyValuePut:
		logger.Info("Processing configuration update")
		spec, err := ParseServiceSpec(entry.Value())
		if err != nil {
			logger.Error("Failed to parse service spec from KV", "error", err)
			nr.cleanupAppInstancesAndCron(ctx, appInfo, logger)
			appInfo.mu.Unlock()
			return
		}
		if spec.Name != appName {
			logger.Error("Service name in spec does not match KV key", "spec_name", spec.Name, "key", appName)
			nr.cleanupAppInstancesAndCron(ctx, appInfo, logger)
			appInfo.mu.Unlock()
			return
		}

		newConfigHash, err := calculateSpecHash(spec)
		if err != nil {
			logger.Error("Failed to hash service spec", "error", err)
			nr.cleanupAppInstancesAndCron(ctx, appInfo, logger)
			appInfo.mu.Unlock()
			return
		}

		configHashChanged := appInfo.configHash != newConfigHash
		appInfo.spec = spec
		appInfo.configHash = newConfigHash

		appInfo.cronScheduler.Lock()
		if appInfo.cronEntryID != 0 {
			nr.cronScheduler.Remove(cron.EntryID(appInfo.cronEntryID))
			appInfo.cronEntryID = 0
			logger.Info("Removed existing cron job due to config update")
		}
		if spec.CronSchedule != "" {
			shouldRunCronOnThisNode := false
			if len(spec.Nodes) == 0 {
				shouldRunCronOnThisNode = true
			} else {
				for _, nodeSel := range spec.Nodes {
					if nodeSel.Name == nr.nodeID {
						shouldRunCronOnThisNode = true
						break
					}
				}
			}

			if shouldRunCronOnThisNode {
				jobID, err := nr.cronScheduler.AddJob(spec.CronSchedule, cronJobWrapper{
					runner:  nr,
					appInfo: appInfo,
					logger:  logger.With("component", "cron-job-wrapper", "app", appName, "schedule", spec.CronSchedule),
				})
				if err != nil {
					logger.Error("Failed to add/update cron job", "cron_schedule", spec.CronSchedule, "error", err)
				} else {
					appInfo.cronEntryID = int(jobID)
					logger.Info("Scheduled/Updated cron job", "cron_schedule", spec.CronSchedule, "job_id", jobID)
				}
			} else {
				logger.Info("Cron job defined in spec, but this node is not targeted. Not scheduling.", "app", appName)
			}
		}
		appInfo.cronScheduler.Unlock()

		targetReplicas := spec.FindTargetReplicas(nr.nodeID)
		currentPersistentReplicas := 0
		for _, inst := range appInfo.instances {
			if !inst.IsCronJobRun {
				currentPersistentReplicas++
			}
		}
		logger.Info("Reconciling persistent service replicas", "target", targetReplicas, "current", currentPersistentReplicas, "hash_changed", configHashChanged)

		if configHashChanged {
			logger.Info("Configuration hash changed, restarting all persistent service replicas")
			var persistentToStop []*ManagedApp
			var remainingInstancesAfterStop []*ManagedApp
			for _, inst := range appInfo.instances {
				if !inst.IsCronJobRun {
					persistentToStop = append(persistentToStop, inst)
				} else {
					remainingInstancesAfterStop = append(remainingInstancesAfterStop, inst)
				}
			}
			appInfo.instances = remainingInstancesAfterStop

			for _, inst := range persistentToStop {
				if err := nr.stopAppInstance(inst, true); err != nil {
					logger.Error("Error stopping instance for config update", "instance_id", inst.InstanceID, "error", err)
				}
				nr.cleanupInstanceMetrics(inst)
			}
			currentPersistentReplicas = 0
		}

		if targetReplicas > currentPersistentReplicas {
			needed := targetReplicas - currentPersistentReplicas
			logger.Info("Scaling up persistent replicas", "needed", needed)
			for i := 0; i < needed; i++ {
				nonCronInstances := make([]*ManagedApp, 0)
				for _, inst := range appInfo.instances {
					if !inst.IsCronJobRun {
						nonCronInstances = append(nonCronInstances, inst)
					}
				}
				replicaIndex := findNextReplicaIndex(nonCronInstances, targetReplicas)
				if replicaIndex == -1 {
					logger.Error("Could not find available replica index slot for persistent replica", "target", targetReplicas)
					break
				}
				instanceID := GenerateInstanceID(appName, replicaIndex)
				if err := nr.startAppInstance(ctx, appInfo, instanceID, false, 0); err != nil {
					logger.Error("Failed to start new persistent replica during scale up", "replica_index", replicaIndex, "error", err)
				} else {
					time.Sleep(100 * time.Millisecond)
				}
			}
		} else if targetReplicas < currentPersistentReplicas {
			excess := currentPersistentReplicas - targetReplicas
			logger.Info("Scaling down persistent replicas", "excess", excess)

			persistentInstancesToConsider := make([]*ManagedApp, 0)
			cronJobInstancesToKeep := make([]*ManagedApp, 0)
			for _, inst := range appInfo.instances {
				if !inst.IsCronJobRun {
					persistentInstancesToConsider = append(persistentInstancesToConsider, inst)
				} else {
					cronJobInstancesToKeep = append(cronJobInstancesToKeep, inst)
				}
			}
			sort.Slice(persistentInstancesToConsider, func(i, j int) bool {
				idxI := extractReplicaIndex(persistentInstancesToConsider[i].InstanceID)
				idxJ := extractReplicaIndex(persistentInstancesToConsider[j].InstanceID)
				return idxI > idxJ
			})

			instancesToStopThisRound := make([]*ManagedApp, 0, excess)
			if excess <= len(persistentInstancesToConsider) {
				instancesToStopThisRound = persistentInstancesToConsider[:excess]
			} else {
				instancesToStopThisRound = persistentInstancesToConsider
			}

			newAppInstances := make([]*ManagedApp, 0, len(appInfo.instances)-len(instancesToStopThisRound))
			newAppInstances = append(newAppInstances, cronJobInstancesToKeep...)

			stoppedIDs := make(map[string]bool)
			for _, instToStop := range instancesToStopThisRound {
				logger.Info("Stopping excess persistent replica", "instance_id", instToStop.InstanceID)
				if err := nr.stopAppInstance(instToStop, true); err != nil {
					logger.Error("Error stopping excess persistent replica", "instance_id", instToStop.InstanceID, "error", err)
				}
				nr.cleanupInstanceMetrics(instToStop)
				stoppedIDs[instToStop.InstanceID] = true
			}
			for _, pInst := range persistentInstancesToConsider {
				if !stoppedIDs[pInst.InstanceID] {
					newAppInstances = append(newAppInstances, pInst)
				}
			}
			appInfo.instances = newAppInstances

		} else {
			if targetReplicas == 0 && !configHashChanged {
				logger.Info("Target replica count is 0 for persistent services. Ensuring no persistent replicas are running.")
				var persistentToStop []*ManagedApp
				var remainingInstances []*ManagedApp
				for _, inst := range appInfo.instances {
					if !inst.IsCronJobRun {
						persistentToStop = append(persistentToStop, inst)
					} else {
						remainingInstances = append(remainingInstances, inst)
					}
				}
				if len(persistentToStop) > 0 {
					appInfo.instances = remainingInstances
					for _, inst := range persistentToStop {
						if err := nr.stopAppInstance(inst, true); err != nil {
							logger.Error("Error stopping persistent replica for target 0", "instance_id", inst.InstanceID, "error", err)
						}
						nr.cleanupInstanceMetrics(inst)
					}
				}
			} else if !configHashChanged {
				logger.Info("Persistent replica count matches target and config hash unchanged. No action needed for replicas.")
			}
		}
		appInfo.mu.Unlock()

	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		logger.Info("Processing configuration delete/purge request")
		nr.cleanupAppInstancesAndCron(ctx, appInfo, logger)
		appInfo.mu.Unlock()
		nr.state.DeleteApp(appName)

	default:
		logger.Warn("Ignoring unknown KV operation")
		appInfo.mu.Unlock()
	}
}

func (nr *NodeRunner) cleanupAppInstancesAndCron(ctx context.Context, appInfo *appInfo, logger *slog.Logger) {
	instancesToCleanup := make([]*ManagedApp, len(appInfo.instances))
	copy(instancesToCleanup, appInfo.instances)

	appInfo.instances = make([]*ManagedApp, 0)
	var appNameForLog string
	if appInfo.spec != nil {
		appNameForLog = appInfo.spec.Name
	} else {
		// Attempt to derive from one of the instances if spec is already nil
		if len(instancesToCleanup) > 0 {
			appNameForLog = InstanceIDToAppName(instancesToCleanup[0].InstanceID)
		} else {
			appNameForLog = "unknown"
		}
	}

	logger.Info("Stopping all instances for app (in cleanupAppInstancesAndCron)", "app_name", appNameForLog)
	var wg sync.WaitGroup
	for _, instance := range instancesToCleanup {
		if instance.Status == StatusRunning || instance.Status == StatusStarting || instance.Status == StatusStopping {
			wg.Add(1)
			go func(inst *ManagedApp) {
				defer wg.Done()
				localLogger := logger.With("instance_id_to_stop", inst.InstanceID, "run_id", inst.RunID)
				localLogger.Debug("Attempting to stop instance in cleanup")
				if err := nr.stopAppInstance(inst, true); err != nil {
					localLogger.Error("Error stopping instance", "error", err)
				} else {
					localLogger.Debug("Instance stop signal sent/processed in cleanup")
				}
				nr.cleanupInstanceMetrics(inst)
			}(instance)
		} else {
			nr.cleanupInstanceMetrics(instance)
		}
	}
	wg.Wait()
	logger.Info("Finished stopping all instances for app (in cleanupAppInstancesAndCron)")

	appInfo.cronScheduler.Lock()
	if appInfo.cronEntryID != 0 {
		nr.cronScheduler.Remove(cron.EntryID(appInfo.cronEntryID))
		appInfo.cronEntryID = 0
		logger.Info("Removed cron job for app due to delete/error")
	}
	appInfo.cronScheduler.Unlock()

	appInfo.spec = nil
	appInfo.configHash = ""
}

func (nr *NodeRunner) fetchAndPlaceFile(ctx context.Context, objectName, instanceWorkDir, targetRelativePath string, logger *slog.Logger) error {
	logger = logger.With("mount_object", objectName, "mount_path", targetRelativePath)
	logger.Info("Fetching and placing file mount")

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

	cleanRelativePath := filepath.Clean(targetRelativePath)
	if strings.HasPrefix(cleanRelativePath, "..") || filepath.IsAbs(cleanRelativePath) {
		return fmt.Errorf("invalid target mount path '%s': must be relative and within the work directory", targetRelativePath)
	}
	localPath := filepath.Join(instanceWorkDir, cleanRelativePath)
	localDir := filepath.Dir(localPath)

	_, statErr := os.Stat(localPath)
	if statErr == nil {
		hashMatches, verifyErr := verifyLocalFileHash(localPath, expectedDigest)
		if verifyErr == nil && hashMatches {
			logger.Info("Local file mount exists and hash verified.", "path", localPath)
			return nil
		}
		logger.Warn("Local file mount exists but hash verification failed or errored. Re-downloading.", "path", localPath, "verify_err", verifyErr)
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("failed to stat local file mount path %s: %w", localPath, statErr)
	} else {
		logger.Info("Local file mount not found.", "path", localPath)
	}

	logger.Info("Downloading file mount object", "object", objectName, "local_path", localPath)
	if err := os.MkdirAll(localDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory for mount path %s: %w", localDir, err)
	}

	tempLocalPath := localPath + ".tmp." + fmt.Sprintf("%d", time.Now().UnixNano())
	outFile, err := os.Create(tempLocalPath)
	if err != nil {
		return fmt.Errorf("failed to create temporary file %s: %w", tempLocalPath, err)
	}

	var finalErr error
	defer func() {
		if outFile != nil {
			outFile.Close()
		}
		if finalErr != nil {
			logger.Warn("Removing temporary mount file due to error", "path", tempLocalPath, "error", finalErr)
			if remErr := os.Remove(tempLocalPath); remErr != nil && !errors.Is(remErr, os.ErrNotExist) {
				logger.Error("Failed to remove temporary download file", "path", tempLocalPath, "error", remErr)
			}
		}
	}()

	objResult, err := nr.fileStore.Get(ctx, objectName)
	if err != nil {
		finalErr = fmt.Errorf("failed to get object reader for %s: %w", objectName, err)
		return finalErr
	}
	defer objResult.Close()

	bytesCopied, err := io.Copy(outFile, objResult)
	if err != nil {
		finalErr = fmt.Errorf("failed to copy object data to %s: %w", tempLocalPath, err)
		return finalErr
	}
	logger.Debug("File mount data copied", "temp_path", tempLocalPath, "bytes_copied", bytesCopied)

	if syncErr := outFile.Sync(); syncErr != nil {
		logger.Error("Failed to sync temporary file", "path", tempLocalPath, "error", syncErr)
	}
	if closeErr := outFile.Close(); closeErr != nil {
		outFile = nil
		finalErr = fmt.Errorf("failed to close temporary file %s: %w", tempLocalPath, closeErr)
		return finalErr
	}
	outFile = nil

	hashMatches, verifyErr := verifyLocalFileHash(tempLocalPath, expectedDigest)
	if verifyErr != nil || !hashMatches {
		errMsg := fmt.Sprintf("hash verification failed (err: %v, match: %v)", verifyErr, hashMatches)
		logger.Error(errMsg, "path", tempLocalPath, "expected_digest", expectedDigest)
		finalErr = fmt.Errorf("hash verification failed for %s: %s", tempLocalPath, errMsg)
		return finalErr
	}

	logger.Debug("Verification successful, renaming mount file", "from", tempLocalPath, "to", localPath)
	if err = os.Rename(tempLocalPath, localPath); err != nil {
		finalErr = fmt.Errorf("failed to finalize file mount %s: %w", localPath, err)
		_ = os.Remove(tempLocalPath)
		return finalErr
	}

	logger.Info("File mount placed successfully", "path", localPath)
	return nil
}

func (nr *NodeRunner) stopAllInstancesForApp(ctx context.Context, appInfo *appInfo, logger *slog.Logger) {
	if len(appInfo.instances) == 0 {
		logger.Debug("No instances in appInfo to stop.")
		return
	}
	logger.Info("Stopping all instances for app", "instance_count_in_appinfo", len(appInfo.instances))

	instancesToStop := append([]*ManagedApp{}, appInfo.instances...)

	var wg sync.WaitGroup
	for _, instance := range instancesToStop {
		if instance.Status == StatusRunning || instance.Status == StatusStarting || instance.Status == StatusStopping {
			wg.Add(1)
			go func(inst *ManagedApp) {
				defer wg.Done()
				localLogger := logger.With("instance_id_to_stop", inst.InstanceID, "run_id", inst.RunID)
				localLogger.Debug("Attempting to stop instance")
				if err := nr.stopAppInstance(inst, true); err != nil {
					localLogger.Error("Error stopping instance", "error", err)
				} else {
					localLogger.Debug("Instance stop signal sent/processed")
				}
			}(instance)
		}
	}
	wg.Wait()
	logger.Info("Finished stopping all instances for app")
}

func (nr *NodeRunner) shutdownAllAppInstances() {
	nr.logger.Info("Stopping all managed application instances...")
	allAppInfos := nr.state.GetAllAppInfos()

	var wg sync.WaitGroup
	for appName, info := range allAppInfos {
		info.mu.Lock()
		instancesToStop := append([]*ManagedApp{}, info.instances...)
		info.instances = make([]*ManagedApp, 0)
		info.mu.Unlock()

		for _, instance := range instancesToStop {
			wg.Add(1)
			go func(appName string, inst *ManagedApp) {
				defer wg.Done()
				logger := nr.logger.With("app", appName, "instance_id", inst.InstanceID)
				logger.Info("Initiating shutdown")
				if err := nr.stopAppInstance(inst, true); err != nil {
					logger.Error("Error during instance shutdown", "error", err)
				} else {
					logger.Info("Instance shutdown completed")
				}
			}(appName, instance)
		}
	}
	wg.Wait()
	nr.logger.Info("Finished stopping all managed application instances.")
}

func (nr *NodeRunner) Stop() {
	nr.logger.Info("Received external stop signal")
	select {
	case <-nr.globalCtx.Done():
	default:
		nr.globalCancel()
	}
}

func findNextReplicaIndex(currentNonCronInstances []*ManagedApp, targetReplicas int) int {
	usedIndices := make(map[int]bool)
	for _, inst := range currentNonCronInstances {
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
	if strings.Contains(instanceID, "-cron-") {
		return -1
	}
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

func (nr *NodeRunner) cleanupInstanceMetrics(instance *ManagedApp) {
	if instance == nil {
		nr.logger.Warn("cleanupInstanceMetrics called with nil instance")
		return
	}
	appName := InstanceIDToAppName(instance.InstanceID)
	instanceID := instance.InstanceID
	runID := instance.RunID
	nodeID := nr.nodeID

	metrics.NarunNodeRunnerInstanceUp.WithLabelValues(appName, instanceID, nodeID, runID).Set(0)
	nr.logger.Debug("Metrics finalized for instance run (no deletion)", "instance_id", instanceID, "run_id", runID)
}

type cronJobWrapper struct {
	runner  *NodeRunner
	appInfo *appInfo
	logger  *slog.Logger
}

func (w cronJobWrapper) Run() {
	w.runner.triggerCronJobExecution(w.appInfo, w.logger)
}

type CronSlogLogger struct {
	logger *slog.Logger
}

func (l CronSlogLogger) Info(msg string, keysAndValues ...interface{}) {
	l.logger.Info(msg, keysAndValues...)
}

func (l CronSlogLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	allArgs := make([]interface{}, 0, len(keysAndValues)+2)
	allArgs = append(allArgs, "error", err)
	allArgs = append(allArgs, keysAndValues...)
	l.logger.Error(msg, allArgs...)
}

var _ cron.Logger = CronSlogLogger{}

func (nr *NodeRunner) triggerCronJobExecution(appInfoToUse *appInfo, jobLogger *slog.Logger) {
	appInfoToUse.mu.RLock()
	spec := appInfoToUse.spec
	if spec == nil {
		jobLogger.Error("Cron job triggered, but app spec is nil. Skipping.", "app_name_from_info_guess", "unknown")
		appInfoToUse.mu.RUnlock()
		return
	}
	appName := spec.Name
	logger := jobLogger.With("app", appName, "cron_schedule", spec.CronSchedule)
	logger.Info("Cron job triggered")

	currentSpec := *spec
	appInfoToUse.mu.RUnlock()

	if !currentSpec.CronAllowConcurrent {
		appInfoToUse.mu.Lock()
		var runningCronJobForApp *ManagedApp
		for _, inst := range appInfoToUse.instances {
			if inst.IsCronJobRun && (inst.Status == StatusRunning || inst.Status == StatusStarting) {
				runningCronJobForApp = inst
				break
			}
		}
		appInfoToUse.mu.Unlock()

		if runningCronJobForApp != nil {
			logger.Warn("Cron job triggered, but a previous run is still active and concurrent runs are disallowed. Skipping.",
				"running_instance_id", runningCronJobForApp.InstanceID, "run_id", runningCronJobForApp.RunID)
			return
		}
	}

	var lastErr error
	for attempt := 0; attempt <= currentSpec.CronMaxRetries; attempt++ {
		if attempt > 0 {
			logger.Info("Retrying cron job execution", "attempt", attempt, "max_retries", currentSpec.CronMaxRetries)
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		cronInstanceID := GenerateCronInstanceID(appName)
		logger := logger.With("cron_instance_id", cronInstanceID)

		err := nr.startAppInstance(context.Background(), appInfoToUse, cronInstanceID, true, time.Duration(currentSpec.CronTimeoutSeconds)*time.Second)
		if err != nil {
			logger.Error("Cron job instance failed to start", "error", err)
			lastErr = err
			continue
		}

		logger.Info("Cron job instance launched. Monitor will handle its lifecycle.", "attempt", attempt)
		lastErr = nil
		break
	}

	if lastErr != nil {
		logger.Error("Cron job execution failed after all retries", "final_error", lastErr)
	}
}
