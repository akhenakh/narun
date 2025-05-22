package noderunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

var runnerVersion = "0.2.1-dev" // Incremented version

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

	// Cron scheduler
	cronScheduler *cron.Cron
	cronJobs      map[string]cron.EntryID // appName -> cron.EntryID
}

// NewNodeRunner creates and initializes a new NodeRunner.
func NewNodeRunner(nodeID, natsURL, dataDir, masterKeyBase64, metricsAddr string, logger *slog.Logger) (*NodeRunner, error) {
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

	// NATS connection
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

	// Bind to Cron Job History KV Store
	kvCronHistory, err := js.CreateOrUpdateKeyValue(setupCtx, jetstream.KeyValueConfig{
		Bucket:      CronJobHistoryKVBucket,
		Description: "Stores execution history of cron jobs.",
		// TTL can be considered for very old history, but pruning is primary mechanism
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to bind/create KV store '%s': %w", CronJobHistoryKVBucket, err)
	}
	logger.Debug("Bound to Cron Job History KV store", "bucket", CronJobHistoryKVBucket)

	gCtx, gCancel := context.WithCancel(context.Background())
	runnerStartTime := time.Now()

	runner := &NodeRunner{
		nodeID:        nodeID,
		nc:            nc,
		js:            js,
		kvAppConfigs:  kvAppConfigs,
		kvNodeStates:  kvNodeStates,
		kvSecrets:     kvSecrets, // Store handle
		kvCronHistory: kvCronHistory,
		masterKey:     masterKey, // Store decoded key
		fileStore:     fileStore, // Store handle for file store
		appBinaries:   osBucket,
		state:         NewAppStateManager(),
		logger:        logger,
		dataDir:       dataDirAbs,
		version:       runnerVersion,
		startTime:     runnerStartTime,
		localOS:       localOS,   // Store detected OS
		localArch:     localArch, //  Store detected Arch
		metricsAddr:   metricsAddr,
		globalCtx:     gCtx,
		globalCancel:  gCancel,

		// Initialize cron scheduler
		cronScheduler: cron.New(cron.WithSeconds()),
		cronJobs:      make(map[string]cron.EntryID),
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

	// Initial Registration and Heartbeat Loop
	if err := nr.updateNodeState("running"); err != nil {
		nr.logger.Error("Initial node state registration failed", "error", err)
	} else {
		nr.logger.Info("Node registered successfully")
	}
	nr.shutdownWg.Add(1)
	go nr.heartbeatLoop()
	nr.logger.Info("Heartbeat loop started", "interval", NodeHeartbeatInterval)

	// Start cron scheduler
	nr.cronScheduler.Start()
	nr.logger.Info("Cron scheduler started")

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

	// Stop the cron scheduler. This prevents new cron jobs from starting.
	// Existing running cron jobs will continue until completion unless explicitly stopped.
	nr.logger.Info("Stopping cron scheduler...")
	cronStopCtx := nr.cronScheduler.Stop() // This returns a context that is done when the scheduler has stopped
	select {
	case <-cronStopCtx.Done():
		nr.logger.Info("Cron scheduler stopped gracefully.")
	case <-time.After(10 * time.Second): // Timeout for scheduler to stop
		nr.logger.Warn("Cron scheduler stop timed out.")
	}

	// Initiate shutdown of managed apps (including any currently running cron job instances)
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

	// Wait for background goroutines (heartbeat, watcher, metrics server if running)
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

// heartbeatLoop
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

		if spec.RunMode == "cron" {
			logger.Info("App configured to run as cron job", "schedule", spec.CronSchedule)
			// Remove any existing cron job for this app
			if oldEntryID, exists := nr.cronJobs[appName]; exists {
				nr.cronScheduler.Remove(oldEntryID)
				delete(nr.cronJobs, appName)
				logger.Info("Removed existing cron entry for app")
			}

			// Add new cron job if schedule is provided
			if spec.CronSchedule != "" {
				// Deep copy spec for the closure
				specCopy := *spec // Shallow copy initially
				// If spec contains slices/maps, they need to be deep copied too.
				// For this example, assuming Env, Args, Nodes, Mounts, Landlock.Paths are relevant.
				specCopy.Args = append([]string(nil), spec.Args...)
				specCopy.Env = make([]EnvVar, len(spec.Env))
				copy(specCopy.Env, spec.Env) // EnvVar itself is struct, direct copy is fine if no pointers
				specCopy.Nodes = make([]NodeSelectorSpec, len(spec.Nodes))
				copy(specCopy.Nodes, spec.Nodes) // NodeSelectorSpec is struct
				specCopy.Mounts = make([]MountSpec, len(spec.Mounts))
				copy(specCopy.Mounts, spec.Mounts) // MountSpec is struct
				specCopy.Landlock.Paths = make([]LandlockPathSpec, len(spec.Landlock.Paths))
				copy(specCopy.Landlock.Paths, spec.Landlock.Paths) // LandlockPathSpec is struct

				entryID, err := nr.cronScheduler.AddFunc(spec.CronSchedule, func() { nr.triggerCronJobExecution(&specCopy) })
				if err != nil {
					logger.Error("Failed to add cron job", "error", err, "schedule", spec.CronSchedule)
				} else {
					nr.cronJobs[appName] = entryID
					logger.Info("Successfully added cron job", "entry_id", entryID, "schedule", spec.CronSchedule)
				}
			} else {
				logger.Info("Cron schedule is empty, no cron job added (effectively disabling cron)")
			}

			// If app was previously a service, stop its instances
			if len(appInfo.instances) > 0 {
				logger.Info("App switching from service to cron. Stopping existing service instances.")
				instancesToCleanup := append([]*ManagedApp{}, appInfo.instances...)
				nr.stopAllInstancesForApp(ctx, appInfo, logger)
				for _, inst := range instancesToCleanup {
					nr.cleanupInstanceMetrics(inst)
				}
				appInfo.instances = make([]*ManagedApp, 0)
			}
		} else { // "service" mode (default or explicit)
			logger.Info("App configured to run as a service")
			// If app was previously a cron job, remove it from scheduler
			if oldEntryID, exists := nr.cronJobs[appName]; exists {
				nr.cronScheduler.Remove(oldEntryID)
				delete(nr.cronJobs, appName)
				logger.Info("Removed cron entry for app (switched to service mode)")
			}

			// Proceed with existing service logic
			targetReplicas := spec.FindTargetReplicas(nr.nodeID)
			currentReplicas := len(appInfo.instances)
			logger.Info("Reconciling service instances", "target", targetReplicas, "current", currentReplicas, "hash_changed", configHashChanged)

			if configHashChanged {
				logger.Info("Configuration hash changed, restarting all service instances")
				instancesToCleanup := append([]*ManagedApp{}, appInfo.instances...)
				nr.stopAllInstancesForApp(ctx, appInfo, logger)
				for _, inst := range instancesToCleanup {
					nr.cleanupInstanceMetrics(inst)
				}
				appInfo.instances = make([]*ManagedApp, 0, targetReplicas)
				currentReplicas = 0
			}

			// Adjust Replicas for service mode
			if targetReplicas > currentReplicas {
				needed := targetReplicas - currentReplicas
				logger.Info("Scaling up service instances", "needed", needed)
				for i := 0; i < needed; i++ {
					replicaIndex := findNextReplicaIndex(appInfo.instances, targetReplicas)
					if replicaIndex == -1 {
						logger.Error("Could not find available replica index slot for service", "target", targetReplicas, "current_count", len(appInfo.instances))
						break
					}
					if err := nr.startAppInstance(ctx, appInfo, replicaIndex); err != nil {
						logger.Error("Failed to start new service instance during scale up", "replica_index", replicaIndex, "error", err)
					} else {
						time.Sleep(100 * time.Millisecond)
					}
				}
			} else if targetReplicas < currentReplicas {
				excess := currentReplicas - targetReplicas
				logger.Info("Scaling down service instances", "excess", excess)
				// ... (existing scale down logic copied here) ...
				sortedInstances := append([]*ManagedApp{}, appInfo.instances...)
				sort.Slice(sortedInstances, func(i, j int) bool {
					idxI := extractReplicaIndex(sortedInstances[i].InstanceID)
					idxJ := extractReplicaIndex(sortedInstances[j].InstanceID)
					return idxI > idxJ
				})
				instancesToStop := make([]*ManagedApp, 0, excess)
				if excess <= len(sortedInstances) {
					instancesToStop = sortedInstances[:excess]
				} else {
					instancesToStop = sortedInstances
				}
				remainingInstancesMap := make(map[string]*ManagedApp)
				for _, inst := range appInfo.instances {
					remainingInstancesMap[inst.InstanceID] = inst
				}
				for _, instToStop := range instancesToStop {
					logger.Info("Stopping excess service instance", "instance_id", instToStop.InstanceID)
					if err := nr.stopAppInstance(instToStop, true); err != nil {
						logger.Error("Error stopping excess service instance", "instance_id", instToStop.InstanceID, "error", err)
					}
					nr.cleanupInstanceMetrics(instToStop)
					delete(remainingInstancesMap, instToStop.InstanceID)
				}
				newInstancesSlice := make([]*ManagedApp, 0, len(remainingInstancesMap))
				for _, inst := range remainingInstancesMap {
					newInstancesSlice = append(newInstancesSlice, inst)
				}
				appInfo.instances = newInstancesSlice
			} else { // target == current AND hash didn't change
				if targetReplicas == 0 {
					logger.Info("Target service replica count is 0. Ensuring no instances are running.")
					if len(appInfo.instances) > 0 {
						instancesToCleanup := append([]*ManagedApp{}, appInfo.instances...)
						nr.stopAllInstancesForApp(ctx, appInfo, logger)
						for _, inst := range instancesToCleanup {
							nr.cleanupInstanceMetrics(inst)
						}
						appInfo.instances = make([]*ManagedApp, 0)
					}
				} else {
					logger.Info("Service instance count matches target and config hash unchanged. No action needed.")
				}
			}
		}

	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		logger.Info("Processing configuration delete/purge request")
		// Remove from cron scheduler if it's a cron job
		if oldEntryID, exists := nr.cronJobs[appName]; exists {
			nr.cronScheduler.Remove(oldEntryID)
			delete(nr.cronJobs, appName)
			logger.Info("Removed cron entry for app due to delete/purge event")
		}
		// Stop any running service instances
		instancesToCleanup := append([]*ManagedApp{}, appInfo.instances...)
		nr.stopAllInstancesForApp(ctx, appInfo, logger)
		for _, inst := range instancesToCleanup {
			nr.cleanupInstanceMetrics(inst)
		}
		appInfo.spec = nil
		appInfo.configHash = ""
		appInfo.instances = make([]*ManagedApp, 0)
		nr.state.DeleteApp(appName)

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

// generateRunID creates a unique ID for a job run.
// For now, using a simple UUID. This can be moved from process.go or made more sophisticated later.
func generateRunID() string {
	// return uuid.NewString() // Requires import "github.com/google/uuid"
	// Simple alternative for now if UUID is not immediately available/desired as a direct import here:
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// triggerCronJobExecution is called by the cron scheduler when a job is due.
func (nr *NodeRunner) triggerCronJobExecution(spec *ServiceSpec) {
	runID := generateRunID()
	scheduledTime := time.Now() // Record the time the job was actually triggered

	logger := nr.logger.With("app", spec.Name, "run_id", runID, "schedule", spec.CronSchedule)
	logger.Info("Cron job triggered by scheduler", "job_max_retries", spec.JobMaxRetries, "job_timeout_sec", spec.JobTimeoutSeconds)

	// Run the job instance. This is non-blocking.
	// The context passed (nr.globalCtx) ensures that if the runner is shutting down,
	// the attempt to start a new job instance can be gracefully handled.
	// Actual job timeout is handled within startJobInstance.
	go func() {
		// TODO: Add logic to check if an instance of this cron job (appName) is already running.
		// If spec.AllowConcurrentRuns (new field, default false) is false, and a job is running, skip this trigger.
		// This check might involve a new map in NodeRunner: `runningCronJobs map[string]string` (appName -> runID)
		// This map would be updated in startJobInstance and monitorJobInstance.

		_, err := nr.startJobInstance(nr.globalCtx, spec, runID, 0, scheduledTime)
		if err != nil {
			// This error is from the attempt to *start* the job (e.g., binary fetch failed).
			// Actual job execution errors are handled by monitorJobInstance.
			logger.Error("Failed to start cron job instance", "error", err)

			// Create and store a failure record if starting itself fails.
			record := &CronJobExecutionRecord{
				AppName:           spec.Name,
				RunID:             runID,
				CronSchedule:      spec.CronSchedule,
				RequestedTime:     scheduledTime, // Or the actual scheduled time from cron library if available
				StartTime:         time.Now(),    // Approximate start time
				EndTime:           time.Now(),
				Status:            "failure_to_start",
				ExitCode:          -1, // Indicate a start failure, not a process exit code
				JobMaxRetries:     spec.JobMaxRetries,
				RetriesAttempted:  0,
				JobTimeoutSeconds: spec.JobTimeoutSeconds,
				ErrorMessage:      fmt.Sprintf("Failed to start job instance: %v", err),
				NodeID:            nr.nodeID,
			}
			nr.storeCronJobExecutionRecord(record)
		}
	}()
}
