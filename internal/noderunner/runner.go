// narun/internal/noderunner/runner.go
package noderunner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime" // Import runtime
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var runnerVersion = "0.2.1-dev" // Incremented version

// NodeRunner manages application processes on a single node.
type NodeRunner struct {
	nodeID       string
	nc           *nats.Conn
	js           jetstream.JetStream
	kvAppConfigs jetstream.KeyValue
	kvNodeStates jetstream.KeyValue
	appBinaries  jetstream.ObjectStore
	state        *AppStateManager // Updated state manager
	logger       *slog.Logger
	dataDir      string
	version      string
	startTime    time.Time
	localOS      string // ** NEW: Detected OS **
	localArch    string // ** NEW: Detected Arch **
	globalCtx    context.Context
	globalCancel context.CancelFunc
	shutdownWg   sync.WaitGroup
}

// NewNodeRunner creates and initializes a new NodeRunner.
func NewNodeRunner(nodeID, natsURL, dataDir string, logger *slog.Logger) (*NodeRunner, error) {
	// ... (hostname, dataDir, logger setup identical) ...
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

	// ** Detect Local Platform **
	localOS := runtime.GOOS
	localArch := runtime.GOARCH
	logger.Info("Detected local platform", "goos", localOS, "goarch", localArch)

	// ... (NATS connection setup identical) ...
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
	logger.Info("Connected to NATS", "url", natsURL)

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}
	logger.Info("JetStream context created")

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
	logger.Info("Bound to App Config KV store", "bucket", AppConfigKVBucket)
	kvNodeStates, err := js.CreateOrUpdateKeyValue(setupCtx, jetstream.KeyValueConfig{
		Bucket: NodeStateKVBucket, Description: "Stores node runner states and heartbeats.", TTL: NodeStateKVTTL, History: 1,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to bind/create KV store '%s': %w", NodeStateKVBucket, err)
	}
	logger.Info("Bound to Node State KV store", "bucket", NodeStateKVBucket, "ttl", NodeStateKVTTL)

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
		appBinaries:  osBucket,
		state:        NewAppStateManager(),
		logger:       logger,
		dataDir:      dataDirAbs,
		version:      runnerVersion,
		startTime:    runnerStartTime,
		localOS:      localOS,   // ** Store detected OS **
		localArch:    localArch, // ** Store detected Arch **
		globalCtx:    gCtx,
		globalCancel: gCancel,
	}

	return runner, nil
}

// Run starts the node runner's main loop. (Watch loop structure unchanged, but sync/handle changes)
func (nr *NodeRunner) Run() error {
	nr.logger.Info("Starting node runner", "version", nr.version, "goos", nr.localOS, "goarch", nr.localArch) // Log platform
	defer nr.logger.Info("Node runner stopped")

	// Initial Registration and Heartbeat Loop (Unchanged setup, uses updated updateNodeState)
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
	defer watcher.Stop()
	nr.logger.Info("Started watching for app configuration changes", "bucket", AppConfigKVBucket)

	// Watcher Loop
	nr.shutdownWg.Add(1)
	go func() {
		defer nr.shutdownWg.Done()
		for {
			select {
			case <-nr.globalCtx.Done():
				nr.logger.Info("Configuration watcher stopping due to context cancellation.")
				if err := watcher.Stop(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
					nr.logger.Warn("Error stopping KV watcher", "error", err)
				}
				return
			case entry, ok := <-watcher.Updates():
				if !ok {
					nr.logger.Warn("KV Watcher updates channel closed.")
					if nr.globalCtx.Err() == nil {
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

	// Update node state (Unchanged setup, uses updated updateNodeState)
	if err := nr.updateNodeState("shutting_down"); err != nil {
		nr.logger.Warn("Failed to update node state to shutting_down", "error", err)
	}

	// Initiate shutdown of managed apps
	nr.shutdownAllAppInstances()

	// Stop watcher
	if err := watcher.Stop(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) && !errors.Is(err, context.Canceled) {
		nr.logger.Warn("Error stopping watcher during shutdown", "error", err)
	}

	// Wait for background goroutines
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
	if nr.globalCtx.Err() != nil && nr.globalCtx.Err() != context.Canceled {
		return fmt.Errorf("runner stopped due to context error: %w", nr.globalCtx.Err())
	}
	return nil
}

// heartbeatLoop (Unchanged logic, uses updated updateNodeState)
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
		GOOS:             nr.localOS,   // ** Include OS **
		GOARCH:           nr.localArch, // ** Include Arch **
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
	errChan := make(chan error, 1)

	keysChan := keysLister.Keys()

syncLoop:
	for {
		select {
		case <-ctx.Done():
			nr.logger.Warn("Initial synchronization canceled or timed out.", "error", ctx.Err())
			keysLister.Stop() // Ensure lister is stopped on timeout/cancel
			// Wait for any already running goroutines before returning
			syncWg.Wait()
			close(errChan)
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
				entryCtx, entryCancel := context.WithTimeout(ctx, 20*time.Second)
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
						default:
						}
					}
					return
				}
				nr.handleAppConfigUpdate(entryCtx, entry) // Process the update
			}(key)
		}
	}

	syncWg.Wait()
	close(errChan)

	if firstErr := <-errChan; firstErr != nil {
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
			nr.stopAllInstancesForApp(ctx, appInfo, nr.logger.With("app", appName))
			// Clear state within appInfo
			appInfo.spec = nil
			appInfo.configHash = ""
			appInfo.instances = make([]*ManagedApp, 0)
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
			nr.stopAllInstancesForApp(ctx, appInfo, logger)
			appInfo.spec = nil
			appInfo.configHash = ""
			return
		}
		if spec.Name != appName {
			logger.Error("Service name in spec does not match KV key", "spec_name", spec.Name, "key", appName)
			nr.stopAllInstancesForApp(ctx, appInfo, logger)
			appInfo.spec = nil
			appInfo.configHash = ""
			return
		}

		newConfigHash, err := calculateSpecHash(spec)
		if err != nil {
			logger.Error("Failed to hash service spec", "error", err)
			nr.stopAllInstancesForApp(ctx, appInfo, logger)
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
			nr.stopAllInstancesForApp(ctx, appInfo, logger)
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
				// ** Pass BinaryVersionTag to startAppInstance **
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
				return idxI > idxJ
			})
			instancesToStop := make([]*ManagedApp, 0, excess)
			if excess <= len(sortedInstances) {
				instancesToStop = sortedInstances[:excess]
			} else {
				logger.Warn("Excess count greater than sorted instances, stopping all.", "excess", excess, "count", len(sortedInstances))
				instancesToStop = sortedInstances
			}
			remainingInstances := make([]*ManagedApp, 0, targetReplicas)
			stopMap := make(map[string]bool)
			for _, inst := range instancesToStop {
				stopMap[inst.InstanceID] = true
				logger.Info("Stopping excess instance", "instance_id", inst.InstanceID)
				go func(instanceToStop *ManagedApp) {
					if err := nr.stopAppInstance(instanceToStop, true); err != nil {
						logger.Error("Error stopping excess instance", "instance_id", instanceToStop.InstanceID, "error", err)
					}
				}(inst)
			}
			for _, inst := range appInfo.instances {
				if !stopMap[inst.InstanceID] {
					remainingInstances = append(remainingInstances, inst)
				}
			}
			appInfo.instances = remainingInstances // Update the main slice
		} else {
			// target == current AND hash didn't change
			if targetReplicas == 0 {
				logger.Info("Target replica count is 0. Ensuring no instances are running.")
				if len(appInfo.instances) > 0 {
					nr.stopAllInstancesForApp(ctx, appInfo, logger)
					appInfo.instances = make([]*ManagedApp, 0)
				}
			} else {
				logger.Info("Instance count matches target and config hash unchanged. No action needed.")
			}
		}

	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		logger.Info("Processing configuration delete/purge request")
		nr.stopAllInstancesForApp(ctx, appInfo, logger) // Stop all instances
		appInfo.spec = nil                              // Clear spec
		appInfo.configHash = ""
		appInfo.instances = make([]*ManagedApp, 0) // Clear instances slice
		// Delete the app entry from the state manager itself upon delete/purge
		nr.state.DeleteApp(appName) // Call this *after* lock is released by defer

	default:
		logger.Warn("Ignoring unknown KV operation")
	}
}

// Helper to stop all running/starting instances for a specific app
func (nr *NodeRunner) stopAllInstancesForApp(ctx context.Context, appInfo *appInfo, logger *slog.Logger) {
	logger.Info("Stopping all instances for app")
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
		info.mu.RLock() // Lock read to get instances safely
		instancesToStop := append([]*ManagedApp{}, info.instances...)
		info.mu.RUnlock() // Unlock after copying

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
