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
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	// Added for hashing spec
)

// Hardcode version for now, could be set via ldflags during build
var runnerVersion = "0.1.0-dev"

// NodeRunner manages application processes on a single node.
type NodeRunner struct {
	nodeID       string
	nc           *nats.Conn
	js           jetstream.JetStream
	kvAppConfigs jetstream.KeyValue // Renamed for clarity
	kvNodeStates jetstream.KeyValue // KV store for node states + heartbeats
	appBinaries  jetstream.ObjectStore
	state        *AppStateManager
	logger       *slog.Logger
	dataDir      string
	version      string    // Store the runner's version
	startTime    time.Time // Store the runner's start time
	globalCtx    context.Context
	globalCancel context.CancelFunc
	shutdownWg   sync.WaitGroup
}

// NewNodeRunner creates and initializes a new NodeRunner.
func NewNodeRunner(nodeID, natsURL, dataDir string, logger *slog.Logger) (*NodeRunner, error) {
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
	// Ensure dataDir exists
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

	// Connect to NATS
	nc, err := nats.Connect(natsURL,
		nats.Name(fmt.Sprintf("node-runner-%s", nodeID)),
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil { // Avoid logging nil errors on graceful close
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

	// Get JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}
	logger.Info("JetStream context created")

	// --- Bind to KV Stores ---
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer setupCancel()

	// App Configs KV
	kvAppConfigs, err := js.CreateOrUpdateKeyValue(setupCtx, jetstream.KeyValueConfig{
		Bucket:      AppConfigKVBucket,
		Description: "Stores application service specifications for node runners.",
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to bind/create KV store '%s': %w", AppConfigKVBucket, err)
	}
	logger.Info("Bound to App Config KV store", "bucket", AppConfigKVBucket)

	// Node States KV (with TTL)
	kvNodeStates, err := js.CreateOrUpdateKeyValue(setupCtx, jetstream.KeyValueConfig{
		Bucket:      NodeStateKVBucket,
		Description: "Stores node runner states and heartbeats.",
		TTL:         NodeStateKVTTL, // *** IMPORTANT: Set TTL for heartbeating ***
		History:     1,              // Only need the latest state
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to bind/create KV store '%s': %w", NodeStateKVBucket, err)
	}
	logger.Info("Bound to Node State KV store", "bucket", NodeStateKVBucket, "ttl", NodeStateKVTTL)

	logger.Info("Bound to KV store", "bucket", AppConfigKVBucket)

	// Bind to Object Store
	osBucket, err := js.CreateOrUpdateObjectStore(setupCtx, jetstream.ObjectStoreConfig{
		Bucket:      AppBinariesOSBucket,
		Description: "Stores application binaries.",
		// TTL: 0, // Binaries usually don't expire by default
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
		kvAppConfigs: kvAppConfigs, // Use renamed field
		kvNodeStates: kvNodeStates, // Store the new KV handle
		appBinaries:  osBucket,
		state:        NewAppStateManager(),
		logger:       logger,
		dataDir:      dataDirAbs,
		version:      runnerVersion,   // Store version
		startTime:    runnerStartTime, // Store start time
		globalCtx:    gCtx,
		globalCancel: gCancel,
	}

	return runner, nil
}

// Run starts the node runner's main loop, watching for configuration changes.
func (nr *NodeRunner) Run() error {
	nr.logger.Info("Starting node runner")
	defer nr.logger.Info("Node runner stopped")

	// --- Initial Registration ---
	// Do an initial heartbeat/registration write immediately
	if err := nr.updateNodeState("running"); err != nil {
		// Log error but continue; heartbeat loop will retry
		nr.logger.Error("Initial node state registration failed", "error", err)
	} else {
		nr.logger.Info("Node registered successfully")
	}

	// --- Start Heartbeat Loop ---
	nr.shutdownWg.Add(1)
	go nr.heartbeatLoop()
	nr.logger.Info("Heartbeat loop started", "interval", NodeHeartbeatInterval)

	// Initial sync: Get all existing configs and start apps
	// Use a timeout for the initial sync
	syncCtx, syncCancel := context.WithTimeout(nr.globalCtx, 30*time.Second)
	if err := nr.syncAllApps(syncCtx); err != nil {
		nr.logger.Error("Failed initial app synchronization", "error", err)
		// Continue running to watch for future updates? Or return error?
		// Let's continue, maybe the issue was temporary.
	}
	syncCancel() // Release context resources

	// Watch for configuration changes
	// Corrected WatchAll call: Options are passed without arguments if needed.
	// Default behavior is IncludeDeletes=true, UpdatesOnly=false (send initial values)
	// which matches the original intent of IgnoreDeletes(false), UpdatesOnly(false).
	watcher, err := nr.kvAppConfigs.WatchAll(nr.globalCtx /* No options needed */)
	if err != nil {
		// If context was already canceled, it's not an unexpected error
		if nr.globalCtx.Err() != nil {
			nr.logger.Info("Failed to start KV watcher due to shutdown signal")
			return nr.globalCtx.Err()
		}
		return fmt.Errorf("failed to start KV watcher on '%s': %w", AppConfigKVBucket, err)
	}
	defer watcher.Stop() // Ensure watcher is stopped on exit

	nr.logger.Info("Started watching for app configuration changes", "bucket", AppConfigKVBucket)

	// Add self to waitgroup for the watcher goroutine
	nr.shutdownWg.Add(1)
	go func() {
		defer nr.shutdownWg.Done()
		for {
			select {
			case <-nr.globalCtx.Done():
				nr.logger.Info("Configuration watcher stopping due to context cancellation.")
				// Ensure watcher is stopped cleanly
				if err := watcher.Stop(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
					nr.logger.Warn("Error stopping KV watcher", "error", err)
				}
				return

			// Corrected way to check for watcher updates and closure
			case entry, ok := <-watcher.Updates():
				if !ok { // Channel closed
					nr.logger.Warn("KV Watcher updates channel closed.")
					// Check if shutdown was intended via global context
					if nr.globalCtx.Err() == nil {
						nr.logger.Error("Watcher closed unexpectedly while runner context is still active.")
						// Signal global shutdown as a core component failed
						nr.globalCancel()
					} else {
						nr.logger.Info("Watcher closed during shutdown.")
					}
					return // Exit the goroutine
				}

				// Process the entry
				if entry == nil {
					// Initial marker received or potential issue, often safe to ignore initial nils
					nr.logger.Debug("Received nil update from watcher (initial scan complete or marker)")
					continue
				}
				nr.handleAppConfigUpdate(entry)

				// Removed watcher.Done case as it doesn't exist
			}
		}
	}()

	// Wait for global context to be cancelled (e.g., by signal or internal error)
	<-nr.globalCtx.Done()
	nr.logger.Info("Shutdown signal received, initiating shutdown...")

	// Update node state to "shutting_down" (best effort)
	if err := nr.updateNodeState("shutting_down"); err != nil {
		nr.logger.Warn("Failed to update node state to shutting_down", "error", err)
	}

	// Initiate shutdown of managed apps (no change here)
	nr.shutdownApps()

	// Stop the watcher explicitly if not already stopped by context cancellation
	if err := watcher.Stop(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) && !errors.Is(err, context.Canceled) {
		nr.logger.Warn("Error stopping watcher during shutdown", "error", err)
	}

	// Wait for watcher and heartbeat goroutines to finish
	nr.logger.Debug("Waiting for background goroutines to finish...")
	nr.shutdownWg.Wait()
	nr.logger.Debug("Background goroutines finished.")

	// Explicitly delete node state on graceful shutdown
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer deleteCancel()
	nr.logger.Info("Deleting node state from KV store", "key", nr.nodeID)
	if err := nr.kvNodeStates.Delete(deleteCtx, nr.nodeID); err != nil {
		// Log error, but don't fail shutdown for this
		nr.logger.Error("Failed to delete node state from KV", "key", nr.nodeID, "error", err)
	}

	// --- Corrected NATS Connection Drain ---
	if nr.nc != nil && !nr.nc.IsClosed() {
		nr.logger.Info("Draining NATS connection...")
		drainTimeout := 10 * time.Second // Timeout for drain operation
		drainDone := make(chan error, 1)

		// Run Drain in a goroutine
		go func() {
			drainDone <- nr.nc.Drain()
		}()

		// Wait for Drain to complete or timeout
		select {
		case err := <-drainDone:
			if err != nil {
				nr.logger.Error("Error during NATS connection drain", "error", err)
				// Force close if drain reported an error
				nr.nc.Close()
			} else {
				nr.logger.Info("NATS connection drained successfully.")
				// nc.Close() is implicitly called by successful Drain, no need to call it again.
			}
		case <-time.After(drainTimeout):
			nr.logger.Warn("NATS connection drain timed out. Forcing close.", "timeout", drainTimeout)
			// Force close the connection if drain takes too long
			nr.nc.Close()
		}
		nr.logger.Info("NATS connection closed.")
	}
	// --- End Corrected NATS Connection Drain ---

	// Return the error from the global context (like context.Canceled)
	// Avoid masking it if it exists
	if nr.globalCtx.Err() != nil && nr.globalCtx.Err() != context.Canceled {
		return fmt.Errorf("runner stopped due to context error: %w", nr.globalCtx.Err())
	} else if nr.globalCtx.Err() == context.Canceled {
		return nil // Clean shutdown signaled by context cancellation
	}

	return nil // Should ideally not be reached if context manages lifecycle
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
				// Consider more robust error handling? Maybe retry once?
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
	// Get current list of managed apps (read lock)
	managedAppsMap := nr.state.GetAll() // Gets a copy
	managedAppNames := make([]string, 0, len(managedAppsMap))
	for name := range managedAppsMap {
		managedAppNames = append(managedAppNames, name)
	}
	sort.Strings(managedAppNames) // Sort for consistent output

	// Construct state
	state := NodeState{
		NodeID:      nr.nodeID,
		LastSeen:    time.Now(),
		Version:     nr.version,
		StartTime:   nr.startTime,
		ManagedApps: managedAppNames,
		Status:      status, // Use provided status
	}

	stateData, err := json.Marshal(state)
	if err != nil {
		// This should not happen with our struct
		return fmt.Errorf("failed to marshal node state: %w", err)
	}

	// Use a short timeout for the KV Put operation
	putCtx, putCancel := context.WithTimeout(context.Background(), NodeHeartbeatInterval/2) // Use background, not globalCtx
	defer putCancel()

	// Put the state into KV. The TTL is handled by the bucket configuration.
	_, err = nr.kvNodeStates.Put(putCtx, nr.nodeID, stateData)
	if err != nil {
		return fmt.Errorf("failed to put node state to KV '%s': %w", NodeStateKVBucket, err)
	}
	return nil
}

// syncAllApps gets all current configurations and ensures apps are running.
// It now uses the provided context properly.
func (nr *NodeRunner) syncAllApps(ctx context.Context) error {
	nr.logger.Info("Performing initial synchronization of all applications...")
	keysLister, err := nr.kvAppConfigs.ListKeys(ctx) // Use ListKeys for streaming
	if err != nil {
		// Don't treat ErrNoKeysFound as a fatal error for sync
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			nr.logger.Info("No existing app configurations found in KV store.")
			return nil
		}
		return fmt.Errorf("failed to start listing keys from KV store '%s': %w", AppConfigKVBucket, err)
	}
	defer keysLister.Stop() // Ensure the lister resources are cleaned up

	syncWg := sync.WaitGroup{}
	syncErrChan := make(chan error, 1) // Buffer of 1 is enough

	keyChan := keysLister.Keys()

syncLoop:
	for {
		select {
		case <-ctx.Done():
			nr.logger.Warn("Initial synchronization canceled or timed out.", "error", ctx.Err())
			return ctx.Err()
		case key, ok := <-keyChan:
			if !ok { // Channel closed, all keys processed or Stop() called
				break syncLoop
			}
			if key == "" { // Should not happen, but guard
				continue
			}

			syncWg.Add(1)
			go func(k string) {
				defer syncWg.Done()
				entryCtx, entryCancel := context.WithTimeout(ctx, 10*time.Second) // Timeout per entry get
				defer entryCancel()
				entry, err := nr.kvAppConfigs.Get(entryCtx, k)
				if err != nil {
					// Log but don't stop the whole sync for one bad key
					nr.logger.Warn("Failed to get KV entry during sync", "key", k, "error", err)
					// Report first error encountered
					select {
					case syncErrChan <- fmt.Errorf("failed getting key %s: %w", k, err):
					default: // Avoid blocking if channel full
					}
					return
				}
				// Use the runner's global context for handleAppConfigUpdate
				// as starting an app might take longer than the sync timeout.
				nr.handleAppConfigUpdate(entry)
			}(key)
		}
	}

	syncWg.Wait()      // Wait for all Get/handle goroutines to finish
	close(syncErrChan) // Close error chan after WaitGroup

	// Check if any error occurred during sync
	firstErr := <-syncErrChan // Read the first error (or nil if chan empty)

	if firstErr != nil {
		nr.logger.Error("Errors occurred during initial synchronization", "first_error", firstErr)
		// Decide whether to return the error or just log it. Let's return it.
		return fmt.Errorf("initial sync encountered errors: %w", firstErr)
	}

	nr.logger.Info("Initial synchronization complete.")
	return nil
}

// handleAppConfigUpdate processes a single KV entry update (PUT or DELETE).
func (nr *NodeRunner) handleAppConfigUpdate(entry jetstream.KeyValueEntry) {
	// Make sure we are processing updates from the *App Config* bucket
	if entry.Bucket() != AppConfigKVBucket {
		nr.logger.Warn("Received KV update from unexpected bucket", "bucket", entry.Bucket())
		return
	}

	appName := entry.Key()
	// Add entry details to logger context for better traceability
	logger := nr.logger.With("app", appName, "kv_revision", entry.Revision(), "kv_operation", entry.Operation().String())

	switch entry.Operation() {
	case jetstream.KeyValuePut:
		logger.Info("Received configuration update")
		spec, err := ParseServiceSpec(entry.Value())
		if err != nil {
			logger.Error("Failed to parse service spec from KV", "error", err)
			// TODO: Publish a failure status?
			nr.publishStatusUpdate(appName, StatusFailed, nil, nil, fmt.Sprintf("Failed to parse spec: %v", err))
			return
		}
		if spec.Name != appName {
			logger.Error("Service name in spec does not match KV key", "spec_name", spec.Name, "key", appName)
			// TODO: Publish a failure status?
			nr.publishStatusUpdate(appName, StatusFailed, nil, nil, fmt.Sprintf("Spec name '%s' mismatch key '%s'", spec.Name, appName))
			return
		}

		// Get existing state to pass to startApp for comparison/restart logic
		existingApp, _ := nr.state.Get(appName)

		// startApp handles starting/updating/restarting
		// Use runner's global context because starting can take time
		if err := nr.startApp(nr.globalCtx, spec, existingApp); err != nil {
			logger.Error("Failed to start or update application", "error", err)
			// Status should have been updated by startApp/monitorApp
		} else {
			logger.Info("Application started or updated successfully triggered by KV update")
		}

	case jetstream.KeyValueDelete, jetstream.KeyValuePurge: // Treat Purge same as Delete
		logger.Info("Received configuration delete/purge request")
		// Use runner's global context for stop operation as well
		if err := nr.stopApp(appName, true); err != nil { // Intentional stop
			logger.Error("Failed to stop application", "error", err)
		} else {
			logger.Info("Application stopped successfully triggered by KV delete/purge")
		}
		// Remove state after stopping
		nr.state.Delete(appName)
		logger.Info("Application state removed")
		// TODO: Optional: Clean up local binary/workdir?
		// binaryPath := filepath.Join(nr.dataDir, "binaries", appName) // Requires consistent naming
		// workDir := filepath.Join(nr.dataDir, appName, "work")
		// logger.Info("Attempting to clean up local directories", "binary", binaryPath, "work", workDir)
		// if err := os.RemoveAll(filepath.Join(nr.dataDir, appName)); err != nil { // Remove parent dir <dataDir>/<appName>
		// 	logger.Warn("Failed to clean up local directories", "app", appName, "error", err)
		// }

	default:
		logger.Warn("Ignoring unknown KV operation") // Already logged in logger context
	}
}

// shutdownApps iterates through managed apps and stops them.
func (nr *NodeRunner) shutdownApps() {
	nr.logger.Info("Stopping all managed applications...")
	appsToStop := nr.state.GetAll() // Get a copy of the map

	var wg sync.WaitGroup
	// Create a context specifically for shutdown, derived from global, but with a timeout
	// shutdownCtx, cancel := context.WithTimeout(nr.globalCtx, StopTimeout*2) // Give ample time? Or use global? Let's use global for now.
	// defer cancel()

	for name := range appsToStop {
		wg.Add(1)
		go func(appName string) {
			defer wg.Done()
			logger := nr.logger.With("app", appName)
			logger.Info("Initiating shutdown")
			// Pass the runner's global context, stopApp will handle internal timeouts
			if err := nr.stopApp(appName, true); err != nil { // Intentional stop
				logger.Error("Error during shutdown", "error", err)
			} else {
				logger.Info("Shutdown completed")
			}
		}(name)
	}
	wg.Wait()
	nr.logger.Info("Finished stopping all managed applications.")
}

// Stop signals the NodeRunner to shut down gracefully.
func (nr *NodeRunner) Stop() {
	nr.logger.Info("Received external stop signal")
	// Cancel the global context, which will trigger shutdown procedures
	// Ensure cancel is only called once
	select {
	case <-nr.globalCtx.Done():
		// Already shutting down
	default:
		nr.globalCancel()
	}
}
