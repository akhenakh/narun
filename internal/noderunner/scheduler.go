package noderunner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

const (
	AppSchedulesKVBucket = "app-schedules"
	SystemLocksKVBucket  = "system-locks"
	SchedulerLockKey     = "scheduler-leader"
	LockTTL              = 15 * time.Second
)

// AppSchedule defines the actual assignment of replicas to nodes.
type AppSchedule struct {
	AppName     string         `json:"app_name"`
	ConfigHash  string         `json:"config_hash"`
	Assignments map[string]int `json:"assignments"`       // NodeID -> Number of Replicas to run
	Pending     map[string]int `json:"pending,omitempty"` // NodeID -> Replicas waiting for offline node
}

type Scheduler struct {
	nodeID       string
	js           jetstream.JetStream
	kvAppConfigs jetstream.KeyValue
	kvAppScheds  jetstream.KeyValue
	kvNodeStates jetstream.KeyValue
	kvLocks      jetstream.KeyValue
	appBinaries  jetstream.ObjectStore // For checking available platforms
	logger       *slog.Logger

	isLeader atomic.Bool
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	// We use a channel to trigger reconciliation to debounce rapid events
	triggerReconcile chan struct{}
}

func NewScheduler(nodeID string, js jetstream.JetStream, logger *slog.Logger) (*Scheduler, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize new KV buckets required for scheduling
	kvAppScheds, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      AppSchedulesKVBucket,
		Description: "Stores calculated placements of apps to nodes.",
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create schedules KV: %w", err)
	}

	kvLocks, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      SystemLocksKVBucket,
		Description: "System locks for leader election.",
		TTL:         LockTTL,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create locks KV: %w", err)
	}

	// Re-bind existing KVs so the scheduler has access
	kvAppConfigs, _ := js.KeyValue(ctx, AppConfigKVBucket)
	kvNodeStates, _ := js.KeyValue(ctx, NodeStateKVBucket)

	// Bind to app-binaries object store
	appBinaries, err := js.ObjectStore(ctx, AppBinariesOSBucket)
	if err != nil {
		appBinaries, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: AppBinariesOSBucket})
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create app-binaries object store: %w", err)
		}
	}

	s := &Scheduler{
		nodeID:           nodeID,
		js:               js,
		kvAppConfigs:     kvAppConfigs,
		kvAppScheds:      kvAppScheds,
		kvNodeStates:     kvNodeStates,
		kvLocks:          kvLocks,
		appBinaries:      appBinaries,
		logger:           logger.With("component", "scheduler"),
		ctx:              ctx,
		cancel:           cancel,
		triggerReconcile: make(chan struct{}, 1), // buffered channel
	}

	return s, nil
}

// Start begins the leader election loop.
func (s *Scheduler) Start() {
	s.wg.Add(1)
	go s.leaderElectionLoop()
}

func (s *Scheduler) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *Scheduler) tryLock() {
	entry, err := s.kvLocks.Get(s.ctx, SchedulerLockKey)
	if err != nil {
		// Lock doesn't exist, try to acquire it
		_, err := s.kvLocks.Create(s.ctx, SchedulerLockKey, []byte(s.nodeID))
		if err == nil {
			s.becomeLeader()
		} else {
			s.stepDown()
		}
	} else {
		// Lock exists
		if string(entry.Value()) == s.nodeID {
			// We are the leader, renew the lock
			_, err := s.kvLocks.Update(s.ctx, SchedulerLockKey, []byte(s.nodeID), entry.Revision())
			if err == nil {
				s.becomeLeader()
			} else {
				s.stepDown()
			}
		} else {
			// Someone else is the leader
			s.stepDown()
		}
	}
}

func (s *Scheduler) leaderElectionLoop() {
	defer s.wg.Done()

	// Try acquiring the lock immediately so we don't wait for the first tick
	s.tryLock()

	ticker := time.NewTicker(LockTTL / 3) // Renew lock every 5s
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			// Release lock on shutdown if we are the leader
			if s.isLeader.Load() {
				_ = s.kvLocks.Delete(context.Background(), SchedulerLockKey)
			}
			return
		case <-ticker.C:
			s.tryLock()
		}
	}
}

func (s *Scheduler) becomeLeader() {
	if !s.isLeader.Swap(true) {
		s.logger.Info("Acquired scheduler leadership. Starting reconciliation loop.")
		s.wg.Add(1)
		go s.reconciliationLoop()
		s.trigger() // Initial reconciliation
	}
}

func (s *Scheduler) stepDown() {
	if s.isLeader.Swap(false) {
		s.logger.Info("Lost scheduler leadership. Stepping down.")
	}
}

// trigger safely sends a signal to reconcile without blocking
func (s *Scheduler) trigger() {
	select {
	case s.triggerReconcile <- struct{}{}:
	default:
		// already triggered
	}
}

func (s *Scheduler) reconciliationLoop() {
	defer s.wg.Done()

	// Set up NATS KV watchers for state changes
	appWatcher, err := s.kvAppConfigs.WatchAll(s.ctx)
	if err != nil {
		s.logger.Error("Failed to watch app configs", "error", err)
		return
	}
	defer appWatcher.Stop()

	nodeWatcher, err := s.kvNodeStates.WatchAll(s.ctx)
	if err != nil {
		s.logger.Error("Failed to watch node states", "error", err)
		return
	}
	defer nodeWatcher.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-appWatcher.Updates():
			s.trigger()
		case <-nodeWatcher.Updates():
			s.trigger()
		case <-s.triggerReconcile:
			if !s.isLeader.Load() {
				return
			}
			s.reconcile()
		}
	}
}

func (s *Scheduler) reconcile() {
	s.logger.Info("Reconciling cluster state...")

	// 1. Get all healthy nodes with their platform info
	nodeStates := s.getHealthyNodeStates()
	if len(nodeStates) == 0 {
		s.logger.Warn("No healthy nodes available for scheduling")
		return
	}

	// 2. Get all apps
	apps := s.getAllApps()

	// 3. Calculate and publish schedule for each app
	for _, app := range apps {
		hash, err := calculateSpecHash(&app)
		if err != nil {
			s.logger.Error("Failed to calculate spec hash", "app", app.Name, "error", err)
			continue
		}

		// Get available platforms for this app from object store
		availablePlatforms := s.getAvailablePlatforms(app.Name, app.Tag)
		if len(availablePlatforms) == 0 {
			s.logger.Warn("No binaries found for app", "app", app.Name, "tag", app.Tag)
			continue
		}

		schedule := AppSchedule{
			AppName:     app.Name,
			ConfigHash:  hash,
			Assignments: make(map[string]int),
			Pending:     make(map[string]int),
		}

		// A. Process explicitly targeted nodes (pinned replicas)
		pinnedReplicas := app.GetPinnedReplicas()
		for nodeID, replicas := range pinnedReplicas {
			nodeState, isHealthy := nodeStates[nodeID]
			if !isHealthy {
				// Node is offline - mark as pending, don't redistribute
				schedule.Pending[nodeID] = replicas
				s.logger.Warn("Targeted node is offline. Replicas pending.", "node", nodeID, "app", app.Name, "replicas", replicas)
				continue
			}
			// Check if node has compatible platform
			if !hasCompatiblePlatform(nodeState.OS, nodeState.Arch, availablePlatforms) {
				schedule.Pending[nodeID] = replicas
				s.logger.Warn("Targeted node has incompatible platform. Replicas pending.",
					"node", nodeID, "app", app.Name,
					"node_platform", fmt.Sprintf("%s/%s", nodeState.OS, nodeState.Arch),
					"available_platforms", availablePlatforms)
				continue
			}
			schedule.Assignments[nodeID] += replicas
		}

		// B. Distribute balanced replicas across compatible healthy nodes
		balancedReplicas := app.GetBalancedReplicas()
		if balancedReplicas > 0 {
			// Filter nodes to only those with compatible platforms
			compatibleNodes := getCompatibleNodes(nodeStates, availablePlatforms)
			if len(compatibleNodes) == 0 {
				s.logger.Warn("No compatible nodes for balanced replicas",
					"app", app.Name,
					"available_platforms", availablePlatforms)
			} else {
				// Sort nodes alphabetically to ensure deterministic round-robin
				sort.Strings(compatibleNodes)
				for i := 0; i < balancedReplicas; i++ {
					targetNode := compatibleNodes[i%len(compatibleNodes)]
					schedule.Assignments[targetNode]++
				}
			}
		}

		// Clean up empty pending map
		if len(schedule.Pending) == 0 {
			schedule.Pending = nil
		}

		// 4. Save schedule to NATS
		scheduleData, err := json.Marshal(schedule)
		if err != nil {
			s.logger.Error("Failed to marshal schedule", "app", app.Name, "error", err)
			continue
		}

		_, err = s.kvAppScheds.Put(s.ctx, app.Name, scheduleData)
		if err != nil {
			s.logger.Error("Failed to write schedule", "app", app.Name, "error", err)
		} else {
			s.logger.Info("Updated schedule", "app", app.Name, "assignments", schedule.Assignments, "pending", schedule.Pending)
		}
	}
}

// getHealthyNodeStates returns a map of nodeID -> NodeState for all healthy nodes
func (s *Scheduler) getHealthyNodeStates() map[string]*NodeState {
	lister, err := s.kvNodeStates.ListKeys(s.ctx)
	if err != nil {
		s.logger.Warn("Failed to list healthy nodes", "error", err)
		return nil
	}
	defer lister.Stop()

	states := make(map[string]*NodeState)
	for k := range lister.Keys() {
		entry, err := s.kvNodeStates.Get(s.ctx, k)
		if err != nil {
			continue
		}
		var state NodeState
		if err := json.Unmarshal(entry.Value(), &state); err == nil {
			states[k] = &state
		}
	}
	return states
}

// getAvailablePlatforms returns the list of os/arch platforms available for an app
func (s *Scheduler) getAvailablePlatforms(appName, tag string) []Platform {
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()

	// List objects with prefix "{appName}-{tag}-"
	prefix := fmt.Sprintf("%s-%s-", appName, tag)
	var platforms []Platform

	objects, err := s.appBinaries.List(ctx)
	if err != nil {
		s.logger.Debug("Failed to list binaries", "error", err)
		return nil
	}

	for _, obj := range objects {
		// Check if object name starts with prefix
		if !strings.HasPrefix(obj.Name, prefix) {
			continue
		}
		// Extract OS and Arch from object metadata
		if obj.Metadata != nil {
			goos := obj.Metadata["narun-goos"]
			goarch := obj.Metadata["narun-goarch"]
			if goos != "" && goarch != "" {
				platforms = append(platforms, Platform{OS: goos, Arch: goarch})
			}
		}
	}
	return platforms
}

// Platform represents an os/arch combination
type Platform struct {
	OS   string
	Arch string
}

// hasCompatiblePlatform checks if a node's platform matches any available platform
func hasCompatiblePlatform(nodeOS, nodeArch string, platforms []Platform) bool {
	for _, p := range platforms {
		if p.OS == nodeOS && p.Arch == nodeArch {
			return true
		}
	}
	return false
}

// getCompatibleNodes returns a list of node IDs that have compatible platforms
func getCompatibleNodes(nodeStates map[string]*NodeState, platforms []Platform) []string {
	var compatible []string
	for nodeID, state := range nodeStates {
		if hasCompatiblePlatform(state.OS, state.Arch, platforms) {
			compatible = append(compatible, nodeID)
		}
	}
	return compatible
}

func (s *Scheduler) getAllApps() []ServiceSpec {
	lister, err := s.kvAppConfigs.ListKeys(s.ctx)
	if err != nil {
		s.logger.Warn("Failed to list app configs", "error", err)
		return nil
	}
	defer lister.Stop()

	var apps []ServiceSpec
	for key := range lister.Keys() {
		entry, err := s.kvAppConfigs.Get(s.ctx, key)
		if err != nil {
			continue
		}
		var spec ServiceSpec
		if err := yaml.Unmarshal(entry.Value(), &spec); err == nil {
			apps = append(apps, spec)
		}
	}
	return apps
}
