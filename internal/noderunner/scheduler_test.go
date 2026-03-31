package noderunner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

// setupTestNATS starts an in-memory NATS server with JetStream enabled
// and initializes the required KV buckets for the tests.
func setupTestNATS(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()

	storeDir := t.TempDir()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1, // Random port
		JetStream: true,
		StoreDir:  storeDir,
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Failed to create NATS server: %v", err)
	}

	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatalf("NATS server not ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream context: %v", err)
	}

	ctx := context.Background()

	// Pre-create standard KVs
	_, _ = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: AppConfigKVBucket})
	_, _ = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: NodeStateKVBucket})

	// Pre-create app-binaries object store
	_, _ = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: AppBinariesOSBucket})

	// Pre-create System Locks with a much shorter TTL just for testing leader election speed
	// Note: The Scheduler attempts to create it with LockTTL (15s), but if it exists, it uses the existing one.
	_, _ = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: SystemLocksKVBucket,
		TTL:    3 * time.Second, // Shorter TTL for faster tests
	})

	return ns, nc, js
}

func mockNodeState(t *testing.T, js jetstream.JetStream, nodeID string) {
	t.Helper()
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, NodeStateKVBucket)
	if err != nil {
		t.Fatalf("Failed to get node state KV: %v", err)
	}
	// Default to linux/amd64 for backward compatibility with existing tests
	state := NodeState{NodeID: nodeID, Status: "running", OS: "linux", Arch: "amd64"}
	b, _ := json.Marshal(state)
	_, err = kv.Put(ctx, nodeID, b)
	if err != nil {
		t.Fatalf("Failed to put node state: %v", err)
	}
}

func deleteNodeState(t *testing.T, js jetstream.JetStream, nodeID string) {
	t.Helper()
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, NodeStateKVBucket)
	if err != nil {
		t.Fatalf("Failed to get node state KV: %v", err)
	}
	err = kv.Delete(ctx, nodeID)
	if err != nil {
		t.Fatalf("Failed to delete node state: %v", err)
	}
}

func mockAppConfig(t *testing.T, js jetstream.JetStream, spec ServiceSpec) {
	t.Helper()
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, AppConfigKVBucket)
	if err != nil {
		t.Fatalf("Failed to get app config KV: %v", err)
	}
	b, _ := yaml.Marshal(spec)
	_, err = kv.Put(ctx, spec.Name, b)
	if err != nil {
		t.Fatalf("Failed to put app config: %v", err)
	}
}

// waitForSchedule polls the app-schedules KV until the target app's schedule appears, or times out.
func waitForSchedule(t *testing.T, js jetstream.JetStream, appName string, timeout time.Duration) AppSchedule {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	kv, err := js.KeyValue(ctx, AppSchedulesKVBucket)
	if err != nil {
		t.Fatalf("Failed to get schedules KV: %v", err)
	}

	for {
		entry, err := kv.Get(ctx, appName)
		if err == nil {
			var sched AppSchedule
			if err := json.Unmarshal(entry.Value(), &sched); err == nil {
				return sched
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for schedule of app: %s", appName)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func TestScheduler_LeaderElection(t *testing.T) {
	ns, nc, js := setupTestNATS(t)
	defer nc.Close()
	defer ns.Shutdown()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Create Scheduler 1
	sched1, err := NewScheduler("node-1", js, logger)
	if err != nil {
		t.Fatalf("Failed to create sched1: %v", err)
	}
	sched1.Start()
	defer sched1.Stop()

	// Wait a moment for sched1 to acquire lock
	time.Sleep(1 * time.Second)
	if !sched1.isLeader.Load() {
		t.Fatalf("Expected sched1 to be leader")
	}

	// Create Scheduler 2
	sched2, err := NewScheduler("node-2", js, logger)
	if err != nil {
		t.Fatalf("Failed to create sched2: %v", err)
	}
	sched2.Start()
	defer sched2.Stop()

	time.Sleep(1 * time.Second)
	if sched2.isLeader.Load() {
		t.Fatalf("Expected sched2 NOT to be leader")
	}

	// Stop Scheduler 1, wait for TTL to expire (Test TTL is 3s)
	sched1.Stop()
	time.Sleep(6 * time.Second)

	// Scheduler 2 should now be the leader
	if !sched2.isLeader.Load() {
		t.Fatalf("Expected sched2 to become leader after sched1 stopped")
	}
}

func TestScheduler_GlobalReplicas(t *testing.T) {
	ns, nc, js := setupTestNATS(t)
	defer nc.Close()
	defer ns.Shutdown()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Setup 3 healthy nodes
	mockNodeState(t, js, "node-A")
	mockNodeState(t, js, "node-B")
	mockNodeState(t, js, "node-C")

	// Upload binary for the app
	mockBinaryForApp(t, js, "global-app", "v1")

	// Start Scheduler
	sched, err := NewScheduler("node-A", js, logger)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	sched.Start()
	defer sched.Stop()

	// Deploy an app with 3 global replicas
	mockAppConfig(t, js, ServiceSpec{
		Name:     "global-app",
		Tag:      "v1",
		Replicas: 3,
	})

	// Wait for the scheduler to process it
	schedResult := waitForSchedule(t, js, "global-app", 5*time.Second)

	// Verify assignments (1 for each node)
	if len(schedResult.Assignments) != 3 {
		t.Fatalf("Expected 3 assignments, got %d", len(schedResult.Assignments))
	}
	if schedResult.Assignments["node-A"] != 1 || schedResult.Assignments["node-B"] != 1 || schedResult.Assignments["node-C"] != 1 {
		t.Fatalf("Expected 1 replica per node, got %v", schedResult.Assignments)
	}
}

func TestScheduler_NodeAffinity(t *testing.T) {
	ns, nc, js := setupTestNATS(t)
	defer nc.Close()
	defer ns.Shutdown()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Setup 2 healthy nodes
	mockNodeState(t, js, "node-A")
	mockNodeState(t, js, "node-B")

	// Upload binary for the app
	mockBinaryForApp(t, js, "affinity-app", "v1")

	// Start Scheduler
	sched, err := NewScheduler("node-A", js, logger)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	sched.Start()
	defer sched.Stop()

	// Deploy an app targeting strictly node-B
	mockAppConfig(t, js, ServiceSpec{
		Name: "affinity-app",
		Tag:  "v1",
		Nodes: []NodeSelectorSpec{
			{Name: "node-B", Replicas: 2},
		},
	})

	// Wait for schedule
	schedResult := waitForSchedule(t, js, "affinity-app", 5*time.Second)

	// Verify assignments
	if schedResult.Assignments["node-A"] != 0 {
		t.Fatalf("Expected 0 replicas on node-A, got %d", schedResult.Assignments["node-A"])
	}
	if schedResult.Assignments["node-B"] != 2 {
		t.Fatalf("Expected 2 replicas on node-B, got %d", schedResult.Assignments["node-B"])
	}
}

func TestScheduler_NodeFailureRebalancing(t *testing.T) {
	ns, nc, js := setupTestNATS(t)
	defer nc.Close()
	defer ns.Shutdown()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Setup 3 healthy nodes
	mockNodeState(t, js, "node-1")
	mockNodeState(t, js, "node-2")
	mockNodeState(t, js, "node-3")

	// Upload binary for the app
	mockBinaryForApp(t, js, "rebalance-app", "v1")

	// Start Scheduler
	sched, err := NewScheduler("node-1", js, logger)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	sched.Start()
	defer sched.Stop()

	// Deploy app with 3 replicas
	mockAppConfig(t, js, ServiceSpec{
		Name:     "rebalance-app",
		Tag:      "v1",
		Replicas: 3,
	})

	// Wait for initial schedule (1 per node)
	schedResult := waitForSchedule(t, js, "rebalance-app", 5*time.Second)
	if schedResult.Assignments["node-1"] != 1 || schedResult.Assignments["node-2"] != 1 || schedResult.Assignments["node-3"] != 1 {
		t.Fatalf("Expected 1 replica per node initially, got %v", schedResult.Assignments)
	}

	// SIMULATE NODE FAILURE: Delete node-3 from NodeStateKV
	deleteNodeState(t, js, "node-3")

	// Wait a moment for watchers to trigger and reconcile
	time.Sleep(1 * time.Second)

	// Fetch the updated schedule
	updatedSched := waitForSchedule(t, js, "rebalance-app", 5*time.Second)

	// Verify node-3 has NO assignments
	if val, exists := updatedSched.Assignments["node-3"]; exists && val > 0 {
		t.Fatalf("Expected node-3 to have 0 replicas, got %d", val)
	}

	// Verify the 3 replicas are now distributed between node-1 and node-2
	totalReplicas := updatedSched.Assignments["node-1"] + updatedSched.Assignments["node-2"]
	if totalReplicas != 3 {
		t.Fatalf("Expected total 3 replicas redistributed, got %d", totalReplicas)
	}

	// One should have 2, the other should have 1 (round robin logic)
	n1 := updatedSched.Assignments["node-1"]
	n2 := updatedSched.Assignments["node-2"]
	if (n1 == 2 && n2 == 1) || (n1 == 1 && n2 == 2) {
		t.Logf("Successfully rebalanced: node-1:%d, node-2:%d", n1, n2)
	} else {
		t.Fatalf("Expected 2/1 distribution, got node-1:%d, node-2:%d", n1, n2)
	}
}

func TestScheduler_EmptyNodeName(t *testing.T) {
	ns, nc, js := setupTestNATS(t)
	defer nc.Close()
	defer ns.Shutdown()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Setup 3 healthy nodes
	mockNodeState(t, js, "node-A")
	mockNodeState(t, js, "node-B")
	mockNodeState(t, js, "node-C")

	// Upload binary for the app
	mockBinaryForApp(t, js, "balanced-app", "v1")

	// Start Scheduler
	sched, err := NewScheduler("node-A", js, logger)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	sched.Start()
	defer sched.Stop()

	// Deploy an app with empty node name (balanced replicas via nodes field)
	mockAppConfig(t, js, ServiceSpec{
		Name: "balanced-app",
		Tag:  "v1",
		Nodes: []NodeSelectorSpec{
			{Name: "", Replicas: 3}, // Empty name = balance across healthy nodes
		},
	})

	// Wait for the scheduler to process it
	schedResult := waitForSchedule(t, js, "balanced-app", 5*time.Second)

	// Verify assignments (1 for each node, round-robin)
	if len(schedResult.Assignments) != 3 {
		t.Fatalf("Expected 3 assignments, got %d", len(schedResult.Assignments))
	}
	if schedResult.Assignments["node-A"] != 1 || schedResult.Assignments["node-B"] != 1 || schedResult.Assignments["node-C"] != 1 {
		t.Fatalf("Expected 1 replica per node, got %v", schedResult.Assignments)
	}
}

func TestScheduler_HybridScheduling(t *testing.T) {
	ns, nc, js := setupTestNATS(t)
	defer nc.Close()
	defer ns.Shutdown()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Setup 3 healthy nodes
	mockNodeState(t, js, "node-A")
	mockNodeState(t, js, "node-B")
	mockNodeState(t, js, "node-C")

	// Upload binary for the app
	mockBinaryForApp(t, js, "hybrid-app", "v1")

	// Start Scheduler
	sched, err := NewScheduler("node-A", js, logger)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	sched.Start()
	defer sched.Stop()

	// Deploy an app with:
	// - 2 replicas pinned to node-A
	// - 3 balanced replicas (via root replicas field)
	// - 3 balanced replicas (via empty name in nodes)
	mockAppConfig(t, js, ServiceSpec{
		Name:     "hybrid-app",
		Tag:      "v1",
		Replicas: 3, // Root level balanced replicas
		Nodes: []NodeSelectorSpec{
			{Name: "node-A", Replicas: 2}, // Pinned to node-A
			{Name: "", Replicas: 3},       // Additional balanced replicas
		},
	})

	// Wait for the scheduler to process it
	schedResult := waitForSchedule(t, js, "hybrid-app", 5*time.Second)

	// node-A should have: 2 (pinned) + 2 (from 6 balanced / 3 nodes = 2 each)
	// Total balanced = 3 (root) + 3 (empty name) = 6
	// 6 balanced across 3 nodes = 2 per node
	if schedResult.Assignments["node-A"] != 4 {
		t.Fatalf("Expected node-A to have 4 replicas (2 pinned + 2 balanced), got %d", schedResult.Assignments["node-A"])
	}
	if schedResult.Assignments["node-B"] != 2 {
		t.Fatalf("Expected node-B to have 2 replicas (balanced), got %d", schedResult.Assignments["node-B"])
	}
	if schedResult.Assignments["node-C"] != 2 {
		t.Fatalf("Expected node-C to have 2 replicas (balanced), got %d", schedResult.Assignments["node-C"])
	}

	// Total should be 8 (2 pinned + 6 balanced)
	total := schedResult.Assignments["node-A"] + schedResult.Assignments["node-B"] + schedResult.Assignments["node-C"]
	if total != 8 {
		t.Fatalf("Expected total 8 replicas, got %d", total)
	}
}

func TestScheduler_PendingReplicas(t *testing.T) {
	ns, nc, js := setupTestNATS(t)
	defer nc.Close()
	defer ns.Shutdown()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Setup 2 healthy nodes (node-3 is NOT online)
	mockNodeState(t, js, "node-1")
	mockNodeState(t, js, "node-2")

	// Upload binary for the app
	mockBinaryForApp(t, js, "pending-app", "v1")

	// Start Scheduler
	sched, err := NewScheduler("node-1", js, logger)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	sched.Start()
	defer sched.Stop()

	// Deploy an app with:
	// - 2 replicas pinned to node-3 (which is offline)
	// - 3 balanced replicas
	mockAppConfig(t, js, ServiceSpec{
		Name:     "pending-app",
		Tag:      "v1",
		Replicas: 3, // Balanced across healthy nodes
		Nodes: []NodeSelectorSpec{
			{Name: "node-3", Replicas: 2}, // Pinned to offline node
		},
	})

	// Wait for the scheduler to process it
	schedResult := waitForSchedule(t, js, "pending-app", 5*time.Second)

	// Balanced replicas should be distributed to node-1 and node-2
	if schedResult.Assignments["node-1"] < 1 || schedResult.Assignments["node-2"] < 1 {
		t.Fatalf("Expected balanced replicas on healthy nodes, got %v", schedResult.Assignments)
	}

	// Total assigned should be 3 (only balanced, not pending)
	totalAssigned := schedResult.Assignments["node-1"] + schedResult.Assignments["node-2"]
	if totalAssigned != 3 {
		t.Fatalf("Expected 3 assigned replicas, got %d", totalAssigned)
	}

	// node-3 should be in Pending
	if schedResult.Pending == nil {
		t.Fatal("Expected Pending map to exist")
	}
	if schedResult.Pending["node-3"] != 2 {
		t.Fatalf("Expected node-3 to have 2 pending replicas, got %d", schedResult.Pending["node-3"])
	}

	// Now bring node-3 online
	mockNodeState(t, js, "node-3")

	// Wait for reconciliation
	time.Sleep(1 * time.Second)

	// Fetch updated schedule
	updatedSched := waitForSchedule(t, js, "pending-app", 5*time.Second)

	// node-3 should now have its pinned replicas assigned
	// Note: balanced replicas (3) are now distributed across 3 nodes (1 each)
	// So node-3 has: 2 (pinned) + 1 (balanced) = 3
	if updatedSched.Assignments["node-3"] != 3 {
		t.Fatalf("Expected node-3 to have 3 assigned replicas after coming online (2 pinned + 1 balanced), got %d", updatedSched.Assignments["node-3"])
	}

	// Pending should be empty or nil
	if len(updatedSched.Pending) > 0 {
		t.Fatalf("Expected no pending replicas after node-3 came online, got %v", updatedSched.Pending)
	}
}

func TestScheduler_PlatformFiltering(t *testing.T) {
	ns, nc, js := setupTestNATS(t)
	defer nc.Close()
	defer ns.Shutdown()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Setup 3 healthy nodes with different platforms
	mockNodeStateWithPlatform(t, js, "node-amd64", "linux", "amd64")
	mockNodeStateWithPlatform(t, js, "node-arm64", "linux", "arm64")
	mockNodeStateWithPlatform(t, js, "node-mac", "darwin", "arm64")

	// Start Scheduler
	sched, err := NewScheduler("node-amd64", js, logger)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	sched.Start()
	defer sched.Stop()

	// Upload binaries for linux/amd64 and linux/arm64 only
	mockBinary(t, js, "platform-app", "v1", "linux", "amd64")
	mockBinary(t, js, "platform-app", "v1", "linux", "arm64")
	// Note: No darwin/arm64 binary

	// Deploy app with 6 balanced replicas
	mockAppConfig(t, js, ServiceSpec{
		Name:     "platform-app",
		Tag:      "v1",
		Replicas: 6,
	})

	// Wait for the scheduler to process it
	schedResult := waitForSchedule(t, js, "platform-app", 5*time.Second)

	// node-amd64 and node-arm64 should have replicas (3 each)
	// node-mac should have 0 (no compatible binary)
	if schedResult.Assignments["node-amd64"] != 3 {
		t.Fatalf("Expected node-amd64 to have 3 replicas, got %d", schedResult.Assignments["node-amd64"])
	}
	if schedResult.Assignments["node-arm64"] != 3 {
		t.Fatalf("Expected node-arm64 to have 3 replicas, got %d", schedResult.Assignments["node-arm64"])
	}
	if schedResult.Assignments["node-mac"] != 0 {
		t.Fatalf("Expected node-mac to have 0 replicas (no compatible binary), got %d", schedResult.Assignments["node-mac"])
	}

	// Total should be 6
	total := 0
	for _, v := range schedResult.Assignments {
		total += v
	}
	if total != 6 {
		t.Fatalf("Expected total 6 replicas, got %d", total)
	}
}

func TestScheduler_PinnedPlatformMismatch(t *testing.T) {
	ns, nc, js := setupTestNATS(t)
	defer nc.Close()
	defer ns.Shutdown()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	// Setup 2 healthy nodes
	mockNodeStateWithPlatform(t, js, "node-linux", "linux", "amd64")
	mockNodeStateWithPlatform(t, js, "node-darwin", "darwin", "arm64")

	// Start Scheduler
	sched, err := NewScheduler("node-linux", js, logger)
	if err != nil {
		t.Fatalf("Failed to create scheduler: %v", err)
	}
	sched.Start()
	defer sched.Stop()

	// Only upload linux/amd64 binary
	mockBinary(t, js, "mismatch-app", "v1", "linux", "amd64")

	// Deploy app pinned to darwin node (which has no binary)
	mockAppConfig(t, js, ServiceSpec{
		Name: "mismatch-app",
		Tag:  "v1",
		Nodes: []NodeSelectorSpec{
			{Name: "node-darwin", Replicas: 2}, // Pinned to incompatible node
		},
		Replicas: 2, // Balanced replicas
	})

	// Wait for schedule
	schedResult := waitForSchedule(t, js, "mismatch-app", 5*time.Second)

	// node-darwin should be in pending (incompatible platform)
	if schedResult.Pending == nil || schedResult.Pending["node-darwin"] != 2 {
		t.Fatalf("Expected node-darwin to have 2 pending replicas (platform mismatch), got %v", schedResult.Pending)
	}

	// Balanced replicas should go to node-linux
	if schedResult.Assignments["node-linux"] != 2 {
		t.Fatalf("Expected node-linux to have 2 balanced replicas, got %d", schedResult.Assignments["node-linux"])
	}
}

func mockNodeStateWithPlatform(t *testing.T, js jetstream.JetStream, nodeID, os, arch string) {
	t.Helper()
	ctx := context.Background()
	kv, err := js.KeyValue(ctx, NodeStateKVBucket)
	if err != nil {
		t.Fatalf("Failed to get node state KV: %v", err)
	}
	state := NodeState{NodeID: nodeID, Status: "running", OS: os, Arch: arch}
	b, _ := json.Marshal(state)
	_, err = kv.Put(ctx, nodeID, b)
	if err != nil {
		t.Fatalf("Failed to put node state: %v", err)
	}
}

func mockBinary(t *testing.T, js jetstream.JetStream, appName, tag, os, arch string) {
	t.Helper()
	ctx := context.Background()
	objStore, err := js.ObjectStore(ctx, AppBinariesOSBucket)
	if err != nil {
		t.Fatalf("Failed to get object store: %v", err)
	}

	objectName := fmt.Sprintf("%s-%s-%s-%s", appName, tag, os, arch)
	meta := jetstream.ObjectMeta{
		Name: objectName,
		Metadata: map[string]string{
			"narun-app-name":    appName,
			"narun-goos":        os,
			"narun-goarch":      arch,
			"narun-version-tag": tag,
		},
	}

	// Create a dummy binary content
	_, err = objStore.Put(ctx, meta, strings.NewReader("dummy binary content"))
	if err != nil {
		t.Fatalf("Failed to put binary: %v", err)
	}
}

// mockBinaryForApp is a convenience function to upload a default binary for an app
func mockBinaryForApp(t *testing.T, js jetstream.JetStream, appName, tag string) {
	t.Helper()
	mockBinary(t, js, appName, tag, "linux", "amd64")
}
