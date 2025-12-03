//go:build freebsd

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akhenakh/narun/internal/noderunner"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

// Workload source code that checks if it is inside a jail
const freebsdWorkloadSource = `
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
)

func main() {
	// Start Metrics/Status Server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Check Jail Status via sysctl
		cmd := exec.Command("sysctl", "-n", "security.jail.jailed")
		out, err := cmd.CombinedOutput()

		isJailed := "0"
		if err == nil {
			isJailed = strings.TrimSpace(string(out))
		}

		// Check Hostname
		cmdHost := exec.Command("hostname")
		outHost, _ := cmdHost.CombinedOutput()

		resp := map[string]string{
			"status": "ok",
			"jailed": isJailed, // Should be 1
			"hostname": strings.TrimSpace(string(outHost)),
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Listen on all interfaces (inside jail)
	http.ListenAndServe(":8080", nil)
}
`

func TestFreeBSD_Jail_EndToEnd(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("Skipping FreeBSD Jail test: Requires root privileges to create jails/rctl")
	}

	// 1. Setup NATS
	storeDir := t.TempDir()
	opts := &server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  storeDir,
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Failed to create NATS server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatalf("NATS server not ready")
	}
	defer ns.Shutdown()
	natsURL := ns.ClientURL()

	// 2. Setup NATS Resources
	nc, _ := nats.Connect(natsURL)
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx := context.Background()

	// Create Buckets
	js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: noderunner.AppConfigKVBucket})
	js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: noderunner.NodeStateKVBucket})
	js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: noderunner.SecretKVBucket})
	binStore, _ := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: noderunner.AppBinariesOSBucket})
	js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: noderunner.FileOSBucket})

	// 3. Build Artifacts
	tempDir := t.TempDir()

	// Build node-runner
	rootDir, _ := filepath.Abs("../../")
	runnerBinPath := filepath.Join(tempDir, "node-runner")
	cmdBuildRunner := exec.Command("go", "build", "-o", runnerBinPath, filepath.Join(rootDir, "cmd/node-runner"))
	if out, err := cmdBuildRunner.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build node-runner: %v\nOutput: %s", err, out)
	}

	// Build Workload
	workloadSrcPath := filepath.Join(tempDir, "main.go")
	os.WriteFile(workloadSrcPath, []byte(freebsdWorkloadSource), 0644)
	workloadBinPath := filepath.Join(tempDir, "workload")
	cmdBuildWorkload := exec.Command("go", "build", "-o", workloadBinPath, workloadSrcPath)
	if out, err := cmdBuildWorkload.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build workload: %v\nOutput: %s", err, out)
	}

	// Upload Workload
	binName := "test-jail-v1-freebsd-amd64"
	workloadData, _ := os.ReadFile(workloadBinPath)
	meta := jetstream.ObjectMeta{
		Name: binName,
		Metadata: map[string]string{
			"narun-app-name":    "test-jail",
			"narun-version-tag": "v1",
			"narun-goos":        "freebsd",
			"narun-goarch":      "amd64",
		},
	}
	binStore.Put(ctx, meta, bytes.NewReader(workloadData))

	// 4. Start Node Runner
	runnerDataDir := filepath.Join(tempDir, "runner-data")
	// Pick a random port for Metrics
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	metricsPort := l.Addr().(*net.TCPAddr).Port
	l.Close()

	runnerCmd := exec.Command(runnerBinPath,
		"-nats-url", natsURL,
		"-node-id", "freebsd-node",
		"-data-dir", runnerDataDir,
		"-log-level", "debug",
		"-metrics-addr", fmt.Sprintf("127.0.0.1:%d", metricsPort),
		"-admin-addr", ":0",
	)
	runnerCmd.Stdout = os.Stdout
	runnerCmd.Stderr = os.Stderr

	if err := runnerCmd.Start(); err != nil {
		t.Fatalf("Failed to start node-runner: %v", err)
	}
	defer func() {
		runnerCmd.Process.Signal(os.Interrupt)
		runnerCmd.Wait()
	}()
	time.Sleep(2 * time.Second)

	// 5. Deploy Config with Jail Spec
	// We use 127.0.0.50 as the jail alias
	spec := noderunner.ServiceSpec{
		Name: "test-jail",
		Tag:  "v1",
		Mode: "jail",
		Nodes: []noderunner.NodeSelectorSpec{
			{Name: "freebsd-node", Replicas: 1},
		},
		Jail: noderunner.JailSpec{
			Hostname:     "jailed-app",
			IP4Addresses: []string{"127.0.0.50"},
			DevfsRuleset: 4,
		},
		// Set a memory limit to verify RCTL rules
		MemoryMB: 64,
	}

	specBytes, _ := yaml.Marshal(spec)
	kvApps, _ := js.KeyValue(ctx, noderunner.AppConfigKVBucket)
	kvApps.Put(ctx, "test-jail", specBytes)

	t.Log("Deployed Jail Config. Waiting for status...")

	// 6. Monitor Status
	statusSubject := "status.test-jail.freebsd-node"
	subStatus, err := nc.SubscribeSync(statusSubject)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	timeout := time.After(30 * time.Second)
	isRunning := false

	// Ensure the alias is cleaned up if test fails/exits
	defer exec.Command("ifconfig", "lo0", "inet", "127.0.0.50", "-alias").Run()

	// Manually add alias for the jail to bind to (usually node-runner doesn't create interface aliases, just uses them)
	// In a real setup, lo0 aliases are pre-configured or handled by networking scripts.
	// For E2E test, we add it now.
	if out, err := exec.Command("ifconfig", "lo0", "alias", "127.0.0.50/32").CombinedOutput(); err != nil {
		t.Fatalf("Failed to add test alias IP: %s", out)
	}

loop:
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for jail start")
		default:
			msg, err := subStatus.NextMsg(100 * time.Millisecond)
			if err == nil {
				var update struct {
					Status string `json:"status"`
					Error  string `json:"error"`
				}
				json.Unmarshal(msg.Data, &update)
				if update.Status == "running" {
					isRunning = true
					break loop
				}
				if update.Status == "failed" {
					t.Fatalf("App failed: %s", update.Error)
				}
			}
		}
	}

	if !isRunning {
		t.Fatal("App did not reach running state")
	}

	// 7. Verify Jail Isolation
	t.Log("Verifying Jail Isolation via HTTP...")

	// Connect to the Jail IP
	resp, err := http.Get("http://127.0.0.50:8080/")
	if err != nil {
		t.Fatalf("Failed to connect to jailed app: %v", err)
	}
	defer resp.Body.Close()

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	t.Logf("App Response: %+v", result)

	if result["jailed"] != "1" {
		t.Errorf("Expected security.jail.jailed=1, got %s", result["jailed"])
	}
	if result["hostname"] != "jailed-app" {
		t.Errorf("Expected hostname 'jailed-app', got '%s'", result["hostname"])
	}

	// 8. Verify RCTL Rules
	// We expect a rule for the jail limiting memory
	t.Log("Verifying RCTL rules...")
	// Need to find the JID. The Instance ID is test-jail-0
	// Jail name in platform_freebsd.go is fmt.Sprintf("narun_%s", strings.ReplaceAll(instanceID, "-", "_"))
	jailName := "narun_test_jail_0"

	cmdRctl := exec.Command("rctl", "-h", fmt.Sprintf("jail:%s", jailName))
	outRctl, err := cmdRctl.CombinedOutput()
	if err != nil {
		t.Errorf("Failed to query rctl: %v", err)
	}
	rules := string(outRctl)
	t.Logf("RCTL Rules for %s:\n%s", jailName, rules)

	// Check for memoryuse limit (64MB = 67108864 bytes)
	if !strings.Contains(rules, "memoryuse:deny=67108864") {
		t.Errorf("Expected memory limit rule not found in rctl output")
	}

	t.Log("FreeBSD Jail E2E Test Passed")
}
