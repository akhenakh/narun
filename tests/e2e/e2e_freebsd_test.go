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
	"os"
	"syscall"
)

func main() {
	// Start Metrics/Status Server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Check Jail Status via syscall
		jailed, err := syscall.SysctlUint32("security.jail.jailed")
		isJailed := "0"
		if err == nil {
			isJailed = fmt.Sprintf("%d", jailed)
		}

		// Check Hostname
		hostname, _ := os.Hostname()

		resp := map[string]string{
			"status": "ok",
			"jailed": isJailed, // Should be 1
			"hostname": hostname,
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

	// 0. Pre-test Cleanup
	jailName := "narun_test_jail_0"
	exec.Command("jail", "-r", jailName).Run()
	exec.Command("ifconfig", "lo0", "inet", "127.0.0.50", "-alias").Run()

	// Check if RACCT/RCTL is enabled
	racctEnabled := false
	cmdSysctl := exec.Command("sysctl", "-n", "kern.racct.enable")
	outSysctl, err := cmdSysctl.Output()
	if err == nil && strings.TrimSpace(string(outSysctl)) == "1" {
		racctEnabled = true
	} else {
		t.Log("RACCT not enabled. Skipping resource limit tests.")
	}

	// Setup NATS
	storeDir := t.TempDir()
	opts := &server.Options{
		Host:      "127.0.0.1",
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

	// Setup NATS Resources
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}
	ctx := context.Background()

	js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: noderunner.AppConfigKVBucket})
	js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: noderunner.NodeStateKVBucket})
	js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: noderunner.SecretKVBucket})
	binStore, _ := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: noderunner.AppBinariesOSBucket})
	js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: noderunner.FileOSBucket})

	// Build Artifacts
	tempDir := t.TempDir()

	// Build node-runner (Static) with -buildvcs=false
	rootDir, _ := filepath.Abs("../../")
	runnerBinPath := filepath.Join(tempDir, "node-runner")
	cmdBuildRunner := exec.Command("go", "build", "-buildvcs=false", "-tags", "netgo", "-ldflags", "-extldflags -static", "-o", runnerBinPath, filepath.Join(rootDir, "cmd/node-runner"))
	cmdBuildRunner.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmdBuildRunner.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build node-runner: %v\nOutput: %s", err, out)
	}

	// Build Workload (Static) with -buildvcs=false
	workloadSrcPath := filepath.Join(tempDir, "main.go")
	os.WriteFile(workloadSrcPath, []byte(freebsdWorkloadSource), 0644)
	workloadBinPath := filepath.Join(tempDir, "workload")
	cmdBuildWorkload := exec.Command("go", "build", "-buildvcs=false", "-tags", "netgo", "-ldflags", "-extldflags -static", "-o", workloadBinPath, workloadSrcPath)
	cmdBuildWorkload.Env = append(os.Environ(), "CGO_ENABLED=0")
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

	// Subscribe to Logs for Debugging
	logSub, _ := nc.SubscribeSync("logs.>")

	// Start Node Runner
	runnerDataDir := filepath.Join(tempDir, "runner-data")
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
		// Dump any collected logs if failed
		if t.Failed() {
			for {
				msg, err := logSub.NextMsg(10 * time.Millisecond)
				if err != nil {
					break
				}
				t.Logf("[NATS LOG] %s: %s", msg.Subject, string(msg.Data))
			}
		}
		runnerCmd.Process.Signal(os.Interrupt)
		runnerCmd.Wait()
		// Manual cleanup of jail resources to prevent "busy" errors in TempDir cleanup
		exec.Command("jail", "-r", jailName).Run()
		// Ensure devfs is unmounted with force
		devfsPath := filepath.Join(runnerDataDir, "instances/test-jail-0/work/dev")
		if err := exec.Command("umount", "-f", devfsPath).Run(); err != nil {
			// If force unmount failed, try one more time after a short delay
			time.Sleep(100 * time.Millisecond)
			exec.Command("umount", "-f", devfsPath).Run()
		}
		exec.Command("ifconfig", "lo0", "inet", "127.0.0.50", "-alias").Run()
	}()
	time.Sleep(2 * time.Second)

	// Ensure Alias exists BEFORE deploy to prevent network race condition
	if err := exec.Command("ifconfig", "lo0", "inet", "127.0.0.50/32", "alias").Run(); err != nil {
		t.Fatalf("Failed to set IP alias: %v", err)
	}

	// Deploy Config
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
	}
	if racctEnabled {
		spec.MemoryMB = 64
	}

	specBytes, _ := yaml.Marshal(spec)
	kvApps, _ := js.KeyValue(ctx, noderunner.AppConfigKVBucket)
	kvApps.Put(ctx, "test-jail", specBytes)

	t.Log("Deployed Jail Config. Waiting for status...")

	// Monitor Status
	statusSubject := "status.test-jail.freebsd-node"
	subStatus, err := nc.SubscribeSync(statusSubject)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	timeout := time.After(30 * time.Second)
	isRunning := false

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

	// Verify Jail
	t.Log("Verifying Jail Isolation via HTTP...")
	var resp *http.Response
	var connectErr error
	deadline := time.Now().Add(10 * time.Second)

	for time.Now().Before(deadline) {
		resp, connectErr = http.Get("http://127.0.0.50:8080/")
		if connectErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if connectErr != nil {
		t.Fatalf("Failed to connect to jailed app: %v", connectErr)
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

	if racctEnabled {
		t.Log("Verifying RCTL rules...")
		cmdRctl := exec.Command("rctl", "-h", fmt.Sprintf("jail:%s", jailName))
		outRctl, err := cmdRctl.CombinedOutput()
		if err != nil {
			t.Errorf("Failed to query rctl: %v", err)
		}
		rules := string(outRctl)
		t.Logf("RCTL Rules for %s:\n%s", jailName, rules)
		if !strings.Contains(rules, "memoryuse:deny=67108864") {
			t.Errorf("Expected memory limit rule not found")
		}
	}

	t.Log("FreeBSD Jail E2E Test Passed")
}
