package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/akhenakh/narun/internal/crypto"
	"github.com/akhenakh/narun/internal/noderunner"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gopkg.in/yaml.v3"
)

// Define the workload source code here to keep the test self-contained.
const workloadSource = `
package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	results := make(map[string]string)
	results["status"] = "failed"

	// Read Secret Env
	secret := os.Getenv("SECRET")
	if secret == "super-secret-value" {
		results["secret"] = "ok"
	} else {
		results["secret"] = fmt.Sprintf("failed: got '%s'", secret)
	}

	// Read File from Object Store (mounted at ./mydata/stored_file)
	// Note: Path is relative to work dir
	content, err := os.ReadFile("mydata/stored_file")
	if err == nil && string(content) == "hello-object-store" {
		results["file_store"] = "ok"
	} else {
		results["file_store"] = fmt.Sprintf("failed: err=%v content='%s'", err, string(content))
	}

	// Read Host File via Landlock (/etc/hostname is usually safe and present)
	// The test config must allow this.
	hostContent, err := os.ReadFile("/etc/hostname")
	if err == nil && len(hostContent) > 0 {
		results["host_read"] = "ok"
	} else {
		results["host_read"] = fmt.Sprintf("failed: %v", err)
	}

	// Test LocalPorts (Connect to Host port forwarded to guest)
	// We expect port 9999 to be mapped.
	var conn net.Conn
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err = net.DialTimeout("tcp", "127.0.0.1:9999", 100*time.Millisecond)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if err == nil {
		results["network"] = "ok"
		conn.Close()
	} else {
		results["network"] = fmt.Sprintf("failed: %v", err)
	}

	// Final Status
	if results["secret"] == "ok" && results["file_store"] == "ok" &&
	   results["host_read"] == "ok" && results["network"] == "ok" {
		results["status"] = "success"
	}

	// Print JSON to stdout, which node-runner sends to NATS logs
	jsonBytes, _ := json.Marshal(results)
	fmt.Println(string(jsonBytes))

	// Keep running briefly to ensure logs flush
	time.Sleep(2 * time.Second)
}
`

func TestEndToEnd(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Skipping E2E test: Node Runner Landlock/Namespaces only supported on Linux")
	}
	if os.Geteuid() != 0 {
		t.Log("WARNING: This test usually requires root privileges for 'unshare' and cgroups.")
		t.Log("Try running with: sudo go test -v ./tests/e2e/...")
	}

	// Start Embedded NATS Server WITH JetStream
	storeDir := t.TempDir()
	opts := &server.Options{
		Port:      -1, // Random port
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
	t.Logf("NATS Server started at %s", natsURL)

	// wait a bit
	time.Sleep(1 * time.Second)

	// Setup NATS Client & Resources
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to init JetStream: %v", err)
	}

	// Create Buckets
	ctx := context.Background()
	_, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: noderunner.AppConfigKVBucket})
	if err != nil {
		t.Fatalf("Failed to create AppConfigKV: %v", err)
	}
	_, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: noderunner.NodeStateKVBucket})
	if err != nil {
		t.Fatalf("Failed to create NodeStateKV: %v", err)
	}
	kvSecrets, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: noderunner.SecretKVBucket})
	if err != nil {
		t.Fatalf("Failed to create SecretKV: %v", err)
	}

	fileStore, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: noderunner.FileOSBucket})
	if err != nil {
		t.Fatalf("Failed to create FileOS: %v", err)
	}
	binStore, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: noderunner.AppBinariesOSBucket})
	if err != nil {
		t.Fatalf("Failed to create AppBinariesOS: %v", err)
	}

	// Provision Data
	// Master Key
	masterKey := make([]byte, 32)
	rand.Read(masterKey)
	masterKeyB64 := base64.StdEncoding.EncodeToString(masterKey)

	// Secret
	secretVal := "super-secret-value"
	encSecret, nonce, err := crypto.Encrypt(masterKey, []byte(secretVal), []byte("TEST_SECRET"))
	if err != nil {
		t.Fatalf("Failed to encrypt secret: %v", err)
	}
	storedSecret := noderunner.StoredSecret{
		Name:       "TEST_SECRET",
		Ciphertext: encSecret,
		Nonce:      nonce,
		CreatedAt:  time.Now(),
	}
	secretBytes, _ := json.Marshal(storedSecret)
	if _, err := kvSecrets.Put(ctx, "TEST_SECRET", secretBytes); err != nil {
		t.Fatalf("Failed to put secret: %v", err)
	}

	// File in Object Store
	fileContent := []byte("hello-object-store")
	if _, err := fileStore.PutBytes(ctx, "myfile", fileContent); err != nil {
		t.Fatalf("Failed to put file: %v", err)
	}

	// Build Binaries
	tempDir := t.TempDir()

	// Build node-runner
	rootDir, _ := filepath.Abs("../../")
	runnerBinPath := filepath.Join(tempDir, "node-runner")
	cmdBuildRunner := exec.Command("go", "build", "-o", runnerBinPath, filepath.Join(rootDir, "cmd/node-runner"))
	cmdBuildRunner.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmdBuildRunner.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build node-runner: %v\nOutput: %s", err, out)
	}

	// Build Workload
	workloadSrcPath := filepath.Join(tempDir, "main.go")
	if err := os.WriteFile(workloadSrcPath, []byte(workloadSource), 0644); err != nil {
		t.Fatalf("Failed to write workload source: %v", err)
	}
	workloadBinPath := filepath.Join(tempDir, "workload")
	cmdBuildWorkload := exec.Command("go", "build", "-o", workloadBinPath, workloadSrcPath)
	cmdBuildWorkload.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmdBuildWorkload.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build workload: %v\nOutput: %s", err, out)
	}

	// Upload Workload to NATS
	// Name: app-tag-os-arch
	binName := fmt.Sprintf("test-app-v1-%s-%s", runtime.GOOS, runtime.GOARCH)
	workloadData, err := os.ReadFile(workloadBinPath)
	if err != nil {
		t.Fatalf("Failed to read built workload: %v", err)
	}
	meta := jetstream.ObjectMeta{
		Name: binName,
		Metadata: map[string]string{
			"narun-app-name":    "test-app",
			"narun-version-tag": "v1",
			"narun-goos":        runtime.GOOS,
			"narun-goarch":      runtime.GOARCH,
		},
	}
	// Use Put instead of PutBytes to handle metadata
	_, err = binStore.Put(ctx, meta, bytes.NewReader(workloadData))
	if err != nil {
		t.Fatalf("Failed to upload binary: %v", err)
	}

	// Start Host Listener for localPorts test
	listener, err := net.Listen("tcp", "127.0.0.1:9999")
	if err != nil {
		t.Fatalf("Failed to start host listener: %v", err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	// Start Node Runner
	runnerDataDir := filepath.Join(tempDir, "runner-data")
	runnerCmd := exec.Command(runnerBinPath,
		"-nats-url", natsURL,
		"-node-id", "test-node",
		"-data-dir", runnerDataDir,
		"-master-key", masterKeyB64,
		"-log-level", "debug",
		"-metrics-addr", ":0",
		"-admin-addr", ":0",
	)
	runnerCmd.Env = append(os.Environ(), "PATH="+os.Getenv("PATH"))
	runnerCmd.Stdout = os.Stdout
	runnerCmd.Stderr = os.Stderr

	if err := runnerCmd.Start(); err != nil {
		t.Fatalf("Failed to start node-runner: %v", err)
	}
	defer func() {
		runnerCmd.Process.Signal(os.Interrupt)
		runnerCmd.Wait()
	}()

	t.Log("Node Runner started...")
	time.Sleep(2 * time.Second) // Give it a moment to connect

	// Deploy App Config
	spec := noderunner.ServiceSpec{
		Name: "test-app",
		Tag:  "v1",
		Mode: "landlock",
		Nodes: []noderunner.NodeSelectorSpec{
			{Name: "test-node", Replicas: 1},
		},
		Env: []noderunner.EnvVar{
			{Name: "SECRET", ValueFromSecret: "TEST_SECRET"},
		},
		Mounts: []noderunner.MountSpec{
			{
				Path:   "mydata/stored_file",
				Source: noderunner.SourceSpec{ObjectStore: "myfile"},
			},
		},
		Landlock: noderunner.LandlockSpec{
			Shared: false, // Static binary does not need shared libs
			Tmp:    true,
			DNS:    true,

			Paths: []noderunner.LandlockPathSpec{
				{Path: "/etc/hostname", Modes: "r"},
			},
		},
		NoNet: noderunner.NoNetSpec{
			LocalPorts: []noderunner.PortForward{
				{Port: 9999, Protocol: "tcp"},
			},
		},
		User: os.Getenv("USER"),
	}

	specBytes, _ := yaml.Marshal(spec)
	kvApps, err := js.KeyValue(ctx, noderunner.AppConfigKVBucket)
	if err != nil {
		t.Fatalf("Failed to get app KV: %v", err)
	}
	_, err = kvApps.Put(ctx, "test-app", specBytes)
	if err != nil {
		t.Fatalf("Failed to put app config: %v", err)
	}
	t.Log("App config deployed.")

	// Verify Execution
	logSubject := "logs.test-app.test-node"
	statusSubject := "status.test-app.test-node"

	subLogs, err := nc.SubscribeSync(logSubject)
	if err != nil {
		t.Fatalf("Failed to subscribe to logs: %v", err)
	}
	subStatus, err := nc.SubscribeSync(statusSubject)
	if err != nil {
		t.Fatalf("Failed to subscribe to status: %v", err)
	}

	timeout := time.After(30 * time.Second)
	success := false

	type ResultJSON struct {
		Status string `json:"status"`
		Secret string `json:"secret"`
		File   string `json:"file_store"`
		Host   string `json:"host_read"`
		Net    string `json:"network"`
	}

	t.Log("Waiting for app logs or status failure...")
loop:
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for successful app execution")
		default:
			// Check Status
			msgStatus, err := subStatus.NextMsg(100 * time.Millisecond)
			if err == nil {
				var statusUpdate struct {
					Status   string `json:"status"`
					ExitCode *int   `json:"exit_code"`
					Error    string `json:"error"`
				}
				if err := json.Unmarshal(msgStatus.Data, &statusUpdate); err == nil {
					if statusUpdate.Status == "failed" || statusUpdate.Status == "crashed" {
						// Log detailed error
						t.Logf("App status update: %s (Error: %s, Exit: %v)", statusUpdate.Status, statusUpdate.Error, statusUpdate.ExitCode)
						// Fail immediately if it's the specific Landlock error
						if statusUpdate.ExitCode != nil && *statusUpdate.ExitCode == 122 {
							t.Fatalf("App failed with Landlock error (exit 122). Check Landlock paths/permissions.")
						}
					}
				}
			}

			// Check Logs
			msg, err := subLogs.NextMsg(100 * time.Millisecond)
			if err != nil {
				continue
			}

			var logWrapper struct {
				Message string `json:"message"`
			}
			if err := json.Unmarshal(msg.Data, &logWrapper); err != nil {
				continue
			}

			var res ResultJSON
			if err := json.Unmarshal([]byte(logWrapper.Message), &res); err == nil {
				t.Logf("Received App Result: %+v", res)
				if res.Status == "success" {
					success = true
					break loop
				} else if res.Status == "failed" {
					t.Fatalf("App reported failure: %+v", res)
				}
			} else {
				t.Logf("[APP LOG] %s", logWrapper.Message)
			}
		}
	}

	if !success {
		t.Fatal("Did not receive success status from app")
	}
	t.Log("E2E Test Passed Successfully!")
}
