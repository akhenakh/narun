//go:build freebsd

package fbjail

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestJailConfig_ToParams(t *testing.T) {
	tests := []struct {
		name     string
		config   JailConfig
		expected map[string]interface{}
		hasKeys  []string
	}{
		{
			name: "Basic Config",
			config: JailConfig{
				Name:     "test_jail",
				Path:     "/tmp/jail",
				Hostname: "jail.host",
				Persist:  true,
			},
			expected: map[string]interface{}{
				"name":          "test_jail",
				"path":          "/tmp/jail",
				"host.hostname": "jail.host",
			},
			hasKeys: []string{"persist"},
		},
		{
			name: "IP Config",
			config: JailConfig{
				Name:         "test_jail_ip",
				IP4Addresses: []string{"127.0.0.1"},
			},
			expected: map[string]interface{}{
				"ip4.addr": []byte{127, 0, 0, 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := tt.config.ToParams()
			for k, v := range tt.expected {
				val, ok := params[k]
				if !ok {
					t.Errorf("Missing key: %s", k)
					continue
				}

				// Handle []byte comparison for IPs
				if expectedBytes, ok := v.([]byte); ok {
					gotBytes, ok := val.([]byte)
					if !ok {
						t.Errorf("Key %s: expected []byte, got %T", k, val)
						continue
					}
					if !bytes.Equal(expectedBytes, gotBytes) {
						t.Errorf("Key %s: expected %v, got %v", k, expectedBytes, gotBytes)
					}
					continue
				}

				// Handle String comparison
				if strVal, ok := val.(string); ok {
					if expectedStr, ok := v.(string); ok {
						if strVal != expectedStr {
							t.Errorf("Key %s: expected %s, got %s", k, expectedStr, strVal)
						}
					}
				}
			}
		})
	}
}

func TestManager_Lifecycle(t *testing.T) {
	if runtime.GOOS != "freebsd" || os.Geteuid() != 0 {
		t.Skip("Skipping integration test: Requires FreeBSD and Root")
	}

	// 1. Generate unique jail name to avoid collisions
	rand.Seed(time.Now().UnixNano())
	jailName := fmt.Sprintf("fbjail_test_%d", rand.Intn(100000))
	jailRoot := t.TempDir()

	// Register cleanup BEFORE Create() to ensure it runs even if Create fails
	defer func() {
		// Use system command to ensure removal by name if JID logic failed
		_ = exec.Command("jail", "-r", jailName).Run()
	}()

	cfg := JailConfig{
		Name:          jailName,
		Path:          jailRoot,
		Hostname:      "test-unit",
		Persist:       true,
		EnforceStatfs: 1, // Allow seeing own mounts
	}

	mgr := NewManager(cfg)

	// 2. Create
	if err := mgr.Create(); err != nil {
		t.Fatalf("Failed to create jail: %v", err)
	}

	// 3. Verify
	if mgr.JID <= 0 {
		t.Errorf("Invalid JID: %d", mgr.JID)
	}

	cmd := exec.Command("jls", "-j", jailName, "jid")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("jls verification failed: %v. Output: %s", err, string(out))
	}
	t.Logf("Jail verified. JID: %s", strings.TrimSpace(string(out)))
}

// TestIOVecBuilding verifies the unsafe pointer logic (internal).
func TestIOVecBuilding(t *testing.T) {
	params := map[string]interface{}{
		"test_key": "val",
		"flag":     nil,
		"int":      int32(1),
	}
	iovecs, keep, err := buildIovec(params)
	if err != nil {
		t.Fatalf("buildIovec failed: %v", err)
	}
	defer runtime.KeepAlive(keep)

	// 3 params * 2 (key+val) = 6 iovecs
	if len(iovecs) != 6 {
		t.Errorf("Expected 6 iovecs, got %d", len(iovecs))
	}
}
