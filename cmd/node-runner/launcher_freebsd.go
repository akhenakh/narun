//go:build freebsd

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"syscall"

	"github.com/akhenakh/narun/internal/fbjail"
)

// Constants shared with main.go (need to be defined there or implicit string matching)
// We rely on constants defined in main.go
const (
	envJailID        = "NARUN_INTERNAL_JAIL_ID"
	envJailTargetCmd = "NARUN_INTERNAL_JAIL_TARGET_CMD"
)

func runLauncher() {
	// Get Config
	jidStr := os.Getenv(envJailID)
	targetCmd := os.Getenv(envJailTargetCmd)
	targetArgsJSON := os.Getenv(envLandlockTargetArgsJSON) // Reuse existing const

	targetUIDStr := os.Getenv(envTargetUID)
	targetGIDStr := os.Getenv(envTargetGID)

	if jidStr == "" || targetCmd == "" {
		fmt.Fprintf(os.Stderr, "[narun-jail] Error: Missing required env vars (JID/CMD)\n")
		os.Exit(1)
	}

	jid, err := strconv.Atoi(jidStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[narun-jail] Error: Invalid JID: %v\n", err)
		os.Exit(1)
	}

	var targetArgs []string
	if targetArgsJSON != "" {
		_ = json.Unmarshal([]byte(targetArgsJSON), &targetArgs)
	}

	// Attach to Jail
	fmt.Fprintf(os.Stderr, "[narun-jail] Attaching to JID %d\n", jid)
	if err := fbjail.JailAttach(jid); err != nil {
		fmt.Fprintf(os.Stderr, "[narun-jail] Error: Failed to attach to jail: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "[narun-jail] Attached successfully.\n")

	// Drop Privileges (inside Jail)
	// Even if Jail restricts root, we might want to switch user for file permissions
	if targetUIDStr != "" && targetGIDStr != "" {
		uid, _ := strconv.Atoi(targetUIDStr)
		gid, _ := strconv.Atoi(targetGIDStr)

		// Set GID
		if err := syscall.Setgid(gid); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-jail] Error: Setgid failed: %v\n", err)
			os.Exit(1)
		}
		// Set UID
		if err := syscall.Setuid(uid); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-jail] Error: Setuid failed: %v\n", err)
			os.Exit(1)
		}
	}

	// Exec Target
	argv := []string{targetCmd}
	argv = append(argv, targetArgs...)
	envv := os.Environ() // Inherit environment (contains user specific vars set in process.go)

	fmt.Fprintf(os.Stderr, "[narun-jail] Executing: %s args: %v\n", targetCmd, argv)
	if err := syscall.Exec(targetCmd, argv, envv); err != nil {
		fmt.Fprintf(os.Stderr, "[narun-jail] Error: Exec failed: %v\n", err)
		os.Exit(123)
	}
}
