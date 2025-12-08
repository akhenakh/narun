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

func runLauncher() {
	// Get Config
	jidStr := os.Getenv(envJailID)
	targetCmd := os.Getenv(envJailTargetCmd)
	targetArgsJSON := os.Getenv(envLandlockTargetArgsJSON) // Reuse existing const
	instanceRoot := os.Getenv(envLandlockInstanceRoot)     // Reuse existing const

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

	// Change Directory to Jail Path (BEFORE Attaching)
	// We must acquire a handle (CWD) to the jail root while we are still in the host context.
	// Once we attach to the jail, absolute host paths might become inaccessible.
	if instanceRoot != "" {
		// Verify path exists
		if _, err := os.Stat(instanceRoot); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-jail] Error: Instance root %s inaccessible: %v\n", instanceRoot, err)
			os.Exit(1)
		}

		fmt.Fprintf(os.Stderr, "[narun-jail] Chdir to %s before attach\n", instanceRoot)
		if err := os.Chdir(instanceRoot); err != nil {
			fmt.Fprintf(os.Stderr, "[narun-jail] Error: Failed to chdir to instance root: %v\n", err)
			os.Exit(1)
		}
	} else {
		// This is fatal; running without chroot breaks the jail isolation model
		fmt.Fprintf(os.Stderr, "[narun-jail] Error: Missing instance root env var\n")
		os.Exit(1)
	}

	// Attach to Jail
	// jail_attach puts the process inside the jail ID context (networking, hostname, etc.)
	fmt.Fprintf(os.Stderr, "[narun-jail] Attaching to JID %d\n", jid)
	if err := fbjail.JailAttach(jid); err != nil {
		fmt.Fprintf(os.Stderr, "[narun-jail] Error: Failed to attach to jail: %v\n", err)
		os.Exit(1)
	}

	// Chroot to Current Directory (.)
	// Since we are already in the correct directory, we chroot to "."
	fmt.Fprintf(os.Stderr, "[narun-jail] Chrooting to .\n")
	if err := syscall.Chroot("."); err != nil {
		fmt.Fprintf(os.Stderr, "[narun-jail] Error: Failed to chroot to .: %v\n", err)
		os.Exit(1)
	}
	// Chroot does not change CWD, so we must explicitly chdir to root / to update our view
	if err := syscall.Chdir("/"); err != nil {
		fmt.Fprintf(os.Stderr, "[narun-jail] Error: Failed to chdir /: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "[narun-jail] Attached and chrooted successfully.\n")

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
	// targetCmd should be an absolute path *inside* the jail (e.g., "/app_binary")
	argv := []string{targetCmd}
	argv = append(argv, targetArgs...)
	envv := os.Environ() // Inherit environment

	fmt.Fprintf(os.Stderr, "[narun-jail] Executing: %s args: %v\n", targetCmd, argv)
	if err := syscall.Exec(targetCmd, argv, envv); err != nil {
		fmt.Fprintf(os.Stderr, "[narun-jail] Error: Exec failed: %v\n", err)
		os.Exit(123)
	}
}
