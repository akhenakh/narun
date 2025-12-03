//go:build !linux

package main

import (
	"fmt"
	"os"
)

// runLandlockLauncher is the fallback for non-Linux OS.
func runLandlockLauncher() {
	fmt.Fprintf(os.Stderr, "[narun-launcher] Error: Landlock/Namespace launching is only supported on Linux.\n")
	os.Exit(landlockUnsupportedErrCode)
}
