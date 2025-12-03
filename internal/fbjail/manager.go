//go:build freebsd

package fbjail

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Manager handles the lifecycle of a jail.
type Manager struct {
	JID    int
	Config JailConfig
}

// NewManager creates a manager for a specific jail config.
func NewManager(cfg JailConfig) *Manager {
	// Set defaults suitable for Narun
	if cfg.DevfsRuleset == 0 {
		cfg.DevfsRuleset = 4 // devfsrules_jail
	}
	if cfg.EnforceStatfs == 0 {
		cfg.EnforceStatfs = 2 // Most restrictive
	}
	return &Manager{Config: cfg, JID: -1}
}

// Create creates the jail in the kernel and performs setup (DNS, Certs).
func (m *Manager) Create() error {
	// 1. Prepare Filesystem requirements
	if err := m.setupFilesystem(); err != nil {
		return fmt.Errorf("fs setup failed: %w", err)
	}

	// 2. Call jail_set
	params := m.Config.ToParams()
	jid, err := JailSet(params, JAIL_CREATE|JAIL_UPDATE)
	if err != nil {
		return err
	}
	m.JID = jid

	return nil
}

// setupFilesystem handles file copying and mounting before jail creation.
func (m *Manager) setupFilesystem() error {
	// Ensure destination etc exists
	destEtc := filepath.Join(m.Config.Path, "etc")
	if err := os.MkdirAll(destEtc, 0755); err != nil {
		return err
	}

	// DNS: Copy /etc/resolv.conf
	if m.Config.CopyResolvConf {
		if err := copyFile("/etc/resolv.conf", filepath.Join(destEtc, "resolv.conf")); err != nil {
			return fmt.Errorf("failed to copy resolv.conf: %w", err)
		}
	}

	// Certs: Mount system certificates (read-only)
	// FreeBSD typically stores certs in /usr/share/certs or /etc/ssl
	// For simplicity, we assume standard paths.
	if m.Config.MountSystemCerts {
		// We need to ensure the destination directory exists inside the jail
		// then mount_nullfs -o ro source target
		sysCertPath := "/etc/ssl"
		jailCertPath := filepath.Join(m.Config.Path, "etc", "ssl")
		if err := os.MkdirAll(jailCertPath, 0755); err != nil {
			return err
		}
		if err := mountNullfsRO(sysCertPath, jailCertPath); err != nil {
			return fmt.Errorf("failed to mount certs: %w", err)
		}

		// Also standard FreeBSD certs path
		sysShareCert := "/usr/share/certs"
		jailShareCert := filepath.Join(m.Config.Path, "usr", "share", "certs")
		if _, err := os.Stat(sysShareCert); err == nil {
			if err := os.MkdirAll(jailShareCert, 0755); err != nil {
				return err
			}
			if err := mountNullfsRO(sysShareCert, jailShareCert); err != nil {
				return fmt.Errorf("failed to mount share certs: %w", err)
			}
		}
	}

	return nil
}

// Stop removes the jail.
func (m *Manager) Stop() error {
	if m.JID <= 0 {
		return nil
	}
	// Before removing jail, we should unmount anything we mounted manually
	// (like certs). Although jail removal often cleans up devfs,
	// nullfs mounts might persist on the host mount table if not careful.
	// However, removing the jail kills processes holding locks.

	// Try to unmount certs if we configured them
	if m.Config.MountSystemCerts {
		_ = unmount(filepath.Join(m.Config.Path, "etc", "ssl"))
		_ = unmount(filepath.Join(m.Config.Path, "usr", "share", "certs"))
	}

	return JailRemove(m.JID)
}

// Helpers

func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

func mountNullfsRO(src, dst string) error {
	// 0x4000 = MNT_RDONLY (generic), but go's syscall might not export MNT_RDONLY consistent with mount(2)
	// It's safer to use exec for mount_nullfs to handle the FS specific logic cleanly
	// unless we implement the specific nmount syscall structure.
	// For robustness in this library, we wrap mount_nullfs.

	cmd := exec.Command("/sbin/mount_nullfs", "-o", "ro", src, dst)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("mount_nullfs failed: %v (%s)", err, string(out))
	}
	return nil
}

func unmount(path string) error {
	return unix.Unmount(path, 0)
}
