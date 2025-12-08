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
	if cfg.DevfsRuleset == 0 {
		cfg.DevfsRuleset = 4 // standard devfsrules_jail
	}
	if cfg.EnforceStatfs == 0 {
		cfg.EnforceStatfs = 2 // mount point visibility: root only
	}
	return &Manager{Config: cfg, JID: -1}
}

// Create creates the jail in the kernel and performs setup.
func (m *Manager) Create() error {
	// Prepare Filesystem requirements
	if err := m.setupFilesystem(); err != nil {
		return fmt.Errorf("fs setup failed: %w", err)
	}

	// Call jail_set
	params := m.Config.ToParams()

	// Use JAIL_CREATE only. Updating 'path' on an existing jail is forbidden.
	jid, err := JailSet(params, JAIL_CREATE)
	if err != nil {
		return err
	}
	m.JID = jid

	// Manual Devfs Mount if requested
	if m.Config.MountDevfs {
		if err := m.mountDevfs(); err != nil {
			// Try to cleanup
			_ = JailRemove(jid)
			return fmt.Errorf("failed to mount devfs: %w", err)
		}
	}

	return nil
}

// mountDevfs mounts the devfs filesystem inside the jail root.
func (m *Manager) mountDevfs() error {
	devPath := filepath.Join(m.Config.Path, "dev")
	if err := os.MkdirAll(devPath, 0755); err != nil {
		return fmt.Errorf("failed to create dev dir: %w", err)
	}

	args := []string{"-t", "devfs"}
	if m.Config.DevfsRuleset > 0 {
		args = append(args, "-o", fmt.Sprintf("ruleset=%d", m.Config.DevfsRuleset))
	}
	args = append(args, "devfs", devPath)

	cmd := exec.Command("/sbin/mount", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("mount cmd failed: %v (%s)", err, string(out))
	}
	return nil
}

// setupFilesystem handles file copying and mounting before jail creation.
func (m *Manager) setupFilesystem() error {
	destEtc := filepath.Join(m.Config.Path, "etc")
	if err := os.MkdirAll(destEtc, 0755); err != nil {
		return err
	}

	// Generate /etc/hosts so localhost resolves locally without DNS
	if err := m.createHostsFile(destEtc); err != nil {
		return fmt.Errorf("failed to create hosts file: %w", err)
	}

	// DNS: Copy resolv.conf
	if m.Config.CopyResolvConf {
		// Only copy if it exists on host
		if _, err := os.Stat("/etc/resolv.conf"); err == nil {
			if err := copyFile("/etc/resolv.conf", filepath.Join(destEtc, "resolv.conf")); err != nil {
				return fmt.Errorf("failed to copy resolv.conf: %w", err)
			}
		}
	}

	// Certs
	if m.Config.MountSystemCerts {
		// Standard FreeBSD locations
		paths := []string{"/etc/ssl", "/usr/share/certs"}
		for _, src := range paths {
			if _, err := os.Stat(src); err == nil {
				dst := filepath.Join(m.Config.Path, src)
				if err := os.MkdirAll(dst, 0755); err != nil {
					return err
				}
				if err := mountNullfsRO(src, dst); err != nil {
					return fmt.Errorf("failed to mount %s: %w", src, err)
				}
			}
		}
	}

	return nil
}

// createHostsFile generates a minimal /etc/hosts inside the jail
func (m *Manager) createHostsFile(destEtc string) error {
	content := "127.0.0.1\tlocalhost\n"

	// Only add ipv6 localhost if we have ipv6 enabled, otherwise it might confuse resolvers
	if len(m.Config.IP6Addresses) > 0 {
		content += "::1\t\tlocalhost\n"
	}

	// If the jail has a specific IP and hostname, add it
	if len(m.Config.IP4Addresses) > 0 && m.Config.Hostname != "" {
		content += fmt.Sprintf("%s\t%s\n", m.Config.IP4Addresses[0], m.Config.Hostname)
	}

	return os.WriteFile(filepath.Join(destEtc, "hosts"), []byte(content), 0644)
}

// Stop removes the jail and unmounts resources.
func (m *Manager) Stop() error {
	if m.JID <= 0 {
		return nil
	}

	// Remove jail FIRST. This ensures all processes inside are killed.
	removeErr := JailRemove(m.JID)

	// Unmount Devfs if it was mounted
	if m.Config.MountDevfs {
		_ = unmount(filepath.Join(m.Config.Path, "dev"))
	}

	// Best-effort unmounts for certs
	if m.Config.MountSystemCerts {
		_ = unmount(filepath.Join(m.Config.Path, "etc", "ssl"))
		_ = unmount(filepath.Join(m.Config.Path, "usr", "share", "certs"))
	}

	return removeErr
}

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
	cmd := exec.Command("/sbin/mount_nullfs", "-o", "ro", src, dst)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("mount_nullfs failed: %v (%s)", err, string(out))
	}
	return nil
}

func unmount(path string) error {
	// Try standard unmount first
	err := unix.Unmount(path, 0)
	if err != nil {
		// If busy or failed, try forced unmount
		return unix.Unmount(path, unix.MNT_FORCE)
	}
	return nil
}
