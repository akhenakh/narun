//go:build freebsd

package fbjail

import (
	"strings"
)

// JailConfig defines the parameters for creating a Narun-compatible jail.
type JailConfig struct {
	// Name of the jail (must be unique).
	Name string
	// Path is the root directory of the jail.
	Path string
	// Hostname to set inside the jail.
	Hostname string

	// Network Configuration
	// IP4Addresses is a list of IPv4 addresses to assign to the jail.
	// Format: "127.0.1.1" or "192.168.1.50".
	// If empty, the jail will not have network access (unless VNET is used, not implemented here).
	IP4Addresses []string

	// Filesystem & Security
	// MountDevfs mounts the device filesystem inside the jail.
	MountDevfs bool
	// DevfsRuleset determines which devices are visible (4 is standard jail ruleset).
	DevfsRuleset int
	// EnforceStatfs controls mount point visibility (2 = jail root only).
	EnforceStatfs int
	// AllowRawSockets enables ping, traceroute, etc.
	AllowRawSockets bool
	// Persist keeps the jail alive even if no processes are running.
	Persist bool

	// Narun Specific Integrations
	// CopyResolvConf will copy the host's /etc/resolv.conf into the jail.
	CopyResolvConf bool
	// MountSystemCerts will nullfs mount host SSL certs into the jail.
	MountSystemCerts bool
}

// ToParams converts the config struct to the map required by jail_set.
func (c *JailConfig) ToParams() map[string]interface{} {
	params := make(map[string]interface{})

	params["name"] = c.Name
	params["path"] = c.Path
	params["host.hostname"] = c.Hostname

	// Default to persist, usually required for manager-based lifecycles
	if c.Persist {
		params["persist"] = nil
	}

	// Networking: standard IP sharing (Aliases)
	if len(c.IP4Addresses) > 0 {
		// Clean and join addresses
		// For jails, if we want to bind to specific interfaces, we might need interface|ip format.
		// However, standard ip4.addr="1.2.3.4,5.6.7.8" works if aliases exist on *any* interface.
		// Narun expects the runner to have set up aliases (e.g., on lo1).
		params["ip4.addr"] = strings.Join(c.IP4Addresses, ",")
	} else {
		params["ip4"] = "disable"
	}

	// DevFS
	if c.MountDevfs {
		params["mount.devfs"] = nil
		if c.DevfsRuleset > 0 {
			params["devfs_ruleset"] = int32(c.DevfsRuleset)
		}
	}

	// Security / Visibility
	params["enforce_statfs"] = int32(c.EnforceStatfs)

	if c.AllowRawSockets {
		params["allow.raw_sockets"] = nil
	}

	// "new" indicates we want to restrict sysvipc to this jail's namespace
	params["sysvmsg"] = "new"
	params["sysvsem"] = "new"
	params["sysvshm"] = "new"

	return params
}
