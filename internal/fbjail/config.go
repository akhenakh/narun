//go:build freebsd

package fbjail

import (
	"net"
)

// JailConfig defines the parameters for creating a jail.
type JailConfig struct {
	// Name of the jail (must be unique).
	Name string
	// Path is the root directory of the jail.
	Path string
	// Hostname to set inside the jail.
	Hostname string

	// IP4Addresses to assign to the jail.
	IP4Addresses []string
	// IP6Addresses to assign to the jail.
	IP6Addresses []string

	// Filesystem & Security
	MountDevfs      bool
	DevfsRuleset    int
	EnforceStatfs   int
	AllowRawSockets bool
	Persist         bool

	// Integrations
	CopyResolvConf   bool
	MountSystemCerts bool
}

// ToParams converts the config struct to the map required by jail_set.
func (c *JailConfig) ToParams() map[string]interface{} {
	params := make(map[string]interface{})

	params["name"] = c.Name
	params["path"] = c.Path
	params["host.hostname"] = c.Hostname

	if c.Persist {
		params["persist"] = nil
	}

	// IPv4 Setup
	if len(c.IP4Addresses) > 0 {
		ipData := make([]byte, 0, len(c.IP4Addresses)*4)
		for _, ipStr := range c.IP4Addresses {
			ip := net.ParseIP(ipStr)
			if ip != nil {
				ip4 := ip.To4()
				if ip4 != nil {
					ipData = append(ipData, ip4...)
				}
			}
		}
		if len(ipData) > 0 {
			params["ip4.addr"] = ipData
			params["ip4"] = int32(1) // JAIL_SYS_NEW: Allow IPv4
		}
	} else {
		// Explicitly disable if none provided (optional, but good practice)
		// params["ip4"] = int32(0)
	}

	// IPv6 Setup
	if len(c.IP6Addresses) > 0 {
		ipData := make([]byte, 0, len(c.IP6Addresses)*16)
		for _, ipStr := range c.IP6Addresses {
			ip := net.ParseIP(ipStr)
			if ip != nil {
				ip6 := ip.To16()
				if ip6 != nil {
					ipData = append(ipData, ip6...)
				}
			}
		}
		if len(ipData) > 0 {
			params["ip6.addr"] = ipData
			params["ip6"] = int32(1) // JAIL_SYS_NEW: Allow IPv6
		}
	} else {
		// CRITICAL: Explicitly DISABLE IPv6 if no addresses are assigned.
		// Otherwise, applications (like Go's net pkg) might try to create AF_INET6 sockets
		// and receive "protocol not supported" errors.
		params["ip6"] = int32(0) // JAIL_SYS_DISABLE
	}

	if c.MountDevfs {
		if c.DevfsRuleset > 0 {
			params["devfs_ruleset"] = int32(c.DevfsRuleset)
		}
	}

	if c.EnforceStatfs > 0 {
		params["enforce_statfs"] = int32(c.EnforceStatfs)
	}

	if c.AllowRawSockets {
		params["allow.raw_sockets"] = nil
	}

	return params
}
