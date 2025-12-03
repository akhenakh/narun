//go:build freebsd

package fbjail

import (
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	sysJailSet    = 507
	sysJailRemove = 508
	sysJailAttach = 436
)

// Common jail flags
const (
	JAIL_CREATE = uintptr(0x01)
	JAIL_UPDATE = uintptr(0x02)
	JAIL_ATTACH = uintptr(0x04)
	JAIL_DYING  = uintptr(0x08)
)

// buildIovec converts a map of parameters into the format required by jail_set.
// It handles strings, integers, and booleans.
func buildIovec(params map[string]interface{}) ([]unix.Iovec, error) {
	var iovecs []unix.Iovec

	for k, v := range params {
		// Key (Parameter Name)
		keyBytes, err := unix.BytePtrFromString(k)
		if err != nil {
			return nil, err
		}
		iovecs = append(iovecs, unix.Iovec{
			Base: keyBytes,
			Len:  uint64(len(k) + 1),
		})

		// Value
		var valPtr *byte
		var valSize uint64

		if v == nil {
			// Boolean flag (no value)
			valSize = 0
		} else {
			rv := reflect.ValueOf(v)
			switch rv.Kind() {
			case reflect.String:
				str := rv.String()
				sBytes, err := unix.BytePtrFromString(str)
				if err != nil {
					return nil, err
				}
				valPtr = sBytes
				valSize = uint64(len(str) + 1)
			case reflect.Int, reflect.Int32:
				i := int32(rv.Int())
				valPtr = (*byte)(unsafe.Pointer(&i))
				valSize = 4 // sizeof(int32)
			case reflect.Uint32:
				i := uint32(rv.Uint())
				valPtr = (*byte)(unsafe.Pointer(&i))
				valSize = 4
			case reflect.Bool:
				// Jails often treat booleans as value-less keys, but if a value is needed:
				// usually mapped to string "true"/"false" or handled via existence.
				// Here we assume existence unless specific logic requires value.
				// Narun usage mostly maps bools to presence of keys.
				valSize = 0
			default:
				return nil, fmt.Errorf("unsupported type for jail param %s: %T", k, v)
			}
		}

		iovecs = append(iovecs, unix.Iovec{
			Base: valPtr,
			Len:  valSize,
		})
	}

	return iovecs, nil
}

// JailSet creates or updates a jail. Returns the JID (Jail ID) on success.
func JailSet(params map[string]interface{}, flags uintptr) (int, error) {
	iov, err := buildIovec(params)
	if err != nil {
		return -1, err
	}

	// Make the syscall
	// SYS_JAIL_SET(iov, niov, flags)
	jid, _, e1 := unix.Syscall(
		sysJailSet,
		uintptr(unsafe.Pointer(&iov[0])),
		uintptr(len(iov)),
		flags,
	)

	if e1 != 0 {
		return -1, fmt.Errorf("jail_set failed: %w", e1)
	}

	return int(jid), nil
}

// JailRemove kills a jail by JID.
func JailRemove(jid int) error {
	_, _, e1 := unix.Syscall(sysJailRemove, uintptr(jid), 0, 0)
	if e1 != 0 {
		if e1 == unix.EINVAL {
			return errors.New("jail not found")
		}
		return fmt.Errorf("jail_remove failed: %w", e1)
	}
	return nil
}

// JailAttach attaches the current process to a jail.
// WARNING: This affects the calling thread. In Go, this must be called
// inside a LockedOSThread, or preferably, by a child process that immediately execs.
func JailAttach(jid int) error {
	_, _, e1 := unix.Syscall(sysJailAttach, uintptr(jid), 0, 0)
	if e1 != 0 {
		return fmt.Errorf("jail_attach failed: %w", e1)
	}
	return nil
}
