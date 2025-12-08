//go:build freebsd

package fbjail

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	sysJailSet    = 507
	sysJailRemove = 508
	sysJailAttach = 436
)

const (
	JAIL_CREATE = uintptr(0x01)
	JAIL_UPDATE = uintptr(0x02)
	JAIL_ATTACH = uintptr(0x04)
	JAIL_DYING  = uintptr(0x08)
)

// buildIovec prepares the parameter array for the syscall.
// It returns the iovec slice and a slice of objects (keepAlive) that must be retained until the syscall returns.
func buildIovec(params map[string]interface{}) ([]unix.Iovec, []interface{}, error) {
	var iovecs []unix.Iovec
	var keepAlive []interface{}

	for k, v := range params {
		keyBytes, err := unix.ByteSliceFromString(k)
		if err != nil {
			return nil, nil, err
		}
		keepAlive = append(keepAlive, keyBytes)
		iovecs = append(iovecs, unix.Iovec{
			Base: &keyBytes[0],
			Len:  uint64(len(keyBytes)),
		})

		var valPtr *byte
		var valSize uint64

		if v == nil {
			// Boolean Flag (value is NULL, len is 0)
			valPtr = nil
			valSize = 0
		} else {
			rv := reflect.ValueOf(v)
			switch rv.Kind() {
			case reflect.String:
				str := rv.String()
				sBytes, err := unix.ByteSliceFromString(str)
				if err != nil {
					return nil, nil, err
				}
				keepAlive = append(keepAlive, sBytes)
				valPtr = &sBytes[0]
				valSize = uint64(len(sBytes))

			case reflect.Int, reflect.Int32, reflect.Int64:
				// Jail parameters expecting integers are strictly 32-bit
				val := int32(rv.Int())
				b := make([]byte, 4)
				*(*int32)(unsafe.Pointer(&b[0])) = val
				keepAlive = append(keepAlive, b)
				valPtr = &b[0]
				valSize = 4

			case reflect.Uint, reflect.Uint32, reflect.Uint64:
				val := uint32(rv.Uint())
				b := make([]byte, 4)
				*(*uint32)(unsafe.Pointer(&b[0])) = val
				keepAlive = append(keepAlive, b)
				valPtr = &b[0]
				valSize = 4

			case reflect.Slice:
				// Handle []byte (specifically for 'errmsg' buffer)
				if b, ok := v.([]byte); ok && len(b) > 0 {
					keepAlive = append(keepAlive, b)
					valPtr = &b[0]
					valSize = uint64(len(b))
				} else {
					valPtr = nil
					valSize = 0
				}

			default:
				return nil, nil, fmt.Errorf("unsupported type for param '%s': %T", k, v)
			}
		}

		iovecs = append(iovecs, unix.Iovec{
			Base: valPtr,
			Len:  valSize,
		})
	}

	return iovecs, keepAlive, nil
}

// JailSet creates or updates a jail.
func JailSet(params map[string]interface{}, flags uintptr) (int, error) {
	// Inject errmsg buffer to capture kernel complaints
	errMsg := make([]byte, 256)
	params["errmsg"] = errMsg

	iov, keepAlive, err := buildIovec(params)
	if err != nil {
		return -1, err
	}

	jid, _, e1 := unix.Syscall(
		sysJailSet,
		uintptr(unsafe.Pointer(&iov[0])),
		uintptr(len(iov)),
		flags,
	)

	// Keep memory alive until syscall returns
	runtime.KeepAlive(keepAlive)

	if e1 != 0 {
		// Check if the kernel wrote an error message
		kernelMsg := string(bytes.TrimRight(errMsg, "\x00"))
		if kernelMsg != "" {
			return -1, fmt.Errorf("jail_set error: %w | kernel message: %s", e1, kernelMsg)
		}
		return -1, fmt.Errorf("jail_set error: %w", e1)
	}

	return int(jid), nil
}

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

func JailAttach(jid int) error {
	_, _, e1 := unix.Syscall(sysJailAttach, uintptr(jid), 0, 0)
	if e1 != 0 {
		return fmt.Errorf("jail_attach failed: %w", e1)
	}
	return nil
}
