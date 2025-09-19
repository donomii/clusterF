//go:build linux

package discovery

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// newListenConfig configures SO_REUSEADDR and SO_REUSEPORT on Linux using x/sys/unix
func newListenConfig() *net.ListenConfig {
	return &net.ListenConfig{
		Control: func(network, address string, rc syscall.RawConn) error {
			return rc.Control(func(fd uintptr) {
				// Best-effort; ignore errors
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
			})
		},
	}
}
