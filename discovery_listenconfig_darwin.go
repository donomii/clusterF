//go:build darwin

package main

import (
    "net"
    "syscall"
)

// newListenConfig configures SO_REUSEADDR and SO_REUSEPORT on macOS
func newListenConfig() *net.ListenConfig {
    return &net.ListenConfig{
        Control: func(network, address string, rc syscall.RawConn) error {
            return rc.Control(func(fd uintptr) {
                _ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
                _ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)
            })
        },
    }
}

