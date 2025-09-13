//go:build !linux && !darwin

package main

import (
    "net"
)

// newListenConfig returns a default ListenConfig without OS-specific socket options
func newListenConfig() *net.ListenConfig {
    return &net.ListenConfig{}
}

