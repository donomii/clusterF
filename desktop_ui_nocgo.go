//go:build !cgo

package main

// StartDesktopUI is a no-op when CGO is disabled. Intentionally silent.
func StartDesktopUI(httpPort int, cluster *Cluster) {}

// desktopUISupported reports whether the desktop UI is available in this build.
func desktopUISupported() bool { return false }
