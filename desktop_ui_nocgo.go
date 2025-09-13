//go:build !cgo

package main

// StartDesktopUI is a no-op when CGO is disabled. Intentionally silent.
func StartDesktopUI(httpPort int) {}
