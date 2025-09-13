//go:build !cgo

package main

import "log"

// StartDesktopUI is a no-op when CGO is disabled.
func StartDesktopUI(httpPort int) {
    log.Printf("Desktop UI disabled (built without CGO). Open http://localhost:%d/files/ in your browser.", httpPort)
}

