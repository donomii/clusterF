//go:build cgo

// desktop_ui.go - Simple desktop window for drag-and-drop using system WebView
package main

import (
    "fmt"
    "runtime"

    webview "github.com/webview/webview_go"
)

// StartDesktopUI opens a native window that navigates to the built-in file browser
// so you can drag-and-drop files without using an external browser.
func StartDesktopUI(httpPort int) {
    // Open the file browser directly; it has its own first-load splash
    url := fmt.Sprintf("http://localhost:%d/files/", httpPort)
    title := "Frogpond – Drop Files"

    // webview requires main thread on macOS
    if runtime.GOOS == "darwin" {
        runtime.LockOSThread()
        defer runtime.UnlockOSThread()
    }

    debug := false
    w := webview.New(debug)
    defer w.Destroy()
    w.SetTitle(title)
    w.SetSize(1000, 720, webview.HintNone)
    // On macOS, set a frog emoji as the Dock icon
    if runtime.GOOS == "darwin" {
        setDockEmojiIcon("🐸", 256)
    }
    w.Navigate(url)
    w.Run()
}
