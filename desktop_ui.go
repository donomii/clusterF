//go:build cgo

// desktop_ui.go - Simple desktop window for drag-and-drop using system WebView
package main

import (
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	webview "github.com/webview/webview_go"
)

// Global variable to track webview shutdown
var desktopUIShutdown int32

// desktopUISupported reports whether the desktop UI is available in this build.
func desktopUISupported() bool {
	return true
}

// StartDesktopUI opens a native window that navigates to the built-in file browser
// so you can drag-and-drop files without using an external browser.
func StartDesktopUI(httpPort int, cluster *Cluster) {
	// Check for graphics environment on Linux before trying to create webview
	if runtime.GOOS == "linux" {
		if os.Getenv("DISPLAY") == "" && os.Getenv("WAYLAND_DISPLAY") == "" {
			fmt.Printf("[UI] No graphics display detected (DISPLAY and WAYLAND_DISPLAY not set), skipping desktop UI\n")
			return
		}
	}

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

	// Bind quit function that properly shuts down the entire application
	w.Bind("quitApp", func() {
		atomic.StoreInt32(&desktopUIShutdown, 1)
		go func() {
			cluster.Stop()
			time.Sleep(10 * time.Second)
			os.Exit(0)
		}()
		w.Terminate()
	})

	// Navigate to URL
	w.Navigate(url)

	// Inject JavaScript to handle copy operations globally
	w.Init(`
		// Override the keydown event to prevent beeping on copy
		document.addEventListener('keydown', function(e) {
			// Handle Cmd+C/Ctrl+C for copy
			if ((e.metaKey || e.ctrlKey) && e.key === 'c') {
				var selection = window.getSelection();
				if (selection.toString().length > 0) {
					e.preventDefault();
					e.stopPropagation();
					try {
						navigator.clipboard.writeText(selection.toString());
					} catch (err) {
						document.execCommand('copy');
					}
					return false;
				}
			}
			
			// Handle quit shortcuts
			if ((e.metaKey && e.key === 'q') || (e.ctrlKey && e.key === 'q')) {
				e.preventDefault();
				if (window.quitApp) {
					window.quitApp();
				}
			}
			if ((e.metaKey && e.key === 'w') || (e.ctrlKey && e.key === 'w')) {
				e.preventDefault();
				if (window.quitApp) {
					window.quitApp();
				}
			}
		}, true);

		// Handle window close attempts
		window.addEventListener('beforeunload', function(e) {
			if (window.quitApp) {
				window.quitApp();
			}
		});

		// Add quit button to toolbar when page loads
		function addQuitButton() {
			var toolbar = document.querySelector('.toolbar');
			if (toolbar && !document.getElementById('quitButton')) {
				var quitBtn = document.createElement('button');
				quitBtn.id = 'quitButton';
				quitBtn.className = 'btn';
				quitBtn.innerHTML = '🚪 Quit';
				quitBtn.onclick = function() {
					if (window.quitApp) {
						window.quitApp();
					}
				};
				toolbar.appendChild(quitBtn);
			}
		}

		if (document.readyState === 'loading') {
			document.addEventListener('DOMContentLoaded', addQuitButton);
		} else {
			addQuitButton();
		}
	`)

	w.Run()
}
