package main

import (
    "net/http"
    "os"
)

// handleVisualizer serves the cluster visualizer HTML
func (c *Cluster) handleVisualizer(w http.ResponseWriter, r *http.Request) {
    // Read the cluster-visualizer.html file and serve it
    visualizerPath := "cluster-visualizer.html"
    content, err := os.ReadFile(visualizerPath)
    if err != nil {
        // If file doesn't exist, return a helpful error
        w.Header().Set("Content-Type", "text/html; charset=utf-8")
        w.WriteHeader(http.StatusNotFound)
        w.Write([]byte(`<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Visualizer Not Found</title><link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🐸</text></svg>"></head>
<body style="font-family: Arial; padding: 40px; background: #1a1a2e; color: white;">
<h1>🐸 Cluster Visualizer Not Found</h1>
<p>The cluster-visualizer.html file is missing from the current directory.</p>
<p>Make sure cluster-visualizer.html is in the same directory as the cluster executable.</p>
<p><a href="/" style="color: #06b6d4;">← Back to Home</a> · <a href="/monitor" style="color: #06b6d4;">Go to Node Monitor</a></p>
</body></html>`))
        return
    }

    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    w.Write(content)
}

