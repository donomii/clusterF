package frontend

import "net/http"

// HandleVisualizer serves the cluster visualizer HTML.
func (f *Frontend) HandleVisualizer(w http.ResponseWriter, r *http.Request) {
	content, err := loadAsset("cluster-visualizer.html")
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Visualizer Not Found</title><link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🐸</text></svg>"></head>
<body style="font-family: Arial; padding: 40px; background: #1a1a2e; color: white;">
<h1>🐸 Cluster Visualizer Not Found</h1>
<p>The cluster-visualizer.html file is missing from the frontend assets.</p>
<p><a href="/" style="color: #06b6d4;">← Back to Home</a> · <a href="/monitor" style="color: #06b6d4;">Go to Node Monitor</a></p>
</body></html>`))
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(content)
}
