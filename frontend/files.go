package frontend

import "net/http"

// HandleFiles serves the file browser interface.
func (f *Frontend) HandleFiles(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	html := f.generateFileBrowserHTML()
	w.Write([]byte(html))
}

func (f *Frontend) generateFileBrowserHTML() string {
	if content, err := loadAsset("file-browser.html"); err == nil {
		return string(content)
	}

	return `<!DOCTYPE html>
    <html><head><meta charset="utf-8"><title>File Browser</title><link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🐸</text></svg>"></head>
    <body style="font-family: Arial; padding: 40px; background: #1a1a2e; color: white;">
    <h1>🐸 Cluster File Browser</h1>
    <p><a href="/" style="color:#06b6d4; text-decoration:none;">← Back to Home</a></p>
    <p>File browser interface temporarily unavailable.</p>
    <p>The file-browser.html file is missing. Use the API endpoints:</p>
    <ul>
    <li>GET /api/files/ - List root directory</li>
    <li>GET /api/files/path - List directory or download file</li>
    <li>PUT /api/files/path - Upload file</li>
    <li>DELETE /api/files/path - Delete file</li>
    <li><a href="/api">API Reference</a></li>
    <li><a href="/monitor">Monitor</a></li>
    <li><a href="/cluster-visualizer.html">Visualizer</a></li>
    <li><a href="/files/">File Browser</a></li>
    <li><a href="/">Home</a></li>
    </ul>
    </body></html>`
}
