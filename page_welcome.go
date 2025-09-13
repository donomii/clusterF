package main

import (
    "fmt"
    "net/http"
)

// handleWelcome serves the welcome/root page
func (c *Cluster) handleWelcome(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/html; charset=utf-8")

    html := `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>🐸 Frogpond Cluster Node</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🐸</text></svg>">
    <style>
        body { font-family: Arial, sans-serif; background: #1a1a2e; color: white; margin: 0; padding: 40px; }
        .container { max-width: 800px; margin: 0 auto; text-align: center; }
        .header h1 { font-size: 3em; margin-bottom: 10px; background: linear-gradient(45deg, #3b82f6, #8b5cf6, #06b6d4); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .frog-logo { position: relative; display: inline-block; line-height: 1; margin-right: 6px; }
        .frog-logo::after { content: ''; position: absolute; left: 50%; transform: translateX(-50%); bottom: -3px; width: 1.1em; height: 0.3em; background: radial-gradient(ellipse at center, rgba(6,182,212,0.35), rgba(6,182,212,0.05)); border-radius: 50% / 60%; filter: blur(0.3px); }
        .subtitle { font-size: 1.2em; color: #94a3b8; margin-bottom: 40px; }
        .node-info { background: rgba(59, 130, 246, 0.1); border: 1px solid rgba(59, 130, 246, 0.3); border-radius: 12px; padding: 30px; margin: 30px 0; }
        .endpoints { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-top: 30px; }
        .endpoint-card { background: rgba(15, 15, 30, 0.9); border-radius: 8px; padding: 20px; border: 1px solid rgba(59, 130, 246, 0.2); }
        .endpoint-card h3 { color: #06b6d4; margin-bottom: 10px; }
        .endpoint-card a { color: #3b82f6; text-decoration: none; font-weight: bold; }
        .endpoint-card a:hover { text-decoration: underline; }
        .endpoint-card p { color: #94a3b8; font-size: 14px; margin: 10px 0; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1><span class="frog-logo" aria-hidden="true">🐸</span>Frogpond</h1>
            <div class="subtitle">Zero-Config Self-Organizing Storage Cluster</div>
        </div>
        
        <div class="node-info">
            <h2>Node: ` + string(c.ID) + `</h2>
            <p>HTTP Port: <strong>` + fmt.Sprintf("%d", c.HTTPDataPort) + `</strong></p>
            <p>Discovery Port: <strong>` + fmt.Sprintf("%d", c.DiscoveryPort) + `</strong></p>
            <p>Data Directory: <code>` + c.DataDir + `</code></p>
        </div>
        
        <div class="endpoints">
            <div class="endpoint-card">
                <h3>🔍 Profiling</h3>
                <p>Performance analysis and debugging tools</p>
                <a href="/profiling">Open Profiling</a>
            </div>
            
            <div class="endpoint-card">
                <h3>📁 File Browser</h3>
                <p>Web-based file manager with drag & drop</p>
                <a href="/files/">Open File Browser</a>
            </div>
            
            <div class="endpoint-card">
                <h3>📊 Monitor</h3>
                <p>Real-time node statistics and controls</p>
                <a href="/monitor">Open Monitor Dashboard</a>
            </div>
            
            <div class="endpoint-card">
                <h3>🧪 CRDT Inspector</h3>
                <p>Browse frogpond keys and values</p>
                <a href="/crdt">Open CRDT Inspector</a>
            </div>
            
            <div class="endpoint-card">
                <h3>📊 Cluster Visualizer</h3>
                <p>Interactive network visualization</p>
                <a href="/cluster-visualizer.html">Open Visualizer</a>
            </div>
            
            <div class="endpoint-card">
                <h3>🧩 API Reference</h3>
                <p>Browse all REST and JSON endpoints</p>
                <a href="/api">Open API Page</a>
            </div>
        </div>
        
        <div style="margin-top: 40px; color: #6b7280; font-size: 14px;">
            <p>This node is part of a self-organizing storage cluster.</p>
            <p>Add more nodes on the same network and they'll discover each other automatically.</p>
        </div>
    </div>
</body>
</html>`

    w.Write([]byte(html))
}

