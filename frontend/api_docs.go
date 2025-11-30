package frontend

import (
	"fmt"
	"net/http"
)

// HandleAPIDocs serves a human-friendly HTML page listing API endpoints.
func (f *Frontend) HandleAPIDocs(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api" { // keep exact match so /api/* routes work normally
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	nodeID := f.provider.NodeID()
	httpPort := fmt.Sprintf("%d", f.provider.HTTPPort())

	html := `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>🐸 API Reference - Node ` + nodeID + `</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🐸</text></svg>">
    <style>
        body { font-family: Arial, sans-serif; background: #0f172a; color: #e5e7eb; margin: 0; }
        .container { max-width: 1000px; margin: 0 auto; padding: 30px; }
        .header { text-align: center; margin-bottom: 20px; }
        .header h1 { background: linear-gradient(45deg, #3b82f6, #8b5cf6, #06b6d4); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0; }
        .card { background: rgba(15, 15, 30, 0.9); border: 1px solid rgba(59,130,246,0.3); border-radius: 10px; padding: 18px; margin: 14px 0; }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; }
        .section h2 { color: #06b6d4; margin: 24px 0 8px; }
        code, .code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
        .ep { margin: 10px 0; }
        .verb { padding: 2px 6px; border-radius: 4px; font-size: 12px; margin-right: 6px; }
        .GET { background: rgba(34,197,94,0.15); border: 1px solid rgba(34,197,94,0.4); }
        .PUT { background: rgba(59,130,246,0.15); border: 1px solid rgba(59,130,246,0.4); }
        .POST { background: rgba(234,179,8,0.15); border: 1px solid rgba(234,179,8,0.4); }
        .DELETE { background: rgba(239,68,68,0.15); border: 1px solid rgba(239,68,68,0.4); }
        a { color: #60a5fa; text-decoration: none; }
        a:hover { text-decoration: underline; }
        .back { display: inline-block; margin-top: 16px; }
    </style>
}</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🐸 API Reference</h1>
            <div style="color:#9ca3af">Node: ` + nodeID + ` • Port: ` + httpPort + `</div>
            <div style="margin-top:10px;"><a class="back" href="/">← Back to Home</a></div>
        </div>

        <div class="section">
            <h2>Core</h2>
            <div class="grid">
                <div class="card">
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/status</span></div>
                    <div>Node status JSON.</div>
                    <div><a href="/status" >Open</a></div>
                </div>
                <div class="card">
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/api/cluster-stats</span></div>
                    <div>Extended cluster information.</div>
                    <div><a href="/api/cluster-stats" >Open</a></div>
                </div>
                <div class="card">
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/api/partition-stats</span></div>
                    <div>Partition replication statistics.</div>
                    <div><a href="/api/partition-stats" >Open</a></div>
                </div>
                <div class="card">
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/api/under-replicated</span></div>
                    <div>Under-replicated partitions and files (local view).</div>
                    <div><a href="/api/under-replicated" >Open</a></div>
                </div>
            </div>
        </div>

        <div class="section">
            <h2>Files</h2>
            <div class="grid">
                <div class="card">
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/api/files/</span></div>
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/api/files/{path}</span></div>
                    <div class="ep"><span class="verb PUT">PUT</span> <span class="code">/api/files/{path}</span></div>
                    <div class="ep"><span class="verb DELETE">DELETE</span> <span class="code">/api/files/{path}</span></div>
                    <div class="ep"><span class="verb POST">POST</span> <span class="code">/api/files/{path}</span> (with <span class="code">X-Create-Directory: true</span>)</div>
                    <div>File system API.</div>
                    <div><a href="/api/files/" >Browse</a></div>
                </div>
            </div>
        </div>

        <div class="section">
            <h2>Configuration</h2>
            <div class="grid">
                <div class="card">
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/api/replication-factor</span></div>
                    <div class="ep"><span class="verb PUT">PUT</span> <span class="code">/api/replication-factor</span> { replication_factor }</div>
                    <div>Get/set replication factor.</div>
                </div>
               
            </div>
        </div>

        <div class="section">
            <h2>Diagnostics</h2>
            <div class="grid">
                <div class="card">
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/api/profiling</span></div>
                    <div class="ep"><span class="verb POST">POST</span> <span class="code">/api/profiling</span> { action: start|stop }</div>
                    <div>Control profiling mode.</div>
                </div>
                <div class="card">
                    <div>pprof endpoints under <span class="code">/debug/pprof</span></div>
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/debug/pprof/profile?seconds=30</span></div>
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/debug/pprof/heap</span>, <span class="code">/goroutine</span>, etc.</div>
                </div>
                <div class="card">
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/crdt</span></div>
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/api/crdt/list?prefix=...&limit=100&after=...</span></div>
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/api/crdt/search?prefix=...&q=...&limit=100&after=...</span></div>
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/api/crdt/get?key=...</span></div>
                    <div>CRDT inspector UI and APIs.</div>
                    <div><a href="/crdt" >Open</a></div>
                </div>
            </div>
        </div>

      

        <div class="section">
            <h2>Internal (Frogpond)</h2>
            <div class="grid">
                <div class="card">
                    <div class="ep"><span class="verb POST">POST</span> <span class="code">/frogpond/update</span></div>
                    <div class="ep"><span class="verb POST">POST</span> <span class="code">/frogpond/fullsync</span></div>
                    <div class="ep"><span class="verb GET">GET</span> <span class="code">/frogpond/fullstore</span></div>
                    <div>Peer-to-peer CRDT synchronization (internal use).</div>
                </div>
            </div>
        </div>

    </div>
</body>
</html>`

	w.Write([]byte(html))
}
