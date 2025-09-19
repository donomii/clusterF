package frontend

import "net/http"

// HandleProfilingPage serves the profiling control page.
func (f *Frontend) HandleProfilingPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	html := `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>🔍 Profiling - Node ` + f.provider.NodeID() + `</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🐸</text></svg>">
    <style>
        body { font-family: Arial, sans-serif; background: #1a1a2e; color: white; margin: 0; padding: 20px; }
        .header { text-align: center; margin-bottom: 30px; }
        .header h1 { background: linear-gradient(45deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .controls { text-align: center; margin-bottom: 30px; }
        .btn { background: linear-gradient(45deg, #3b82f6, #8b5cf6); color: white; border: none; padding: 12px 24px; border-radius: 6px; cursor: pointer; margin: 0 10px; }
        .btn:hover { transform: translateY(-2px); }
        .btn.danger { background: linear-gradient(45deg, #ef4444, #dc2626); }
        .status { background: rgba(15, 15, 30, 0.9); border-radius: 8px; padding: 20px; margin: 20px 0; }
        .profiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }
        .profile-card { background: rgba(59, 130, 246, 0.1); border: 1px solid rgba(59, 130, 246, 0.3); border-radius: 8px; padding: 20px; text-align: center; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🔍 Performance Profiling</h1>
        <p>Node: ` + f.provider.NodeID() + ` | <a href="/" style="color: #06b6d4;">← Back to Home</a></p>
    </div>
    
    <div class="status">
        <h3>Profiling Status</h3>
        <div>Status: <span id="profilingStatus">Loading...</span></div>
        <div style="margin-top: 20px;">
            <button class="btn" onclick="startProfiling()" id="startBtn">🚀 Start Profiling</button>
            <button class="btn danger" onclick="stopProfiling()" id="stopBtn">🛑 Stop Profiling</button>
        </div>
    </div>
    
    <div class="profiles">
        <div class="profile-card">
            <h3>CPU Flame Graph</h3>
            <p>Click to generate and view flame graph</p>
            <a href="/flamegraph" class="btn">🔥 Generate Flame Graph</a>
        </div>
    </div>
    
    <div class="status">
        <h3>Usage Instructions</h3>
        <ul style="text-align: left; max-width: 600px; margin: 0 auto;">
            <li>Click "Start Profiling" to enable detailed profiling</li>
            <li>Use your application normally to generate load</li>
            <li>Click "Generate Flame Graph" to view CPU flame graph</li>
            <li>Click "Stop Profiling" to disable when done</li>
        </ul>
    </div>
    
    <script>
        async function updateStatus() {
            try {
                const response = await fetch('/api/profiling');
                const data = await response.json();
                
                const status = document.getElementById('profilingStatus');
                const startBtn = document.getElementById('startBtn');
                const stopBtn = document.getElementById('stopBtn');
                
                if (data.active) {
                    status.textContent = '🟢 Active';
                    status.style.color = '#10b981';
                    startBtn.disabled = true;
                    stopBtn.disabled = false;
                } else {
                    status.textContent = '🔴 Inactive';
                    status.style.color = '#ef4444';
                    startBtn.disabled = false;
                    stopBtn.disabled = true;
                }
            } catch (error) {
                console.error('Failed to fetch profiling status:', error);
            }
        }
        
        async function startProfiling() {
            try {
                const response = await fetch('/api/profiling', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ action: 'start' })
                });
                
                if (response.ok) {
                    updateStatus();
                } else {
                    alert('Failed to start profiling');
                }
            } catch (error) {
                alert('Error: ' + error.message);
            }
        }
        
        async function stopProfiling() {
            try {
                const response = await fetch('/api/profiling', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ action: 'stop' })
                });
                
                if (response.ok) {
                    updateStatus();
                } else {
                    alert('Failed to stop profiling');
                }
            } catch (error) {
                alert('Error: ' + error.message);
            }
        }
        
        // Update status on page load and every 5 seconds
        updateStatus();
        setInterval(updateStatus, 5000);
    </script>
</body>
</html>`

	w.Write([]byte(html))
}
