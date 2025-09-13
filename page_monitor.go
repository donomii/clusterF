package main

import (
    "fmt"
    "net/http"
)

// handleMonitorDashboard serves a simple cluster monitoring dashboard
func (c *Cluster) handleMonitorDashboard(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/html; charset=utf-8")

    html := `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>🐸 Cluster Monitor - Node ` + string(c.ID) + `</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🐸</text></svg>">
    <style>
        body { font-family: Arial, sans-serif; background: #1a1a2e; color: white; margin: 0; padding: 20px; }
        .header { text-align: center; margin-bottom: 30px; }
        .header h1 { background: linear-gradient(45deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .frog-logo { position: relative; display: inline-block; line-height: 1; margin-right: 6px; }
        .frog-logo::after { content: ''; position: absolute; left: 50%; transform: translateX(-50%); bottom: -3px; width: 1.0em; height: 0.28em; background: radial-gradient(ellipse at center, rgba(6,182,212,0.35), rgba(6,182,212,0.05)); border-radius: 50% / 60%; filter: blur(0.3px); }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .stat-card { background: rgba(59, 130, 246, 0.1); border: 1px solid rgba(59, 130, 246, 0.3); border-radius: 8px; padding: 20px; text-align: center; }
        .stat-value { font-size: 2em; font-weight: bold; color: #06b6d4; }
        .stat-label { color: #94a3b8; margin-top: 5px; }
        .controls { text-align: center; margin-bottom: 30px; }
        .btn { background: linear-gradient(45deg, #3b82f6, #8b5cf6); color: white; border: none; padding: 12px 24px; border-radius: 6px; cursor: pointer; margin: 0 10px; }
        .btn:hover { transform: translateY(-2px); }
        .info { background: rgba(15, 15, 30, 0.9); border-radius: 8px; padding: 20px; font-family: monospace; font-size: 14px; }
    </style>
</head>
<body>
    <div class="header">
        <h1><span class="frog-logo" aria-hidden="true">🐸</span>Cluster Monitor</h1>
        <p>Node: ` + string(c.ID) + ` | Port: ` + fmt.Sprintf("%d", c.HTTPDataPort) + ` | <a href="/" style="color:#06b6d4;">← Back to Home</a></p>
    </div>
    
    <div class="stats-grid" id="stats">
        <div class="stat-card">
            <div class="stat-value" id="chunks">-</div>
            <div class="stat-label">Local Chunks</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="peers">-</div>
            <div class="stat-label">Connected Peers</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="replication_factor">-</div>
            <div class="stat-label">Replication Factor</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="tombstones">-</div>
            <div class="stat-label">Tombstones</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="under_replicated">-</div>
            <div class="stat-label">Under-Replicated</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="max_chunk_size_mb">-</div>
            <div class="stat-label">Max Chunk Size (MB)</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="local_partitions">-</div>
            <div class="stat-label">Local Partitions</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="total_files">-</div>
            <div class="stat-label">Total Files</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="pending_sync">-</div>
            <div class="stat-label">Pending Sync</div>
        </div>
    </div>
    
    <div class="controls">
        <button class="btn" onclick="addTestData()">📝 Add Test Data</button>
        <button class="btn" onclick="refreshStats()">🔄 Refresh</button>
        <button class="btn" onclick="openVisualizer()">📊 Open Visualizer</button>
        <input type="number" id="rfInput" min="1" max="20" style="margin: 0 10px; padding: 8px; border-radius: 4px; border: 1px solid #3b82f6; background: #1a1a2e; color: white; width: 60px;" placeholder="RF">
        <button class="btn" onclick="setReplicationFactor()">🔧 Set RF</button>
        <input type="number" id="maxSizeInput" min="1" max="1000" style="margin: 0 10px; padding: 8px; border-radius: 4px; border: 1px solid #3b82f6; background: #1a1a2e; color: white; width: 80px;" placeholder="MB">
        <button class="btn" onclick="setMaxChunkSize()">📁 Set Max Size</button>
    </div>
    
    <div class="info">
        <h3>Node Information</h3>
        <div>ID: ` + string(c.ID) + `</div>
        <div>HTTP Port: ` + fmt.Sprintf("%d", c.HTTPDataPort) + `</div>
        <div>Discovery Port: ` + fmt.Sprintf("%d", c.DiscoveryPort) + `</div>
        <div>Data Directory: ` + c.DataDir + `</div>
        <div>Endpoints:</div>
        <div style="margin-left: 20px;">
            <div>Status: <a href="/status" style="color: #06b6d4;">/status</a></div>
            <div>Monitor: <a href="/monitor" style="color: #06b6d4;">/monitor</a></div>
            <div>Cluster Stats: <a href="/api/cluster-stats" style="color: #06b6d4;">/api/cluster-stats</a></div>
            <div>Partition Stats: <a href="/api/partition-stats" style="color: #06b6d4;">/api/partition-stats</a></div>
            <div>Visualizer: <a href="/cluster-visualizer.html" style="color: #06b6d4;">cluster-visualizer.html</a></div>
        </div>
    </div>
    
    <script>
        async function refreshStats() {
            try {
                const response = await fetch('/status');
                const stats = await response.json();
                
                document.getElementById('chunks').textContent = stats.chunks || 0;
                document.getElementById('peers').textContent = stats.peers || 0;
                document.getElementById('replication_factor').textContent = stats.replication_factor || 3;
                document.getElementById('tombstones').textContent = stats.tombstones || 0;
                document.getElementById('under_replicated').textContent = stats.under_replicated || 0;
                document.getElementById('max_chunk_size_mb').textContent = stats.max_chunk_size_mb || 100;
                
                // Partition stats
                if (stats.partition_stats) {
                    document.getElementById('local_partitions').textContent = stats.partition_stats.local_partitions || 0;
                    document.getElementById('total_files').textContent = stats.partition_stats.total_files || 0;
                    document.getElementById('pending_sync').textContent = stats.partition_stats.pending_sync || 0;
                } else {
                    document.getElementById('local_partitions').textContent = 0;
                    document.getElementById('total_files').textContent = 0;
                    document.getElementById('pending_sync').textContent = 0;
                }
                
                // Update input fields with current values
                document.getElementById('rfInput').value = stats.replication_factor || 3;
                document.getElementById('maxSizeInput').value = stats.max_chunk_size_mb || 100;
            } catch (error) {
                console.error('Failed to fetch stats:', error);
            }
        }
        
        async function setMaxChunkSize() {
            try {
                const maxSizeInput = document.getElementById('maxSizeInput');
                const newMaxSize = parseInt(maxSizeInput.value);
                
                if (isNaN(newMaxSize) || newMaxSize < 1) {
                    alert('❌ Max chunk size must be a number >= 1MB');
                    return;
                }
                
                const response = await fetch('/api/max-chunk-size', {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ max_chunk_size_mb: newMaxSize })
                });
                
                if (response.ok) {
                    const result = await response.json();
                    alert('✅ Max chunk size set to ' + result.max_chunk_size_mb + 'MB');
                    refreshStats();
                } else {
                    const error = await response.text();
                    alert('❌ Failed to set max chunk size: ' + error);
                }
            } catch (error) {
                alert('Error: ' + error.message);
            }
        }
        
        async function addTestData() {
            try {
                const chunkId = 'test-chunk-' + Date.now();
                const testData = 'Test data created at ' + new Date().toLocaleString();
                
                // Replace the chunk calls with equivalent file calls
                const fileName = 'test-' + Date.now() + '.txt';
                const response = await fetch('/api/files/' + fileName, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'text/plain' },
                    body: testData
                });
                
                if (response.ok) {
                    alert('✅ Added test chunk: ' + chunkId);
                    refreshStats();
                } else {
                    alert('❌ Failed to add test data');
                }
            } catch (error) {
                alert('Error: ' + error.message);
            }
        }
        
        async function setReplicationFactor() {
            try {
                const rfInput = document.getElementById('rfInput');
                const newRF = parseInt(rfInput.value);
                
                if (isNaN(newRF) || newRF < 1) {
                    alert('❌ Replication factor must be a number >= 1');
                    return;
                }
                
                const response = await fetch('/api/replication-factor', {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ replication_factor: newRF })
                });
                
                if (response.ok) {
                    const result = await response.json();
                    alert('✅ Replication factor set to ' + result.replication_factor);
                    refreshStats();
                } else {
                    const error = await response.text();
                    alert('❌ Failed to set replication factor: ' + error);
                }
            } catch (error) {
                alert('Error: ' + error.message);
            }
        }
        
        function openVisualizer() {
            window.open('/cluster-visualizer.html', '_blank');
        }
        
        // Auto-refresh every 3 seconds
        refreshStats();
        setInterval(refreshStats, 3000);
    </script>
</body>
</html>`

    w.Write([]byte(html))
}

