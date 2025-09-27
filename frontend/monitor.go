package frontend

import (
	"fmt"
	"net/http"
)

// HandleMonitorDashboard serves a simple cluster monitoring dashboard.
func (f *Frontend) HandleMonitorDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	nodeID := f.provider.NodeID()
	httpPort := fmt.Sprintf("%d", f.provider.HTTPPort())
	discoveryPort := fmt.Sprintf("%d", f.provider.DiscoveryPortVal())
	dataDir := f.provider.DataDirPath()

	html := `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>🐸 Cluster Monitor - Node ` + nodeID + `</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🐸</text></svg>">
    <style>
        body { font-family: Arial, sans-serif; background: #1a1a2e; color: white; margin: 0; padding: 20px; }
        .header { text-align: center; margin-bottom: 30px; }
        .header h1 { background: linear-gradient(45deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .frog-logo { position: relative; display: inline-block; line-height: 1; margin-right: 6px; }
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
        <h1>🐸 Cluster Monitor</h1>
        <p>Node: ` + nodeID + ` | Port: ` + httpPort + ` | <a href="/" style="color:#06b6d4;">← Back to Home</a></p>
    </div>
    
    <div class="stats-grid" id="stats">
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
        <div class="stat-card">
            <div class="stat-value" id="cluster_bytes_stored">-</div>
            <div class="stat-label">Cluster Bytes Stored</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="cluster_disk_usage">-</div>
            <div class="stat-label">Cluster Disk Usage %</div>
        </div>
        <div class="stat-card">
            <div class="stat-value" id="cluster_disk_free">-</div>
            <div class="stat-label">Cluster Disk Free</div>
        </div>
    </div>
    
    <div class="controls">
        <button class="btn" onclick="addTestData()">📝 Add Test Data</button>
        <button class="btn" onclick="refreshStats()">🔄 Refresh</button>
        <button class="btn" onclick="openVisualizer()">📊 Open Visualizer</button>
        <input type="number" id="rfInput" min="1" max="20" style="margin: 0 10px; padding: 8px; border-radius: 4px; border: 1px solid #3b82f6; background: #1a1a2e; color: white; width: 60px;" placeholder="RF">
        <button class="btn" onclick="setReplicationFactor()">🔧 Set RF</button>
        <input type="number" id="maxSizeInput" min="1" max="1000" style="margin: 0 10px; padding: 8px; border-radius: 4px; border: 1px solid #3b82f6; background: #1a1a2e; color: white; width: 80px;" placeholder="MB">
    </div>
    
        <div class="info">
        <h3>Node Information</h3>
        <div>ID: ` + nodeID + `</div>
        <div>HTTP Port: ` + httpPort + `</div>
        <div>Discovery Port: ` + discoveryPort + `</div>
        <div>Data Directory: ` + dataDir + `</div>
        <div>Bytes Stored: <span id="node_bytes_stored">-</span></div>
        <div>Disk Usage: <span id="node_disk_usage">-</span></div>
        <div>Disk Free: <span id="node_disk_free">-</span></div>
        <div id="debug-info" style="margin-top: 10px; color: #facc15; font-size: 12px;">API Status: Checking...</div>
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
        function formatBytes(bytes, decimals = 2) {
            if (bytes === 0) return '0 B';
            
            const k = 1024;
            const dm = decimals < 0 ? 0 : decimals;
            const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
            
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            
            return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
        }
        
        function normalizeAPIPath(path) {
            if (!path) {
                return '/';
            }
            return path.startsWith('/') ? path : '/' + path;
        }

        function encodeAPIPath(path) {
            const parts = path.split('/');
            for (let i = 0; i < parts.length; i++) {
                if (i === 0 || parts[i] === '') {
                    continue;
                }
                try {
                    parts[i] = decodeURIComponent(parts[i]);
                } catch (_) {}
                parts[i] = encodeURIComponent(parts[i]);
            }
            return parts.join('/');
        }

        function buildFilesUrl(path) {
            const normalized = encodeAPIPath(normalizeAPIPath(path || '/'));
            return new URL('/api/files' + normalized, window.location.origin).toString();
        }

        async function refreshStats() {
            try {
                const debugDiv = document.getElementById('debug-info');
                debugDiv.textContent = 'Fetching status...';
                
                // Get basic status
                const statusResponse = await fetch('/status');
                if (!statusResponse.ok) {
                    debugDiv.textContent = 'Status API failed: ' + statusResponse.status;
                    throw new Error('Status endpoint returned ' + statusResponse.status);
                }
                const status = await statusResponse.json();
                
                debugDiv.textContent = 'Fetching cluster stats...';
                // Get cluster stats for peer information
                const clusterResponse = await fetch('/api/cluster-stats');
                let clusterStats = {};
                if (clusterResponse.ok) {
                    clusterStats = await clusterResponse.json();
                } else {
                    debugDiv.textContent = 'Cluster stats failed: ' + clusterResponse.status;
                }
                
                // Update display with available data
                const peerCount = clusterStats.peer_list ? clusterStats.peer_list.length : 0;
                document.getElementById('peers').textContent = peerCount;
                
                const rf = status.replication_factor || 3;
                document.getElementById('replication_factor').textContent = rf;
                
                document.getElementById('tombstones').textContent = 0; // Not implemented yet
                
                // Use partition stats from status
                const partitionStats = status.partition_stats || {};
                document.getElementById('under_replicated').textContent = partitionStats.under_replicated || 0;
                document.getElementById('local_partitions').textContent = partitionStats.local_partitions || 0;
                document.getElementById('total_files').textContent = partitionStats.total_files || 0;
                document.getElementById('pending_sync').textContent = partitionStats.pending_sync || 0;
                
                // Calculate cluster-wide disk usage totals
                debugDiv.textContent = 'Calculating cluster totals...';
                const nodeId = status.node_id;
                let currentNodeData = null;
                let clusterTotalBytesStored = 0;
                let clusterTotalDiskSize = 0;
                let clusterTotalDiskFree = 0;
                let nodeCount = 0;
                
                if (clusterStats.peer_list) {
                    for (const peer of clusterStats.peer_list) {
                        if (peer.bytes_stored !== undefined) {
                            clusterTotalBytesStored += peer.bytes_stored;
                            nodeCount++;
                        }
                        if (peer.disk_size !== undefined) {
                            clusterTotalDiskSize += peer.disk_size;
                        }
                        if (peer.disk_free !== undefined) {
                            clusterTotalDiskFree += peer.disk_free;
                        }
                        
                        // Find our own node data
                        if (peer.node_id === nodeId) {
                            currentNodeData = peer;
                        }
                    }
                }
                
                // Display cluster totals
                document.getElementById('cluster_bytes_stored').textContent = formatBytes(clusterTotalBytesStored);
                document.getElementById('cluster_disk_free').textContent = formatBytes(clusterTotalDiskFree);
                
                if (clusterTotalDiskSize > 0) {
                    const clusterUsedBytes = clusterTotalDiskSize - clusterTotalDiskFree;
                    const clusterUsagePercent = Math.round((clusterUsedBytes / clusterTotalDiskSize) * 100);
                    document.getElementById('cluster_disk_usage').textContent = clusterUsagePercent + '%';
                } else {
                    document.getElementById('cluster_disk_usage').textContent = 'N/A';
                }
                
                // Display current node's disk usage in Node Information section
                if (currentNodeData && currentNodeData.bytes_stored !== undefined) {
                    document.getElementById('node_bytes_stored').textContent = formatBytes(currentNodeData.bytes_stored);
                    
                    if (currentNodeData.disk_size && currentNodeData.disk_size > 0) {
                        const nodeUsedBytes = currentNodeData.disk_size - currentNodeData.disk_free;
                        const nodeUsagePercent = Math.round((nodeUsedBytes / currentNodeData.disk_size) * 100);
                        document.getElementById('node_disk_usage').textContent = nodeUsagePercent + '% of ' + formatBytes(currentNodeData.disk_size);
                        document.getElementById('node_disk_free').textContent = formatBytes(currentNodeData.disk_free);
                    } else {
                        document.getElementById('node_disk_usage').textContent = 'N/A';
                        document.getElementById('node_disk_free').textContent = 'N/A';
                    }
                } else {
                    // Try to get disk info from a direct CRDT query
                    try {
                        const crdtResponse = await fetch('/api/crdt/get?key=nodes/' + nodeId);
                        if (crdtResponse.ok) {
                            const crdtData = await crdtResponse.json();
                            if (crdtData.value) {
                                const nodeInfo = JSON.parse(atob(crdtData.value));
                                if (nodeInfo.bytes_stored !== undefined) {
                                    document.getElementById('node_bytes_stored').textContent = formatBytes(nodeInfo.bytes_stored);
                                    
                                    if (nodeInfo.disk_size && nodeInfo.disk_size > 0) {
                                        const nodeUsedBytes = nodeInfo.disk_size - nodeInfo.disk_free;
                                        const nodeUsagePercent = Math.round((nodeUsedBytes / nodeInfo.disk_size) * 100);
                                        document.getElementById('node_disk_usage').textContent = nodeUsagePercent + '% of ' + formatBytes(nodeInfo.disk_size);
                                        document.getElementById('node_disk_free').textContent = formatBytes(nodeInfo.disk_free);
                                    } else {
                                        document.getElementById('node_disk_usage').textContent = 'N/A';
                                        document.getElementById('node_disk_free').textContent = 'N/A';
                                    }
                                } else {
                                    document.getElementById('node_bytes_stored').textContent = 'N/A';
                                    document.getElementById('node_disk_usage').textContent = 'N/A';
                                    document.getElementById('node_disk_free').textContent = 'N/A';
                                }
                            }
                        } else {
                            document.getElementById('node_bytes_stored').textContent = 'N/A';
                            document.getElementById('node_disk_usage').textContent = 'N/A';
                            document.getElementById('node_disk_free').textContent = 'N/A';
                        }
                    } catch (e) {
                        document.getElementById('node_bytes_stored').textContent = 'ERR';
                        document.getElementById('node_disk_usage').textContent = 'ERR';
                        document.getElementById('node_disk_free').textContent = 'ERR';
                    }
                }
                
                // Update input fields with current values
                document.getElementById('rfInput').value = rf;
                
                debugDiv.textContent = 'API OK - Last update: ' + new Date().toLocaleTimeString();
                
            } catch (error) {
                const debugDiv = document.getElementById('debug-info');
                debugDiv.textContent = 'API Error: ' + error.message;
                
                // Show error state instead of leaving dashes
                document.getElementById('peers').textContent = 'ERR';
                document.getElementById('replication_factor').textContent = 'ERR';
                document.getElementById('tombstones').textContent = 'ERR';
                document.getElementById('under_replicated').textContent = 'ERR';
                document.getElementById('local_partitions').textContent = 'ERR';
                document.getElementById('total_files').textContent = 'ERR';
                document.getElementById('pending_sync').textContent = 'ERR';
                document.getElementById('cluster_bytes_stored').textContent = 'ERR';
                document.getElementById('cluster_disk_usage').textContent = 'ERR';
                document.getElementById('cluster_disk_free').textContent = 'ERR';
                document.getElementById('node_bytes_stored').textContent = 'ERR';
                document.getElementById('node_disk_usage').textContent = 'ERR';
                document.getElementById('node_disk_free').textContent = 'ERR';
            }
        }
        
       
        
        async function addTestData() {
            try {
                const testData = 'Test data created at ' + new Date().toLocaleString();
                
                
                const fileName = 'test-' + Date.now() + '.txt';
                const response = await fetch(buildFilesUrl(fileName), {
                    method: 'PUT',
                    headers: { 'Content-Type': 'text/plain' },
                    body: testData
                });
                
                if (response.ok) {
                    alert('✅ Added test file: ' + fileName);
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
        setInterval(() => {
            refreshStats().catch(error => {
                console.error('Auto-refresh failed:', error);
            });
        }, 30000);
    </script>
</body>
</html>`

	w.Write([]byte(html))
}
