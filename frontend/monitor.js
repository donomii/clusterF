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
        
        const statusResponse = await fetch('/status');
        if (!statusResponse.ok) {
            debugDiv.textContent = 'Status API failed: ' + statusResponse.status;
            throw new Error('Status endpoint returned ' + statusResponse.status);
        }
        const status = await statusResponse.json();
        
        // Get partition sync pause state
        const pauseResponse = await fetch('/api/partition-sync-pause');
        let isPaused = false;
        if (pauseResponse.ok) {
            const pauseData = await pauseResponse.json();
            isPaused = pauseData.paused || false;
        }
        
        debugDiv.textContent = 'Fetching cluster stats...';
        const clusterResponse = await fetch('/api/cluster-stats');
        let clusterStats = {};
        if (clusterResponse.ok) {
            clusterStats = await clusterResponse.json();
        } else {
            debugDiv.textContent = 'Cluster stats failed: ' + clusterResponse.status;
        }
        
        document.getElementById('nodeId').textContent = status.node_id;
        document.getElementById('nodeIdInfo').textContent = status.node_id;
        document.getElementById('httpPort').textContent = status.http_port;
        document.getElementById('httpPortInfo').textContent = status.http_port;
        document.getElementById('discoveryPort').textContent = clusterStats.discovery_port || '-';
        document.getElementById('dataDir').textContent = status.data_dir;
        
        const peerCount = clusterStats.peer_list ? clusterStats.peer_list.length : 0;
        document.getElementById('peers').textContent = peerCount;
        
        const currentFile = status.current_file || '';
        let fileName = '-';
        if (currentFile) {
            const parts = currentFile.split('/');
            fileName = parts[parts.length - 1] || '-';
        }
        document.getElementById('current_file_name').textContent = fileName;
        document.getElementById('current_file_path').textContent = currentFile || '-';
        
        const rf = status.replication_factor || 3;
        document.getElementById('replication_factor').textContent = rf;
        
        document.getElementById('tombstones').textContent = 0;
        
        const partitionStats = status.partition_stats || {};
        document.getElementById('under_replicated').textContent = partitionStats.under_replicated || 0;
        document.getElementById('local_partitions').textContent = partitionStats.local_partitions || 0;
        document.getElementById('total_files').textContent = partitionStats.total_files || 0;
        document.getElementById('pending_sync').textContent = partitionStats.pending_sync || 0;
        
        const nodeId = status.node_id;
        let currentNodeData = null;
        let clusterTotalBytesStored = 0;
        let clusterTotalDiskSize = 0;
        let clusterTotalDiskFree = 0;
        
        if (clusterStats.peer_list) {
            for (const peer of clusterStats.peer_list) {
                if (peer.is_storage !== false) {
                    if (peer.bytes_stored !== undefined) {
                        clusterTotalBytesStored += peer.bytes_stored;
                    }
                    if (peer.disk_size !== undefined) {
                        clusterTotalDiskSize += peer.disk_size;
                    }
                    if (peer.disk_free !== undefined) {
                        clusterTotalDiskFree += peer.disk_free;
                    }
                }
                
                if (peer.node_id === nodeId) {
                    currentNodeData = peer;
                }
            }
        }
        
        document.getElementById('cluster_bytes_stored').textContent = formatBytes(clusterTotalBytesStored);
        document.getElementById('cluster_disk_free').textContent = formatBytes(clusterTotalDiskFree);
        
        if (clusterTotalDiskSize > 0) {
            const clusterUsedBytes = clusterTotalDiskSize - clusterTotalDiskFree;
            const clusterUsagePercent = Math.round((clusterUsedBytes / clusterTotalDiskSize) * 100);
            document.getElementById('cluster_disk_usage').textContent = clusterUsagePercent + '%';
        } else {
            document.getElementById('cluster_disk_usage').textContent = 'N/A';
        }
        
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
            document.getElementById('node_bytes_stored').textContent = 'N/A';
            document.getElementById('node_disk_usage').textContent = 'N/A';
            document.getElementById('node_disk_free').textContent = 'N/A';
        }
        
        const rfInput = document.getElementById('rfInput');
        if (!rfInput.value) {
            rfInput.value = rf;
        }
        
        // Update sync status display
        updateSyncStatusDisplay(isPaused);
        
        debugDiv.textContent = 'API OK - Last update: ' + new Date().toLocaleTimeString();
        
    } catch (error) {
        const debugDiv = document.getElementById('debug-info');
        debugDiv.textContent = 'API Error: ' + error.message;
        
        document.getElementById('peers').textContent = 'ERR';
        document.getElementById('current_file_name').textContent = 'ERR';
        document.getElementById('current_file_path').textContent = 'ERR';
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

function updateSyncStatusDisplay(isPaused) {
    const statusSpan = document.getElementById('sync_status');
    const pauseBtn = document.getElementById('pauseBtn');
    
    if (isPaused) {
        statusSpan.textContent = 'PAUSED';
        statusSpan.style.color = '#ef4444';
        pauseBtn.textContent = '▶️ Resume Sync';
    } else {
        statusSpan.textContent = 'RUNNING';
        statusSpan.style.color = '#22c55e';
        pauseBtn.textContent = '⏸️ Pause Sync';
    }
}

async function togglePartitionSyncPause() {
    try {
        // Get current state
        const response = await fetch('/api/partition-sync-pause');
        if (!response.ok) {
            alert('❌ Failed to get current pause state');
            return;
        }
        
        const currentState = await response.json();
        const newPausedState = !currentState.paused;
        
        // Set new state
        const putResponse = await fetch('/api/partition-sync-pause', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ paused: newPausedState })
        });
        
        if (putResponse.ok) {
            const result = await putResponse.json();
            updateSyncStatusDisplay(result.paused);
            const action = result.paused ? 'paused' : 'resumed';
            alert('✅ Partition sync ' + action);
        } else {
            const error = await putResponse.text();
            alert('❌ Failed to toggle partition sync: ' + error);
        }
    } catch (error) {
        alert('Error: ' + error.message);
    }
}

refreshStats();
setInterval(() => {
    refreshStats().catch(error => {
        console.error('Auto-refresh failed:', error);
    });
}, 1000);
