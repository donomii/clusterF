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
        
        // Get sleep mode state
        const sleepResponse = await fetch('/api/crdt/get?key=cluster/sleep_mode');
        let isSleepMode = false;
        if (sleepResponse.ok) {
            const sleepData = await sleepResponse.json();
            if (sleepData.value_json !== undefined && sleepData.value_json !== null) {
                isSleepMode = sleepData.value_json;
            }
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

        // Fetch partition sync interval
        let partitionSyncInterval = null;
        try {
            const intervalResponse = await fetch('/api/partition-sync-interval');
            if (intervalResponse.ok) {
                const intervalData = await intervalResponse.json();
                partitionSyncInterval = intervalData.partition_sync_interval_seconds;
            }
        } catch (e) {
            // ignore, handled below
        }

        if (typeof partitionSyncInterval === 'number') {
            document.getElementById('partition_sync_interval').textContent = partitionSyncInterval;
            const intervalInput = document.getElementById('partitionSyncIntervalInput');
            if (!intervalInput.value) {
                intervalInput.value = partitionSyncInterval;
            }
        } else {
            document.getElementById('partition_sync_interval').textContent = 'WAIT';
        }
        
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
        
        // Update sleep mode display
        updateSleepModeDisplay(isSleepMode);
        
        // Update all nodes information
        updateAllNodesInfo(clusterStats);
        
        debugDiv.textContent = 'API OK - Last update: ' + new Date().toLocaleTimeString();
        
    } catch (error) {
        const debugDiv = document.getElementById('debug-info');
        debugDiv.textContent = 'API Error: ' + error.message;
        
        document.getElementById('peers').textContent = 'WAIT';
        document.getElementById('current_file_name').textContent = 'WAIT';
        document.getElementById('current_file_path').textContent = 'WAIT';
        document.getElementById('replication_factor').textContent = 'WAIT';
        document.getElementById('tombstones').textContent = 'WAIT';
        document.getElementById('under_replicated').textContent = 'WAIT';
        document.getElementById('local_partitions').textContent = 'WAIT';
        document.getElementById('total_files').textContent = 'WAIT';
        document.getElementById('pending_sync').textContent = 'WAIT';
        document.getElementById('cluster_bytes_stored').textContent = 'WAIT';
        document.getElementById('cluster_disk_usage').textContent = 'WAIT';
        document.getElementById('cluster_disk_free').textContent = 'WAIT';
        document.getElementById('node_bytes_stored').textContent = 'WAIT';
        document.getElementById('node_disk_usage').textContent = 'WAIT';
        document.getElementById('node_disk_free').textContent = 'WAIT';
        
        // Update all nodes info with error
        document.getElementById('all-nodes-info').innerHTML = '<div style="color: #94a3b8;">Waiting for cluster data...</div>';
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

async function setPartitionSyncInterval() {
    try {
        const intervalInput = document.getElementById('partitionSyncIntervalInput');
        const newInterval = parseInt(intervalInput.value, 10);

        if (isNaN(newInterval) || newInterval < 1) {
            alert('❌ Partition sync interval must be a number >= 1 second');
            return;
        }

        const response = await fetch('/api/partition-sync-interval', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ partition_sync_interval_seconds: newInterval })
        });

        if (response.ok) {
            const result = await response.json();
            alert('✅ Partition sync interval set to ' + result.partition_sync_interval_seconds + ' seconds');
            refreshStats();
        } else {
            const error = await response.text();
            alert('❌ Failed to set partition sync interval: ' + error);
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

function updateSleepModeDisplay(isSleepMode) {
    const statusSpan = document.getElementById('sleep_mode_status');
    const checkbox = document.getElementById('sleepModeCheckbox');
    
    if (isSleepMode) {
        statusSpan.textContent = 'ENABLED';
        statusSpan.style.color = '#f59e0b';
        checkbox.checked = true;
    } else {
        statusSpan.textContent = 'DISABLED';
        statusSpan.style.color = '#22c55e';
        checkbox.checked = false;
    }
}

async function toggleSleepMode() {
    try {
        const checkbox = document.getElementById('sleepModeCheckbox');
        const newSleepMode = checkbox.checked;
        
        const response = await fetch('/api/crdt/get?key=cluster/sleep_mode');
        let currentDataPoint = null;
        if (response.ok) {
            currentDataPoint = await response.json();
        }
        
        const putResponse = await fetch('/frogpond/update', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify([{
                key: 'cluster/sleep_mode',
                value: JSON.stringify(newSleepMode),
                name: 'cluster/sleep_mode',
                updated: new Date().toISOString(),
                deleted: false
            }])
        });
        
        if (putResponse.ok) {
            updateSleepModeDisplay(newSleepMode);
            const action = newSleepMode ? 'enabled' : 'disabled';
            alert('✅ Sleep mode ' + action);
        } else {
            const error = await putResponse.text();
            alert('❌ Failed to toggle sleep mode: ' + error);
            checkbox.checked = !newSleepMode;
        }
    } catch (error) {
        alert('Error: ' + error.message);
        const checkbox = document.getElementById('sleepModeCheckbox');
        checkbox.checked = !checkbox.checked;
    }
}

async function clusterRestart() {
    console.log('[DEBUG] clusterRestart function called');
    if (!confirm('⚠️ This will restart ALL nodes in the cluster. Are you sure?')) {
        console.log('[DEBUG] User cancelled restart');
        return;
    }
    
    try {
        console.log('[DEBUG] Calling /api/cluster-restart endpoint');
        
        const response = await fetch('/api/cluster-restart', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        
        console.log('[DEBUG] Response status:', response.status);
        console.log('[DEBUG] Response ok:', response.ok);
        
        if (response.ok) {
            const responseData = await response.json();
            console.log('[DEBUG] Response data:', responseData);
            alert('✅ Cluster restart initiated. All nodes will restart shortly.');
        } else {
            const error = await response.text();
            console.log('[DEBUG] Error response:', error);
            alert('❌ Failed to initiate cluster restart: ' + error);
        }
    } catch (error) {
        console.log('[DEBUG] Exception caught:', error);
        alert('Error: ' + error.message);
    }
}

function updateAllNodesInfo(clusterStats) {
    const allNodesDiv = document.getElementById('all-nodes-info');
    
    if (!clusterStats || !clusterStats.peer_list || clusterStats.peer_list.length === 0) {
        allNodesDiv.innerHTML = '<div style="color: #94a3b8;">No other nodes discovered in cluster</div>';
        return;
    }
    
    // Sort nodes by node_id for consistent display
    const sortedNodes = [...clusterStats.peer_list].sort((a, b) => a.node_id.localeCompare(b.node_id));
    
    let html = '';
    sortedNodes.forEach(node => {
        const isCurrentNode = node.node_id === clusterStats.node_id;
        const nodeClass = isCurrentNode ? 'current-node' : 'remote-node';
        const nodeIndicator = isCurrentNode ? ' (current)' : '';
        const availableStatus = node.available ? 'Available' : 'Unavailable';
        const availableColor = node.available ? '#22c55e' : '#ef4444';
        const storageType = node.is_storage ? 'Storage Node' : 'Client Node';
        
        // Calculate disk usage percentage
        let diskUsagePercent = 'N/A';
        if (node.disk_size && node.disk_size > 0) {
            const used = node.disk_size - node.disk_free;
            diskUsagePercent = Math.round((used / node.disk_size) * 100) + '%';
        }
        
        // Format last seen time
        let lastSeenText = 'Never';
        if (node.last_seen) {
            const lastSeen = new Date(node.last_seen);
            const now = new Date();
            const diffSeconds = Math.floor((now - lastSeen) / 1000);
            if (diffSeconds < 60) {
                lastSeenText = `${diffSeconds}s ago`;
            } else if (diffSeconds < 3600) {
                lastSeenText = `${Math.floor(diffSeconds / 60)}m ago`;
            } else {
                lastSeenText = lastSeen.toLocaleString();
            }
        }
        
        html += `
            <div class="${nodeClass}" style="margin: 10px 0; padding: 10px; border: 1px solid rgba(59, 130, 246, 0.3); border-radius: 6px; background: rgba(59, 130, 246, 0.05);">
                <div style="font-weight: bold; color: #06b6d4; margin-bottom: 5px;">
                    ${node.node_id}${nodeIndicator}
                    <span style="margin-left: 10px; color: ${availableColor}; font-size: 12px;">${availableStatus}</span>
                    <span style="margin-left: 10px; color: #94a3b8; font-size: 12px;">${storageType}</span>
                </div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; font-size: 12px; color: #94a3b8;">
                    <div>
                        <div><strong>Address:</strong> ${node.address || 'Unknown'}:${node.http_port || 'N/A'}</div>
                        <div><strong>Discovery Port:</strong> ${node.discovery_port || 'N/A'}</div>
                        <div><strong>Storage Format:</strong> ${node.storage_format || 'N/A'}</div>
                        <div><strong>Storage Minor:</strong> ${node.storage_minor || 'N/A'}</div>
                        <div><strong>Program:</strong> ${node.program || 'N/A'}</div>
                        <div><strong>Version:</strong> ${node.version || 'N/A'}</div>
                    </div>
                    <div>
                        <div><strong>Bytes Stored:</strong> ${formatBytes(node.bytes_stored || 0)}</div>
                        <div><strong>Disk Size:</strong> ${formatBytes(node.disk_size || 0)}</div>
                        <div><strong>Disk Free:</strong> ${formatBytes(node.disk_free || 0)}</div>
                        <div><strong>Disk Usage:</strong> ${diskUsagePercent}</div>
                        <div><strong>Last Seen:</strong> ${lastSeenText}</div>
                        <div><strong>Export Dir:</strong> ${node.export_dir || 'None'}</div>
                        <div><strong>Cluster Dir:</strong> ${node.cluster_dir || 'None'}</div>
                        <div><strong>Import Dir:</strong> ${node.import_dir || 'None'}</div>
                        <div><strong>Debug Mode:</strong> ${node.debug ? 'Enabled' : 'Disabled'}</div>
                    </div>
                </div>
                <div style="margin-top: 8px; border-top: 1px solid rgba(59, 130, 246, 0.2); padding-top: 8px;">
                    <div><strong>Data Directory:</strong></div>
                    <div style="font-family: monospace; word-break: break-all; margin-left: 20px; color: #06b6d4;">${node.data_dir || 'N/A'}</div>
                </div>
                ${node.url ? `<div style="margin-top: 5px;"><strong>URL:</strong> <a href="${node.url}" target="_blank" style="color: #06b6d4;">${node.url}</a></div>` : ''}
            </div>
        `;
    });
    
    allNodesDiv.innerHTML = html;
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

async function debugRestartTask() {
    console.log('[DEBUG] debugRestartTask called');
    try {
        // Check what's in the tasks/restart key
        const response = await fetch('/api/crdt/get?key=tasks/restart');
        console.log('[DEBUG] CRDT GET response status:', response.status);
        
        if (response.ok) {
            const data = await response.json();
            console.log('[DEBUG] CRDT GET response data:', JSON.stringify(data, null, 2));
            alert('tasks/restart content:\n' + JSON.stringify(data, null, 2));
        } else {
            const error = await response.text();
            console.log('[DEBUG] CRDT GET error:', error);
            alert('Error getting tasks/restart: ' + error);
        }
        
        // Also check what's in the tasks/ prefix
        const listResponse = await fetch('/api/crdt/list?prefix=tasks/');
        console.log('[DEBUG] CRDT LIST response status:', listResponse.status);
        
        if (listResponse.ok) {
            const listData = await listResponse.json();
            console.log('[DEBUG] CRDT LIST response data:', JSON.stringify(listData, null, 2));
            alert('tasks/ prefix content:\n' + JSON.stringify(listData, null, 2));
        } else {
            const listError = await listResponse.text();
            console.log('[DEBUG] CRDT LIST error:', listError);
        }
    } catch (error) {
        console.log('[DEBUG] Exception in debugRestartTask:', error);
        alert('Error: ' + error.message);
    }
}

refreshStats();
setInterval(() => {
    refreshStats().catch(error => {
        console.error('Auto-refresh failed:', error);
    });
}, 1000);
