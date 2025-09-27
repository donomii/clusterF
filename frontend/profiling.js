function testJS() {
    console.log('JavaScript test function called');
    alert('JavaScript is working!');
}

async function updateStatus() {
    try {
        console.log('Fetching profiling status...');
        const response = await fetch('/api/profiling');
        console.log('Status response:', response.status);
        const data = await response.json();
        console.log('Status data:', data);
        
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
        document.getElementById('profilingStatus').textContent = '❌ Error: ' + error.message;
    }
}

async function startProfiling() {
    console.log('Start profiling clicked');
    try {
        const response = await fetch('/api/profiling', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'start' })
        });
        
        console.log('Start response:', response.status);
        if (response.ok) {
            console.log('Profiling started successfully');
            updateStatus();
        } else {
            const errorText = await response.text();
            console.error('Start profiling failed:', errorText);
            alert('Failed to start profiling: ' + errorText);
        }
    } catch (error) {
        console.error('Start profiling error:', error);
        alert('Error starting profiling: ' + error.message);
    }
}

async function stopProfiling() {
    console.log('Stop profiling clicked');
    try {
        const response = await fetch('/api/profiling', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: 'stop' })
        });
        
        console.log('Stop response:', response.status);
        if (response.ok) {
            console.log('Profiling stopped successfully');
            updateStatus();
        } else {
            const errorText = await response.text();
            console.error('Stop profiling failed:', errorText);
            alert('Failed to stop profiling: ' + errorText);
        }
    } catch (error) {
        console.error('Stop profiling error:', error);
        alert('Error stopping profiling: ' + error.message);
    }
}

async function showRuntimeStats() {
    console.log('showRuntimeStats called');
    const statsDiv = document.getElementById('runtimeStats');
    const dataDiv = document.getElementById('runtimeData');
    
    try {
        dataDiv.innerHTML = 'Loading runtime stats...';
        statsDiv.style.display = 'block';
        
        const goroutineResp = await fetch('/debug/pprof/goroutine?debug=1');
        if (!goroutineResp.ok) {
            throw new Error('Goroutine fetch failed: ' + goroutineResp.status);
        }
        const goroutineText = await goroutineResp.text();
        const goroutineCount = (goroutineText.match(/goroutine/g) || []).length;
        
        console.log('Found', goroutineCount, 'goroutines');
        
        const heapResp = await fetch('/debug/pprof/heap?debug=1');
        if (!heapResp.ok) {
            throw new Error('Heap fetch failed: ' + heapResp.status);
        }
        const heapText = await heapResp.text();
        console.log('Got heap data, length:', heapText.length);
        
        const html = 
            '<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0;">' +
                '<div style="background: rgba(59, 130, 246, 0.1); padding: 15px; border-radius: 8px;">' +
                    '<h4 style="margin: 0 0 10px 0; color: #3b82f6;">Goroutines</h4>' +
                    '<div style="font-size: 24px; font-weight: bold;">' + goroutineCount + '</div>' +
                '</div>' +
                '<div style="background: rgba(16, 185, 129, 0.1); padding: 15px; border-radius: 8px;">' +
                    '<h4 style="margin: 0 0 10px 0; color: #10b981;">Heap Data</h4>' +
                    '<div style="font-size: 20px; font-weight: bold;">' + formatBytes(heapText.length) + '</div>' +
                '</div>' +
                '<div style="background: rgba(245, 158, 11, 0.1); padding: 15px; border-radius: 8px;">' +
                    '<h4 style="margin: 0 0 10px 0; color: #f59e0b;">Status</h4>' +
                    '<div style="font-size: 20px; font-weight: bold;">Active</div>' +
                '</div>' +
                '<div style="background: rgba(239, 68, 68, 0.1); padding: 15px; border-radius: 8px;">' +
                    '<h4 style="margin: 0 0 10px 0; color: #ef4444;">Updated</h4>' +
                    '<div style="font-size: 20px; font-weight: bold;">' + new Date().toLocaleTimeString() + '</div>' +
                '</div>' +
            '</div>' +
            '<div style="margin-top: 20px;">' +
                '<h4>Profile Links</h4>' +
                '<a href="/debug/pprof/goroutine?debug=2"  style="color: #06b6d4; margin-right: 20px;">📋 Detailed Goroutines</a>' +
                '<a href="/debug/pprof/heap?debug=2"  style="color: #06b6d4; margin-right: 20px;">🧠 Detailed Memory</a>' +
                '<a href="/debug/pprof/"  style="color: #06b6d4;">📊 All Profiles</a>' +
            '</div>';
        
        dataDiv.innerHTML = html;
        console.log('Runtime stats updated successfully');
        
    } catch (error) {
        console.error('Runtime stats error:', error);
        dataDiv.innerHTML = 
            '<div style="color: #ef4444; margin-bottom: 15px;">Error: ' + error.message + '</div>' +
            '<div style="color: #888; font-size: 14px;">Direct links:</div>' +
            '<div style="margin-top: 10px;">' +
                '<a href="/debug/pprof/goroutine?debug=1"  style="color: #06b6d4; margin-right: 15px;">🔄 Goroutines</a>' +
                '<a href="/debug/pprof/heap?debug=1"  style="color: #06b6d4; margin-right: 15px;">🧠 Memory</a>' +
                '<a href="/debug/pprof/"  style="color: #06b6d4;">📊 All Profiles</a>' +
            '</div>';
        statsDiv.style.display = 'block';
    }
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Update status on page load and every 5 seconds
updateStatus();
setInterval(updateStatus, 5000);
