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
            console.log('Failed to start profiling');
        }
    } catch (error) {
        console.log('Error: ' + error.message);
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
            console.log('Failed to stop profiling');
        }
    } catch (error) {
        console.log('Error: ' + error.message);
    }
}

updateStatus();
setInterval(updateStatus, 5000);
