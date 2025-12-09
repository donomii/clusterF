const REFRESH_INTERVAL_MS = 5000;
let refreshTimer = null;

function formatNumber(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
        return '-';
    }
    if (typeof value === 'number') {
        if (Math.abs(value) >= 1000) {
            return value.toLocaleString();
        }
        if (Number.isInteger(value)) {
            return value.toString();
        }
        return value.toFixed(2);
    }
    return value;
}

function formatBytes(bytes) {
    if (typeof bytes !== 'number' || bytes <= 0) {
        return '-';
    }
    const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
    const value = bytes / Math.pow(1024, exponent);
    return `${value.toFixed(exponent === 0 ? 0 : 2)} ${units[exponent]}`;
}

function formatTimestamp(ts) {
    if (!ts) return 'unknown';
    const date = new Date(ts * 1000);
    return date.toLocaleString();
}

function renderCounters(container, counters) {
    const keys = Object.keys(counters || {}).sort();
    if (!keys.length) {
        const empty = document.createElement('div');
        empty.textContent = 'No counters reported.';
        empty.style.color = '#94a3b8';
        empty.style.fontSize = '13px';
        container.appendChild(empty);
        return;
    }

    const table = document.createElement('table');
    table.className = 'metric-table';
    const head = document.createElement('thead');
    head.innerHTML = '<tr><th>Counter</th><th>Value</th></tr>';
    table.appendChild(head);

    const body = document.createElement('tbody');
    for (const key of keys) {
        const row = document.createElement('tr');
        const nameCell = document.createElement('td');
        nameCell.textContent = key;
        const valueCell = document.createElement('td');
        valueCell.textContent = formatNumber(counters[key]);
        row.appendChild(nameCell);
        row.appendChild(valueCell);
        body.appendChild(row);
    }

    table.appendChild(body);
    container.appendChild(table);
}

function renderTimers(container, timers) {
    const keys = Object.keys(timers || {}).sort();
    if (!keys.length) {
        const empty = document.createElement('div');
        empty.textContent = 'No timers reported.';
        empty.style.color = '#94a3b8';
        empty.style.fontSize = '13px';
        container.appendChild(empty);
        return;
    }

    const table = document.createElement('table');
    table.className = 'metric-table';
    const head = document.createElement('thead');
    head.innerHTML = '<tr><th>Timer</th><th>Count</th><th>Avg (ms)</th><th>Min</th><th>Max</th></tr>';
    table.appendChild(head);

    const body = document.createElement('tbody');
    for (const key of keys) {
        const stats = timers[key] || {};
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${key}</td>
            <td>${formatNumber(stats.count)}</td>
            <td>${formatNumber(stats.avg_ms)}</td>
            <td>${formatNumber(stats.min_ms)}</td>
            <td>${formatNumber(stats.max_ms)}</td>
        `;
        body.appendChild(row);
    }

    table.appendChild(body);
    container.appendChild(table);
}

function renderRuntimeStats(container, runtime) {
    const stats = runtime || {};
    const fields = [
        { label: 'Goroutines', value: formatNumber(stats.goroutines) },
        { label: 'Mem Alloc', value: formatBytes((stats.mem_alloc_mb || 0) * 1024 * 1024) },
        { label: 'Mem Sys', value: formatBytes((stats.mem_sys_mb || 0) * 1024 * 1024) },
        { label: 'GC Runs', value: formatNumber(stats.num_gc) },
        { label: 'Next GC', value: formatBytes((stats.next_gc_mb || 0) * 1024 * 1024) },
    ];

    const grid = document.createElement('div');
    grid.className = 'runtime-stats';
    for (const field of fields) {
        const block = document.createElement('div');
        block.innerHTML = `<strong>${field.label}</strong><br>${field.value}`;
        grid.appendChild(block);
    }
    container.appendChild(grid);
}

function renderCircuitBreaker(container, breaker) {
    const state = breaker || {};
    const open = !!state.open;

    const section = document.createElement('div');
    const title = document.createElement('div');
    title.className = 'section-title';
    title.textContent = 'Circuit Breaker';
    section.appendChild(title);

    const pill = document.createElement('div');
    pill.className = `breaker-pill ${open ? 'open' : 'closed'}`;
    pill.textContent = open ? 'OPEN' : 'CLOSED';
    section.appendChild(pill);

    const details = document.createElement('div');
    details.className = 'breaker-details';
    if (open) {
        const reason = state.reason || 'breaker tripped';
        const setter = state.set_by || 'unknown';
        const target = state.target || 'unknown target';
        details.textContent = `Set by ${setter} while calling ${target}. Reason: ${reason}`;
    } else {
        details.textContent = 'Ready for outbound machine calls.';
    }
    section.appendChild(details);

    container.appendChild(section);
}

function renderConnectionStatus(container, connected) {
    const pill = document.createElement('div');
    pill.className = `status-pill ${connected ? 'connected' : 'disconnected'}`;
    pill.textContent = connected ? 'Connected' : 'Disconnected';
    container.appendChild(pill);
}

function renderBreakerBanner(openBreakers) {
    const banner = document.getElementById('breakerBanner');
    if (!banner) {
        return;
    }
    if (!openBreakers.length) {
        banner.style.display = 'none';
        banner.textContent = '';
        return;
    }

    const noun = openBreakers.length === 1 ? 'breaker' : 'breakers';
    const rows = openBreakers.map(info => {
        const reason = info.reason || 'no reason provided';
        const target = info.target || 'unknown target';
        return `<li>${info.node}: set by ${info.set_by || info.node} while calling ${target} — ${reason}</li>`;
    });

    banner.style.display = 'block';
    banner.innerHTML = `<strong>${openBreakers.length} circuit ${noun} open</strong><ul class="breaker-list">${rows.join('')}</ul>`;
}

function renderSnapshots(data) {
    const container = document.getElementById('metricsContainer');
    const emptyState = document.getElementById('emptyState');
    container.innerHTML = '';
    const openBreakers = [];

    if (!data.snapshots || !data.snapshots.length) {
        container.style.display = 'none';
        emptyState.style.display = 'block';
        renderBreakerBanner(openBreakers);
        return;
    }

    container.style.display = 'grid';
    emptyState.style.display = 'none';

    for (const snapshot of data.snapshots) {
        const card = document.createElement('div');
        card.className = 'node-card';

        const title = document.createElement('h2');
        title.textContent = snapshot.node_id || 'Unknown Node';
        card.appendChild(title);

        renderConnectionStatus(card, !!(snapshot.connection && snapshot.connection.connected));
        const ts = document.createElement('div');
        ts.className = 'timestamp';
        ts.textContent = `Last updated ${formatTimestamp(snapshot.timestamp)}`;
        card.appendChild(ts);

        if (snapshot.circuit_breaker && snapshot.circuit_breaker.open) {
            openBreakers.push({
                node: snapshot.node_id || 'unknown',
                set_by: snapshot.circuit_breaker.set_by,
                target: snapshot.circuit_breaker.target,
                reason: snapshot.circuit_breaker.reason,
            });
        }

        renderCircuitBreaker(card, snapshot.circuit_breaker);
        renderRuntimeStats(card, snapshot.runtime);

        const countersTitle = document.createElement('div');
        countersTitle.className = 'section-title';
        countersTitle.textContent = 'Counters';
        card.appendChild(countersTitle);
        renderCounters(card, snapshot.counters);

        const timersTitle = document.createElement('div');
        timersTitle.className = 'section-title';
        timersTitle.textContent = 'Timers';
        card.appendChild(timersTitle);
        renderTimers(card, snapshot.timers);

        container.appendChild(card);
    }

    renderBreakerBanner(openBreakers);
}

async function fetchMetrics() {
    const status = document.getElementById('status');
    try {
        status.textContent = 'Refreshing metrics...';
        const response = await fetch('/api/metrics');
        if (!response.ok) {
            throw new Error(`Server responded with ${response.status}`);
        }
        const payload = await response.json();
        renderSnapshots(payload);
        const updatedAt = payload.generated_at ? new Date(payload.generated_at * 1000).toLocaleTimeString() : new Date().toLocaleTimeString();
        status.textContent = `Updated ${updatedAt}`;
    } catch (err) {
        console.error('Failed to fetch metrics', err);
        status.textContent = `Failed to load metrics: ${err.message}`;
    }
}

function setupAutoRefresh() {
    if (refreshTimer) {
        clearInterval(refreshTimer);
    }
    refreshTimer = setInterval(fetchMetrics, REFRESH_INTERVAL_MS);
}

document.addEventListener('DOMContentLoaded', () => {
    const refreshButton = document.getElementById('refreshButton');
    if (refreshButton) {
        refreshButton.addEventListener('click', fetchMetrics);
    }
    fetchMetrics();
    setupAutoRefresh();
});
