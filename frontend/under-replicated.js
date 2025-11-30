const state = { report: null };

function formatBytes(bytes) {
    if (bytes === 0 || bytes === undefined || bytes === null) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(1) + " " + units[i];
}

function formatDate(value) {
    if (!value) return "-";
    const dt = new Date(value);
    if (Number.isNaN(dt.getTime())) {
        return value;
    }
    return dt.toLocaleString();
}

function encodeAPIPath(path) {
    if (!path) return "/";
    const parts = path.split("/").map((part, idx) => {
        if (idx === 0 && part === "") return "";
        try {
            part = decodeURIComponent(part);
        } catch (_) {}
        return encodeURIComponent(part);
    });
    return parts.join("/") || "/";
}

async function loadReport() {
    const statusEl = document.getElementById("statusText");
    statusEl.textContent = "Loading...";

    try {
        const response = await fetch("/api/under-replicated");
        if (!response.ok) {
            throw new Error("API returned " + response.status);
        }
        const data = await response.json();
        state.report = data;
        renderSummary(data);
        renderPartitions(data);
        statusEl.textContent = "Updated " + new Date().toLocaleTimeString();
    } catch (err) {
        statusEl.textContent = "Failed to load report: " + err.message;
        console.error(err);
    }
}

function renderSummary(report) {
    const partitions = report.partitions || [];
    const rf = report.replication_factor || 0;
    const fileCount = partitions.reduce((acc, p) => acc + (p.files ? p.files.length : 0), 0);
    const missingTotal = partitions.reduce((acc, p) => acc + (p.missing_replicas || 0), 0);

    document.getElementById("rfValue").textContent = rf || "-";
    document.getElementById("partitionCount").textContent = partitions.length;
    document.getElementById("fileCount").textContent = fileCount;
    document.getElementById("missingTotals").textContent = missingTotal;
}

function renderPartitions(report) {
    const list = document.getElementById("partitionList");
    list.innerHTML = "";
    const partitions = report.partitions || [];

    if (partitions.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = "No under-replicated partitions detected.";
        list.appendChild(empty);
        return;
    }

    partitions.forEach((partition) => {
        const card = document.createElement("div");
        card.className = "partition-card";

        const header = document.createElement("div");
        header.className = "partition-header";

        const title = document.createElement("div");
        title.className = "partition-title";
        title.textContent = `Partition ${partition.partition_id}`;

        const missing = document.createElement("span");
        missing.className = "badge danger";
        missing.textContent = `Missing ${partition.missing_replicas} replica(s)`;

        const holders = document.createElement("div");
        holders.className = "holder-list";
        const holderList = partition.holders && partition.holders.length
            ? partition.holders.join(", ")
            : "no holders recorded";
        holders.textContent = `Holders: ${holderList}`;

        header.appendChild(title);
        header.appendChild(missing);
        header.appendChild(holders);
        card.appendChild(header);

        const crdtDetails = renderPartitionCRDT(partition.partition_crdt);
        if (crdtDetails) {
            card.appendChild(crdtDetails);
        }

        if (partition.files_unavailable) {
            const unavailable = document.createElement("div");
            unavailable.className = "empty";
            unavailable.textContent = partition.unavailable_message || "Files for this partition are not available on this node.";
            card.appendChild(unavailable);
            list.appendChild(card);
            return;
        }

        const files = partition.files || [];
        if (files.length === 0) {
            const empty = document.createElement("div");
            empty.className = "empty";
            empty.textContent = "No files enumerated for this partition.";
            card.appendChild(empty);
            list.appendChild(card);
            return;
        }

        const table = document.createElement("table");
        table.className = "file-table";
        const thead = document.createElement("thead");
        thead.innerHTML = `<tr>
            <th>Path</th>
            <th>Size</th>
            <th>Modified</th>
            <th></th>
        </tr>`;
        table.appendChild(thead);

        const tbody = document.createElement("tbody");
        files.forEach((file) => {
            const row = document.createElement("tr");

            const pathCell = document.createElement("td");
            pathCell.className = "path";
            pathCell.textContent = file.path;

            const sizeCell = document.createElement("td");
            sizeCell.textContent = formatBytes(file.size);

            const modCell = document.createElement("td");
            modCell.textContent = formatDate(file.modified_at);

            const actionCell = document.createElement("td");
            const btn = document.createElement("button");
            btn.className = "btn small";
            btn.textContent = "Metadata";
            btn.onclick = () => loadMetadata(file.path);
            actionCell.appendChild(btn);

            row.appendChild(pathCell);
            row.appendChild(sizeCell);
            row.appendChild(modCell);
            row.appendChild(actionCell);
            tbody.appendChild(row);
        });
        table.appendChild(tbody);
        card.appendChild(table);

        list.appendChild(card);
    });
}

function renderPartitionCRDT(crdt) {
    if (!crdt) return null;

    const wrapper = document.createElement("div");
    wrapper.className = "crdt-block";

    const title = document.createElement("div");
    title.className = "crdt-title";
    title.textContent = "CRDT partition details";

    const meta = document.createElement("div");
    meta.className = "crdt-meta";
    meta.textContent = `File count: ${crdt.file_count || 0}`;

    const holderTable = document.createElement("table");
    holderTable.className = "holder-table";
    const head = document.createElement("thead");
    head.innerHTML = `<tr><th>Holder</th><th>File count</th><th>Checksum</th></tr>`;
    holderTable.appendChild(head);

    const body = document.createElement("tbody");
    const holderData = crdt.holder_data || {};
    const holderKeys = Object.keys(holderData);
    if (holderKeys.length === 0) {
        const row = document.createElement("tr");
        const cell = document.createElement("td");
        cell.colSpan = 3;
        cell.textContent = "No holder data recorded";
        row.appendChild(cell);
        body.appendChild(row);
    } else {
        holderKeys.sort().forEach((nodeId) => {
            const data = holderData[nodeId] || {};
            const row = document.createElement("tr");
            row.innerHTML = `<td>${nodeId}</td><td>${data.file_count ?? "-"}</td><td>${data.checksum ?? "-"}</td>`;
            body.appendChild(row);
        });
    }
    holderTable.appendChild(body);

    if (crdt.metadata && Object.keys(crdt.metadata).length > 0) {
        const metaTitle = document.createElement("div");
        metaTitle.className = "crdt-title";
        metaTitle.textContent = "Metadata";
        wrapper.appendChild(metaTitle);

        const metaTable = document.createElement("table");
        metaTable.className = "holder-table";
        const metaHead = document.createElement("thead");
        metaHead.innerHTML = `<tr><th>Key</th><th>Value</th></tr>`;
        metaTable.appendChild(metaHead);
        const metaBody = document.createElement("tbody");
        Object.keys(crdt.metadata).sort().forEach((key) => {
            const row = document.createElement("tr");
            let valText;
            try {
                valText = JSON.stringify(JSON.parse(crdt.metadata[key]), null, 0);
            } catch (_) {
                valText = String(crdt.metadata[key]);
            }
            row.innerHTML = `<td>${key}</td><td>${valText}</td>`;
            metaBody.appendChild(row);
        });
        metaTable.appendChild(metaBody);
        wrapper.appendChild(metaTable);
    }

    const raw = document.createElement("pre");
    raw.className = "crdt-json";
    raw.textContent = JSON.stringify(crdt, null, 2);

    wrapper.appendChild(title);
    wrapper.appendChild(meta);
    wrapper.appendChild(holderTable);
    wrapper.appendChild(raw);
    return wrapper;
}

async function loadMetadata(path) {
    const drawer = document.getElementById("metadataDrawer");
    const body = document.getElementById("metadataBody");
    const title = document.getElementById("metadataTitle");
    const pathLabel = document.getElementById("metadataPath");

    title.textContent = "Metadata";
    pathLabel.textContent = path;
    body.textContent = "Loading...";
    drawer.classList.add("open");

    try {
        const encoded = encodeAPIPath(path);
        const resp = await fetch("/api/metadata" + encoded);
        if (!resp.ok) {
            throw new Error("API returned " + resp.status);
        }
        const data = await resp.json();
        title.textContent = "Metadata";
        body.textContent = JSON.stringify(data, null, 2);
    } catch (err) {
        body.textContent = "Failed to load metadata: " + err.message;
    }
}

function closeMetadata() {
    document.getElementById("metadataDrawer").classList.remove("open");
}

window.loadReport = loadReport;
window.closeMetadata = closeMetadata;

document.addEventListener("DOMContentLoaded", loadReport);
