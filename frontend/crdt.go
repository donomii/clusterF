package frontend

import "net/http"

// HandleCRDTInspectorPageUI serves the tree-browsing UI for the CRDT store.
func (f *Frontend) HandleCRDTInspectorPageUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if b, err := loadAsset("crdt-inspector.html"); err == nil {
		w.Write(b)
		return
	}

	html := `<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>🐸 CRDT Inspector</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🐸</text></svg>">
    <style>
        :root { color-scheme: dark; }
        * { box-sizing: border-box; }
        body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0b1220; color: #e5e7eb; }
        .container { display: grid; grid-template-columns: 360px 1fr; gap: 16px; padding: 20px; height: 100vh; }
        .panel { background: rgba(15, 23, 42, 0.9); border: 1px solid rgba(59,130,246,0.25); border-radius: 12px; overflow: hidden; display:flex; flex-direction:column; }
        .panel h2 { margin: 0; padding: 14px 16px; border-bottom: 1px solid rgba(148,163,184,0.2); color: #93c5fd; font-size: 16px; }
        .tools { padding: 10px 12px; display: grid; grid-template-columns: 1fr auto auto auto auto; gap: 8px; border-bottom: 1px solid rgba(148,163,184,0.15); align-items:center; }
        .tools input { padding: 10px; border-radius: 8px; border: 1px solid rgba(148,163,184,0.3); background: rgba(2,6,23,0.6); color: #e5e7eb; }
        .btn { background: linear-gradient(45deg, #3b82f6, #8b5cf6); color: white; border: none; padding: 10px 14px; border-radius: 8px; cursor: pointer; font-weight: 600; }
        .tree { flex:1; overflow: auto; padding: 8px 10px; }
        .row { display: flex; align-items: center; gap: 8px; padding: 6px 8px; border-radius: 8px; cursor: pointer; }
        .row:hover { background: rgba(59,130,246,0.12); }
        .kind { width: 22px; text-align:center; }
        .path { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; color:#cbd5e1; }
        .crumbs { padding: 8px 12px; color: #94a3b8; font-size: 12px; border-bottom: 1px solid rgba(148,163,184,0.15); display:flex; gap:6px; align-items:center; }
        .value { flex:1; overflow:auto; padding: 12px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 12px; }
        .muted { color:#94a3b8; }
        .split { display:grid; grid-template-columns: 1fr; height:100%; }
        .hl { color:#93c5fd; }
        .footer { padding: 8px 12px; border-top: 1px solid rgba(148,163,184,0.15); display:flex; justify-content: space-between; align-items:center; }
        .link { color:#93c5fd; text-decoration:none; }
        .chip { display:inline-block; margin:4px 6px 0 0; padding:6px 10px; border-radius:999px; border:1px solid rgba(59,130,246,0.35); cursor:pointer; font-size:12px; }
        .chip:hover { background: rgba(59,130,246,0.1); }
    </style>
    <script>
        let curPrefix = '';
        let cursorAfter = '';
        let mode = 'list';
        const pageSize = 100;

        function setPrefix(p, push=true) {
            curPrefix = p || '';
            cursorAfter = '';
            document.getElementById('prefix').value = curPrefix;
            if (push) try { history.pushState({prefix: curPrefix}, '', '#' + encodeURIComponent(curPrefix)); } catch(e){}
            mode = 'list';
            loadList();
        }

        async function loadList(loadMore=false) {
            const params = new URLSearchParams();
            params.set('prefix', curPrefix);
            params.set('limit', String(pageSize));
            if (loadMore && cursorAfter) params.set('after', cursorAfter);
            const res = await fetch('/api/crdt/list?' + params.toString());
            if (!res.ok) { alert('Failed to load list'); return; }
            const data = await res.json();
            renderList(data, loadMore);
        }

        async function runSearch(loadMore=false) {
            const q = document.getElementById('search').value.trim();
            if (!q) { loadList(loadMore); return; }
            const params = new URLSearchParams();
            params.set('prefix', curPrefix);
            params.set('q', q);
            params.set('limit', String(pageSize));
            if (loadMore && cursorAfter) params.set('after', cursorAfter);
            const res = await fetch('/api/crdt/search?' + params.toString());
            if (!res.ok) { alert('Failed to search'); return; }
            const data = await res.json();
            renderSearch(data, loadMore);
        }

        function renderList(data, loadMore) {
            const tree = document.getElementById('tree');
            if (!loadMore) tree.innerHTML = '';
            // Crumbs
            const crumbs = document.getElementById('crumbs');
            crumbs.innerHTML = '';
            const parts = (data.prefix||'').split('/').filter(Boolean);
            let acc = '';
            const add = (name, full) => {
                const a = document.createElement('a');
                a.href = '#';
                a.textContent = name || '/';
                a.className = 'link';
                a.onclick = (e)=>{e.preventDefault(); setPrefix(full)};
                crumbs.appendChild(a);
                const s = document.createElement('span'); s.textContent = ' / '; crumbs.appendChild(s);
            };
            if (parts.length === 0) { add('/', ''); }
            for (let i=0;i<parts.length;i++) { acc = i===0 ? parts[0] : acc + '/' + parts[i]; add(parts[i], acc); }
            
            // Items
            for (const it of data.items||[]) {
                const row = document.createElement('div');
                row.className = 'row';
                const k = document.createElement('div'); k.className = 'kind'; k.textContent = it.type==='dir'?'📁':'🔑'; row.appendChild(k);
                const p = document.createElement('div'); p.className = 'path'; p.textContent = it.name; row.appendChild(p);
                row.onclick = ()=>{
                    if (it.type==='dir') setPrefix((curPrefix?curPrefix+'/':'') + it.name);
                    else loadKey((curPrefix?curPrefix+'/':'') + it.name);
                };
                tree.appendChild(row);
            }
            const more = document.getElementById('more');
            more.style.display = data.has_more ? '' : 'none';
            if (data.has_more) cursorAfter = data.next_after || '';
        }

        function renderSearch(data, loadMore) {
            const tree = document.getElementById('tree');
            if (!loadMore) tree.innerHTML = '';
            for (const it of data.items||[]) {
                const row = document.createElement('div');
                row.className = 'row';
                const k = document.createElement('div'); k.className = 'kind'; k.textContent = '🔎'; row.appendChild(k);
                const p = document.createElement('div'); p.className = 'path'; p.innerHTML = it.name.replace(/</g,'&lt;'); row.appendChild(p);
                row.onclick = ()=> loadKey(it.key);
                tree.appendChild(row);
            }
            const more = document.getElementById('more');
            more.style.display = data.has_more ? '' : 'none';
            if (data.has_more) cursorAfter = data.next_after || '';
        }

        async function loadKey(key) {
            const params = new URLSearchParams();
            params.set('key', key);
            const res = await fetch('/api/crdt/get?' + params.toString());
            if (!res.ok) { alert('Failed to load key'); return; }
            const data = await res.json();
            const v = document.getElementById('value');
            const pretty = data.value_json ? JSON.stringify(data.value_json, null, 2) : (data.value_text || '[binary]');
            v.textContent = pretty;
            document.getElementById('keytitle').textContent = key + (data.deleted ? ' (deleted)' : '');
        }

        function go() { setPrefix(document.getElementById('prefix').value.trim()); }
        function home() { window.location.href = '/'; }
        function up() {
            const p = (curPrefix||'').replace(/\/$/, '');
            const idx = p.lastIndexOf('/');
            const parent = idx > -1 ? p.slice(0, idx) : '';
            setPrefix(parent);
        }
        function back() { history.back(); }
        function searchNow() { mode = 'search'; cursorAfter = ''; runSearch(false); }
        function more() { mode === 'search' ? runSearch(true) : loadList(true); }

        window.addEventListener('popstate', (e) => {
            const p = (e.state && e.state.prefix) || decodeURIComponent((location.hash||'').slice(1));
            setPrefix(p, false);
        });
        document.addEventListener('DOMContentLoaded', () => {
            const init = decodeURIComponent((location.hash||'').slice(1));
            setPrefix(init, false);
        });
    </script>
    </head>
<body>
    <div class="container">
        <div class="panel">
            <h2>CRDT Browser</h2>
            <div class="tools">
                <input id="prefix" placeholder="Prefix, e.g. partitions">
                <button class="btn" onclick="go()">Go</button>
                <button class="btn" onclick="up()">Up</button>
                <button class="btn" onclick="back()">Back</button>
                <a class="btn" style="text-decoration:none; text-align:center;" href="/">Home</a>
                <input id="search" placeholder="Search keys (substring)" style="grid-column: 1 / -2;">
                <button class="btn" onclick="searchNow()">Search</button>
            </div>
            <div id="crumbs" class="crumbs"></div>
            <div id="tree" class="tree"></div>
            <div class="footer">
                <span class="muted">Page size: 100</span>
                <button id="more" class="btn" style="display:none" onclick="more()">Load more</button>
            </div>
        </div>
        <div class="panel">
            <h2 id="keytitle">Value</h2>
            <div class="split">
                <pre id="value" class="value">Select a key to inspect…</pre>
            </div>
        </div>
    </div>
</body>
</html>`
	w.Write([]byte(html))
}
