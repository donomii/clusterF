package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// handleCRDTInspectorPage serves the tree-browsing UI for the CRDT store
func (c *Cluster) handleCRDTInspectorPageUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	// Try to read external html file if present
	const fname = "crdt-inspector.html"
	if b, err := os.ReadFile(fname); err == nil {
		w.Write(b)
		return
	}

	// Built-in minimal UI if file not present
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
                <span class="muted">Page size: ` + fmt.Sprintf("%d", 100) + `</span>
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

// handleCRDTList lists immediate children (dirs and keys) under a prefix, with pagination
func (c *Cluster) handleCRDTListAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	// Normalize: allow trailing slash in UI
	if prefix != "" && strings.HasSuffix(prefix, "/") {
		prefix = strings.TrimSuffix(prefix, "/")
	}
	after := r.URL.Query().Get("after")
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 2000 {
			limit = v
		}
	}

	// Default suggestions without scanning the entire store
	if prefix == "" {
		items := []map[string]string{
			{"type": "dir", "name": "cluster"},
			{"type": "dir", "name": "nodes"},
			{"type": "dir", "name": "partitions"},
		}
		// Apply after/limit over suggestions
		start := 0
		if after != "" {
			for i, it := range items {
				if it["name"] > after {
					start = i
					break
				}
			}
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		slice := items[start:end]
		resp := map[string]interface{}{
			"prefix":   prefix,
			"items":    slice,
			"has_more": end < len(items),
		}
		if end < len(items) {
			resp["next_after"] = items[end-1]["name"]
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Special grouping for very large namespaces: partitions
	if prefix == "partitions" || strings.HasPrefix(prefix, "partitions/") {
		// Compute the remainder after "partitions/" (may be empty)
		rem := strings.TrimPrefix(prefix, "partitions")
		if strings.HasPrefix(rem, "/") {
			rem = rem[1:]
		}

		// Helper to paginate a generated list of group names; returns the page slice
		paginate := func(names []string) (page []string, hasMore bool, next string) {
			sort.Strings(names)
			start := 0
			if after != "" {
				for i := range names {
					if names[i] > after {
						start = i
						break
					}
				}
			}
			end := start + limit
			if end > len(names) {
				end = len(names)
			}
			page = names[start:end]
			if end < len(names) {
				hasMore = true
				next = names[end-1]
			}
			return
		}

		// Levels: p + 5 digits total
		// rem len < 3 => show p00..p65 groups (1000 each)
		// rem len == 3 => show pxxx0..pxxx9 (100 each)
		// rem len == 4 => show pxxxx0..pxxxx9 (10 each)
		// rem len >= 5 => list actual keys under that fixed prefix
		if rem == "" || len(rem) < 3 {
			// p00..p65
			names := make([]string, 0, 66)
			for i := 0; i <= 65; i++ {
				names = append(names, fmt.Sprintf("p%02d", i))
			}
			page, hasMore, next := paginate(names)
			// Build items with live counts of actual partition keys under each group
			items := make([]map[string]interface{}, 0, len(page))
			for _, g := range page {
				cnt := c.countCRDTKeys("partitions/" + g)
				items = append(items, map[string]interface{}{"name": g, "type": "dir", "count": cnt})
			}
			resp := map[string]interface{}{"prefix": prefix, "items": items, "has_more": hasMore}
			if hasMore {
				resp["next_after"] = next
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}
		if len(rem) == 3 {
			// p000..p009
			base := rem
			if len(base) != 3 {
				http.Error(w, "bad prefix", http.StatusBadRequest)
				return
			}
			names := make([]string, 0, 10)
			for i := 0; i <= 9; i++ {
				names = append(names, fmt.Sprintf("%s%d", base, i))
			}
			page, hasMore, next := paginate(names)
			items := make([]map[string]interface{}, 0, len(page))
			for _, g := range page {
				cnt := c.countCRDTKeys("partitions/" + g)
				items = append(items, map[string]interface{}{"name": g, "type": "dir", "count": cnt})
			}
			resp := map[string]interface{}{"prefix": prefix, "items": items, "has_more": hasMore}
			if hasMore {
				resp["next_after"] = next
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}
		if len(rem) == 4 {
			// p0000..p0009
			base := rem
			names := make([]string, 0, 10)
			for i := 0; i <= 9; i++ {
				names = append(names, fmt.Sprintf("%s%d", base, i))
			}
			page, hasMore, next := paginate(names)
			items := make([]map[string]interface{}, 0, len(page))
			for _, g := range page {
				cnt := c.countCRDTKeys("partitions/" + g)
				items = append(items, map[string]interface{}{"name": g, "type": "dir", "count": cnt})
			}
			resp := map[string]interface{}{"prefix": prefix, "items": items, "has_more": hasMore}
			if hasMore {
				resp["next_after"] = next
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}
		// len(rem) >= 5: list actual keys with this fixed prefix (at most 10 expected)
		realPrefix := "partitions/" + rem
		dps := c.frogpond.GetAllMatchingPrefix(realPrefix)
		keySet := make(map[string]struct{})
		for _, dp := range dps {
			k := string(dp.Key)
			rest := strings.TrimPrefix(k, "partitions/")
			// Only immediate keys that start with rem and have no further '/'
			if !strings.HasPrefix(rest, rem) {
				continue
			}
			if strings.Contains(rest[len(rem):], "/") {
				continue
			}
			keySet[rest] = struct{}{}
		}
		names := make([]string, 0, len(keySet))
		for n := range keySet {
			names = append(names, n)
		}
		sort.Strings(names)
		start := 0
		if after != "" {
			for i := range names {
				if names[i] > after {
					start = i
					break
				}
			}
		}
		end := start + limit
		if end > len(names) {
			end = len(names)
		}
		out := make([]map[string]string, 0, end-start)
		for _, n := range names[start:end] {
			out = append(out, map[string]string{"name": n, "type": "key"})
		}
		resp := map[string]interface{}{"prefix": prefix, "items": out, "has_more": end < len(names)}
		if end < len(names) {
			resp["next_after"] = names[end-1]
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Generic listing: fetch all under prefix and compute immediate children
	dps := c.frogpond.GetAllMatchingPrefix(prefix)

	dirSet := make(map[string]struct{})
	keySet := make(map[string]struct{})
	for _, dp := range dps {
		key := string(dp.Key)
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		// Ensure we trim at directory boundary
		rest := strings.TrimPrefix(key, prefix)
		if strings.HasPrefix(rest, "/") {
			rest = rest[1:]
		}
		if rest == "" { // exact key equals prefix; skip in listing
			continue
		}
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			seg := rest[:i]
			if seg != "" {
				dirSet[seg] = struct{}{}
			}
		} else {
			keySet[rest] = struct{}{}
		}
	}
	items := make([]struct{ Name, Type string }, 0, len(dirSet)+len(keySet))
	for name := range dirSet {
		items = append(items, struct{ Name, Type string }{name, "dir"})
	}
	for name := range keySet {
		items = append(items, struct{ Name, Type string }{name, "key"})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Name == items[j].Name {
			return items[i].Type < items[j].Type
		}
		return items[i].Name < items[j].Name
	})
	// Apply pagination
	start := 0
	if after != "" {
		for i := range items {
			if items[i].Name > after {
				start = i
				break
			}
		}
	}
	end := start + limit
	if end > len(items) {
		end = len(items)
	}
	page := items[start:end]
	out := make([]map[string]string, 0, len(page))
	for _, it := range page {
		out = append(out, map[string]string{"name": it.Name, "type": it.Type})
	}

	resp := map[string]interface{}{
		"prefix":   prefix,
		"items":    out,
		"has_more": end < len(items),
	}
	if end < len(items) {
		resp["next_after"] = items[end-1].Name
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleCRDTGet returns the value for a specific CRDT key in multiple representations
func (c *Cluster) handleCRDTGetAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	dp := c.frogpond.GetDataPoint(key)
	val := dp.Value
	var parsed interface{}
	_ = json.Unmarshal(val, &parsed)
	// Attempt UTF-8 text rendering; if not printable, omit
	var textStr string
	if utf8.Valid(val) {
		textStr = string(val)
	}
	// Build response
	resp := map[string]interface{}{
		"key":          key,
		"deleted":      dp.Deleted,
		"size":         len(val),
		"value_base64": base64.StdEncoding.EncodeToString(val),
	}
	if parsed != nil {
		resp["value_json"] = parsed
	}
	if textStr != "" {
		resp["value_text"] = textStr
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// countCRDTKeys counts non-deleted keys under a given prefix in the CRDT store.
func (c *Cluster) countCRDTKeys(prefix string) int {
	dps := c.frogpond.GetAllMatchingPrefix(prefix)
	n := 0
	for _, dp := range dps {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}
		n++
	}
	return n
}

// handleCRDTSearch searches for keys containing a substring within a prefix.
// This is on-demand and may scan the prefix; use with reasonable prefixes.
func (c *Cluster) handleCRDTSearchAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	if prefix != "" && strings.HasSuffix(prefix, "/") {
		prefix = strings.TrimSuffix(prefix, "/")
	}
	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "missing q", http.StatusBadRequest)
		return
	}
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 2000 {
			limit = v
		}
	}
	after := r.URL.Query().Get("after") // full key cursor

	// Normalize prefix for trimming rest
	trimPrefix := prefix
	if trimPrefix != "" && !strings.HasSuffix(trimPrefix, "/") {
		trimPrefix += "/"
	}

	dps := c.frogpond.GetAllMatchingPrefix(prefix)
	keys := make([]string, 0, len(dps))
	for _, dp := range dps {
		k := string(dp.Key)
		if strings.Contains(k, q) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	start := 0
	if after != "" {
		for i := range keys {
			if keys[i] > after {
				start = i
				break
			}
		}
	}
	end := start + limit
	if end > len(keys) {
		end = len(keys)
	}
	items := make([]map[string]string, 0, end-start)
	for _, k := range keys[start:end] {
		name := k
		if trimPrefix != "" && strings.HasPrefix(k, trimPrefix) {
			name = strings.TrimPrefix(k, trimPrefix)
		}
		items = append(items, map[string]string{
			"type": "key",
			"name": name,
			"key":  k,
		})
	}
	resp := map[string]interface{}{
		"prefix":   prefix,
		"q":        q,
		"items":    items,
		"has_more": end < len(keys),
	}
	if end < len(keys) {
		resp["next_after"] = keys[end-1]
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
