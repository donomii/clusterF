package main

import (
    "net/http"
)

// handleLoadingPage serves a full-screen loading splash and redirects to the
// file browser once the node HTTP endpoints respond.
func (c *Cluster) handleLoadingPage(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>🐸 Loading…</title>
    <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🐸</text></svg>">
    <style>
        :root { color-scheme: dark; }
        html, body { height: 100%; }
        body {
            margin: 0;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: radial-gradient(1200px 600px at 50% -20%, rgba(59,130,246,0.18), transparent 60%),
                        linear-gradient(135deg, #0b1220 0%, #0f172a 100%);
            color: #e2e8f0;
            display: grid;
            place-items: center;
        }
        .wrap { text-align: center; }
        .frog { font-size: clamp(48px, 10vw, 120px); line-height: 1; filter: drop-shadow(0 6px 20px rgba(6,182,212,0.35)); }
        .title { font-size: clamp(28px, 6vw, 64px); font-weight: 800; letter-spacing: 0.5px; margin-top: 10px;
                 background: linear-gradient(45deg, #3b82f6, #8b5cf6, #06b6d4); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .sub { margin-top: 10px; color: #94a3b8; font-size: clamp(14px, 2.6vw, 18px); }
        .dots::after { content: '…'; animation: ellipsis 1.2s steps(4,end) infinite; }
        @keyframes ellipsis { 0% { content: ''; } 25% { content: '.'; } 50% { content: '..'; } 75% { content: '...'; } 100% { content: ''; } }
        .hint { margin-top: 24px; color: #64748b; font-size: 14px; }
        .btn { margin-top: 18px; display: inline-block; background: linear-gradient(45deg, #3b82f6, #8b5cf6);
               color: white; text-decoration: none; padding: 10px 16px; border-radius: 10px; font-weight: 600; }
    </style>
    <script>
        const target = '/files/';
        async function checkReady() {
            try {
                // Probe status and file API; both should be quick once server is up
                const [s, f] = await Promise.all([
                    fetch('/status', { cache: 'no-store' }),
                    fetch('/api/files/', { cache: 'no-store' })
                ]);
                if (s.ok && f.ok) {
                    // Small delay for a smooth handoff
                    setTimeout(() => { window.location.replace(target); }, 150);
                    return;
                }
            } catch (e) { /* ignore and retry */ }
            setTimeout(checkReady, 350);
        }
        document.addEventListener('DOMContentLoaded', checkReady);
    </script>
    <noscript>
        <style>#js-hint{display:block !important}</style>
    </noscript>
    </head>
<body>
    <div class="wrap">
        <div class="frog" aria-hidden="true">🐸</div>
        <div class="title">Loading Frogpond<span class="dots" aria-hidden="true"></span></div>
        <div class="sub">Preparing the file browser…</div>
        <div id="js-hint" class="hint" style="display:none">JavaScript required. <a class="btn" href="/files/">Open File Browser</a></div>
    </div>
</body>
</html>`
    w.Write([]byte(html))
}

