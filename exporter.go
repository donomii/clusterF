// exporter.go - Optional OS export of the cluster filesystem to a local directory
package main

import (
    "context"
    "fmt"
    "io/fs"
    "os"
    "path/filepath"
    "strings"
    "sync"
    "time"

    "github.com/fsnotify/fsnotify"
)

// Exporter mirrors files/directories from the cluster FS to a local directory
// so the OS can share them via SMB/CIFS or other means.
type Exporter struct {
    base    string
    watcher *fsnotify.Watcher
    igMu    sync.Mutex
    ignore  map[string]time.Time // full path -> expiry
}

func NewExporter(base string) (*Exporter, error) {
    if base == "" {
        return nil, fmt.Errorf("export base cannot be empty")
    }
    if err := os.MkdirAll(base, 0o755); err != nil {
        return nil, err
    }
    return &Exporter{base: base, ignore: make(map[string]time.Time)}, nil
}

func (e *Exporter) pathFor(clusterPath string) string {
    // clusterPath starts with '/'
    rel := clusterPath
    if len(rel) > 0 && rel[0] == '/' {
        rel = rel[1:]
    }
    return filepath.Join(e.base, filepath.FromSlash(rel))
}

func (e *Exporter) Mkdir(clusterPath string) error { return e.MkdirWithModTime(clusterPath, time.Now()) }

func (e *Exporter) MkdirWithModTime(clusterPath string, modTime time.Time) error {
    full := e.pathFor(clusterPath)
    if err := os.MkdirAll(full, 0o755); err != nil { return err }
    // Set directory times best-effort
    _ = os.Chtimes(full, modTime, modTime)
    return nil
}

func (e *Exporter) WriteFile(clusterPath string, data []byte, modTime time.Time) error {
    full := e.pathFor(clusterPath)
    if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
        return err
    }
    // If existing file is newer or same mtime, skip write
    if st, err := os.Stat(full); err == nil {
        if !st.ModTime().Before(modTime) {
            return nil
        }
    }
    e.markIgnore(full)
    if err := os.WriteFile(full, data, 0o644); err != nil { return err }
    // Apply source modification time
    _ = os.Chtimes(full, modTime, modTime)
    return nil
}

func (e *Exporter) RemoveFile(clusterPath string) error {
    full := e.pathFor(clusterPath)
    e.markIgnore(full)
    if err := os.Remove(full); err != nil && !os.IsNotExist(err) {
        return err
    }
    return nil
}

func (e *Exporter) RemoveDir(clusterPath string) error {
    full := e.pathFor(clusterPath)
    e.markIgnore(full)
    // Remove empty dir only; don't nuke recursively by default to be safe
    if err := os.Remove(full); err != nil {
        // If not empty, attempt recursive remove as last resort
        if err := os.RemoveAll(full); err != nil && !os.IsNotExist(err) {
            return err
        }
    }
    return nil
}

// Run performs an initial import and watches for changes in the export dir,
// mirroring them into the cluster in near real time.
func (e *Exporter) Run(ctx context.Context, c *Cluster) {
    // Initial import
    if err := e.importAll(c); err != nil {
        c.Logger.Printf("[EXPORT] Initial import error: %v", err)
    }

    // Start watcher
    if err := e.startWatcher(ctx, c); err != nil {
        c.Logger.Printf("[EXPORT] Watcher error: %v", err)
    }
}

func (e *Exporter) startWatcher(ctx context.Context, c *Cluster) error {
    w, err := fsnotify.NewWatcher()
    if err != nil { return err }
    e.watcher = w

    // Add watchers recursively
    if err := e.addWatchRecursive(e.base); err != nil {
        c.Logger.Printf("[EXPORT] addWatchRecursive error: %v", err)
    }

    // Event loop
    go func() {
        defer w.Close()
        for {
            select {
            case <-ctx.Done():
                return
            case ev := <-w.Events:
                if e.shouldIgnore(ev.Name) {
                    continue
                }
                // Normalize path and map to cluster path
                clusterPath, ok := e.clusterPathFor(ev.Name)
                if !ok { continue }

                // If a new directory, add watcher
                if ev.Op&(fsnotify.Create) != 0 {
                    if isDir(ev.Name) {
                        // ensure exists in cluster and add watch
                        st, _ := os.Stat(ev.Name)
                        modT := time.Now()
                        if st != nil { modT = st.ModTime() }
                        if err := c.FileSystem.CreateDirectoryWithModTime(clusterPath, modT); err != nil {
                            // ignore if already exists
                        }
                        _ = e.addWatchRecursive(ev.Name)
                        continue
                    }
                }

                // Handle file changes
                if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
                    if !isDir(ev.Name) {
                        // read and store
                        data, err := os.ReadFile(ev.Name)
                        if err == nil {
                            // rudimentary content type by extension
                            ct := contentTypeFromExt(ev.Name)
                            // Decide by modification time: only import if disk newer
                            if meta, err2 := c.FileSystem.getMetadata(clusterPath); err2 == nil {
                                st, _ := os.Stat(ev.Name)
                                if st != nil && !st.ModTime().After(meta.ModifiedAt) {
                                    // cluster is newer or equal — skip
                                    continue
                                }
                            }
                            st, _ := os.Stat(ev.Name)
                            modT := time.Now()
                            if st != nil { modT = st.ModTime() }
                            if err := c.FileSystem.StoreFileWithModTime(clusterPath, data, ct, modT); err != nil {
                                c.Logger.Printf("[EXPORT] StoreFile error for %s: %v", clusterPath, err)
                            }
                        }
                        continue
                    }
                }

                if ev.Op&(fsnotify.Remove) != 0 {
                    // delete from cluster (best-effort)
                    if err := c.FileSystem.DeleteFile(clusterPath); err != nil {
                        // directories might be non-empty; ignore errors
                    }
                    continue
                }
            case err := <-w.Errors:
                if err != nil {
                    c.Logger.Printf("[EXPORT] watcher error: %v", err)
                }
            }
        }
    }()
    return nil
}

func (e *Exporter) addWatchRecursive(dir string) error {
    // Avoid watching non-existent dirs
    info, err := os.Stat(dir)
    if err != nil || !info.IsDir() { return nil }
    // Walk
    return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
        if err != nil { return nil }
        if d.IsDir() {
            _ = e.watcher.Add(p)
        }
        return nil
    })
}

func (e *Exporter) clusterPathFor(full string) (string, bool) {
    // Ensure path is under base
    rel, err := filepath.Rel(e.base, full)
    if err != nil || strings.HasPrefix(rel, "..") { return "", false }
    rel = filepath.ToSlash(rel)
    if rel == "." || rel == "" { return "/", true }
    return "/" + rel, true
}

func (e *Exporter) importAll(c *Cluster) error {
    return filepath.WalkDir(e.base, func(p string, d fs.DirEntry, err error) error {
        if err != nil { return nil }
        clusterPath, ok := e.clusterPathFor(p)
        if !ok || clusterPath == "/" { return nil }
        if d.IsDir() {
            // ensure directory exists in cluster
            if meta, err := c.FileSystem.getMetadata(clusterPath); err == nil && meta.IsDirectory {
                return nil
            }
            _ = c.FileSystem.CreateDirectory(clusterPath)
            return nil
        }
        // file
        data, err := os.ReadFile(p)
        if err != nil { return nil }
        ct := contentTypeFromExt(p)
        // Store file (overwrite or create)
        _ = c.FileSystem.StoreFile(clusterPath, data, ct)
        return nil
    })
}

func (e *Exporter) markIgnore(full string) {
    e.igMu.Lock()
    defer e.igMu.Unlock()
    e.ignore[full] = time.Now().Add(2 * time.Second)
}

func (e *Exporter) shouldIgnore(full string) bool {
    e.igMu.Lock()
    defer e.igMu.Unlock()
    if t, ok := e.ignore[full]; ok {
        if time.Now().Before(t) { return true }
        delete(e.ignore, full)
    }
    return false
}

func isDir(path string) bool {
    fi, err := os.Stat(path)
    return err == nil && fi.IsDir()
}

func contentTypeFromExt(path string) string {
    ext := strings.ToLower(filepath.Ext(path))
    switch ext {
    case ".txt", ".log", ".md":
        return "text/plain"
    case ".json":
        return "application/json"
    case ".html", ".htm":
        return "text/html"
    case ".png":
        return "image/png"
    case ".jpg", ".jpeg":
        return "image/jpeg"
    case ".gif":
        return "image/gif"
    case ".pdf":
        return "application/pdf"
    case ".mp3":
        return "audio/mpeg"
    case ".mp4":
        return "video/mp4"
    default:
        return "application/octet-stream"
    }
}
