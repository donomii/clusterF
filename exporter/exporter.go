// Package exporter provides a filesystem watcher that mirrors changes between
// the cluster file system and a local directory for OS-level sharing.
package exporter

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/types"
	"github.com/fsnotify/fsnotify"
)

// Logger captures the logging functionality required by the exporter.
type Logger interface {
	Printf(format string, v ...any)
}

// Exporter mirrors files/directories from the cluster FS to a local directory
// so the OS can share them via SMB/CIFS or other means.
type Exporter struct {
	base    string
	watcher *fsnotify.Watcher
	ignore  *syncmap.SyncMap[string, time.Time] // full path -> expiry
	watched *syncmap.SyncMap[string, struct{}]

	fs     types.FileSystemLike
	logger *log.Logger
}

// New creates a new exporter for a given local base directory.
func New(base string, logger *log.Logger, fs types.FileSystemLike) (*Exporter, error) {
	if base == "" {
		return nil, fmt.Errorf("export base cannot be empty")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}
	if fs == nil {
		return nil, fmt.Errorf("file system cannot be nil")
	}
	if err := os.MkdirAll(base, 0o755); err != nil {
		return nil, err
	}
	return &Exporter{
		base:    base,
		ignore:  syncmap.NewSyncMap[string, time.Time](),
		watched: syncmap.NewSyncMap[string, struct{}](),
		fs:      fs,
		logger:  logger,
	}, nil
}

func (e *Exporter) pathFor(clusterPath string) string {
	// clusterPath starts with '/'
	rel := clusterPath
	if len(rel) > 0 && rel[0] == '/' {
		rel = rel[1:]
	}
	return filepath.Join(e.base, filepath.FromSlash(rel))
}

func (e *Exporter) Mkdir(clusterPath string) error {
	return e.MkdirWithModTime(clusterPath, time.Now())
}

func (e *Exporter) MkdirWithModTime(clusterPath string, modTime time.Time) error {
	full := e.pathFor(clusterPath)
	if err := os.MkdirAll(full, 0o755); err != nil {
		return err
	}
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
	if err := os.WriteFile(full, data, 0o644); err != nil {
		return err
	}
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
func (e *Exporter) Run(ctx context.Context) {
	// Initial import
	if err := e.importAll(); err != nil {
		e.logger.Printf("[EXPORT] Initial import error: %v", err)
	}

	// Start watcher
	if err := e.startWatcher(ctx); err != nil {
		e.logger.Printf("[EXPORT] Watcher error: %v", err)
	}
}

func (e *Exporter) startWatcher(ctx context.Context) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	e.watcher = w

	// Add watchers recursively
	if err := e.addWatchRecursive(e.base); err != nil {
		e.logger.Printf("[EXPORT] addWatchRecursive error: %v", err)
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
				if !ok {
					continue
				}

				// If a new directory, add watcher
				if ev.Op&(fsnotify.Create) != 0 {
					if isDir(ev.Name) {
						// ensure exists in cluster and add watch
						st, _ := os.Stat(ev.Name)
						modT := time.Now()
						if st != nil {
							modT = st.ModTime()
						}
						if err := e.fs.CreateDirectoryWithModTime(clusterPath, modT); err != nil {
							// ignore if already exists
						}
						_ = e.addWatchRecursive(ev.Name)
						continue
					}
				}

				// Handle file changes
				if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
					if !isDir(ev.Name) {
						st, statErr := os.Stat(ev.Name)
						if statErr != nil {
							continue
						}
						if meta, err2 := e.fs.MetadataForPath(clusterPath); err2 == nil {
							if metadataMatches(meta, st.Size(), st.ModTime()) {
								continue
							}
						}
						data, err := os.ReadFile(ev.Name)
						if err == nil {
							ct := contentTypeFromExt(ev.Name)
							modT := st.ModTime()
							if err := e.fs.StoreFileWithModTime(clusterPath, data, ct, modT); err != nil {
								e.logger.Printf("[EXPORT] StoreFile error for %s: %v", clusterPath, err)
							}
						}
						continue
					}
				}
				if ev.Op&(fsnotify.Remove|fsnotify.Rename) != 0 {
					if e.isWatchedDir(ev.Name) {
						e.removeWatchRecursive(ev.Name)
					}
					// delete from cluster (best-effort)
					if err := e.fs.DeleteFile(clusterPath); err != nil {
						// directories might be non-empty; ignore errors
					}
					continue
				}
			case err := <-w.Errors:
				if err != nil {
					e.logger.Printf("[EXPORT] watcher error: %v", err)
				}
			}
		}
	}()
	return nil
}

func (e *Exporter) addWatchRecursive(dir string) error {
	// Avoid watching non-existent dirs
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil
	}
	// Walk
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if err := e.addWatch(p); err != nil {
				return err
			}
		}
		return nil
	})
}

func (e *Exporter) addWatch(path string) error {
	if _, loaded := e.watched.LoadOrStore(path, struct{}{}); loaded {
		return nil
	}

	if err := e.watcher.Add(path); err != nil {
		e.watched.Delete(path)
		return err
	}
	return nil
}

func (e *Exporter) isWatchedDir(path string) bool {
	_, ok := e.watched.Load(path)
	return ok
}

func (e *Exporter) removeWatchRecursive(dir string) {
	var toRemove []string
	e.watched.Range(func(p string, _ struct{}) bool {
		if hasPathPrefix(p, dir) {
			toRemove = append(toRemove, p)
		}
		return true
	})

	for _, p := range toRemove {
		_ = e.watcher.Remove(p)
		e.watched.Delete(p)
	}
}

func hasPathPrefix(path, prefix string) bool {
	path = filepath.Clean(path)
	prefix = filepath.Clean(prefix)
	if path == prefix {
		return true
	}
	if len(prefix) == 0 {
		return false
	}
	if !strings.HasSuffix(prefix, string(filepath.Separator)) {
		prefix += string(filepath.Separator)
	}
	return strings.HasPrefix(path, prefix)
}

func (e *Exporter) clusterPathFor(full string) (string, bool) {
	// Ensure path is under base
	rel, err := filepath.Rel(e.base, full)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", false
	}
	rel = filepath.ToSlash(rel)
	if rel == "." || rel == "" {
		return "/", true
	}
	return "/" + rel, true
}

func (e *Exporter) importAll() error {
	return filepath.WalkDir(e.base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		clusterPath, ok := e.clusterPathFor(p)
		if !ok || clusterPath == "/" {
			return nil
		}
		if d.IsDir() {
			// ensure directory exists in cluster
			if meta, err := e.fs.MetadataForPath(clusterPath); err == nil && meta != nil && meta.IsDirectory {
				return nil
			}
			_ = e.fs.CreateDirectory(clusterPath)
			return nil
		}
		// file
		st, err := os.Stat(p)
		if err != nil {
			return nil
		}
		if meta, err := e.fs.MetadataForPath(clusterPath); err == nil {
			if metadataMatches(meta, st.Size(), st.ModTime()) {
				return nil
			}
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return nil
		}
		ct := contentTypeFromExt(p)
		_ = e.fs.StoreFileWithModTime(clusterPath, data, ct, st.ModTime())
		return nil
	})
}

func metadataMatches(meta *types.Metadata, size int64, modTime time.Time) bool {
	if meta == nil || meta.IsDirectory {
		return false
	}
	if meta.Size != size {
		return false
	}
	if meta.ModifiedAt.IsZero() {
		return false
	}
	diff := meta.ModifiedAt.Sub(modTime)
	if diff < 0 {
		diff = -diff
	}
	return diff <= time.Second
}

func (e *Exporter) markIgnore(full string) {
	e.ignore.Store(full, time.Now().Add(2*time.Second))
}

func (e *Exporter) shouldIgnore(full string) bool {
	if t, ok := e.ignore.Load(full); ok {
		if time.Now().Before(t) {
			return true
		}
		e.ignore.Delete(full)
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
