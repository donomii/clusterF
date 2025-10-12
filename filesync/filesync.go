// Package exporter provides a filesystem watcher that mirrors changes between
// the cluster file system and a local directory for OS-level sharing.
package filesync

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

// Syncer mirrors files/directories from the cluster FS to a local directory
// so the OS can share them via SMB/CIFS or other means.
type Syncer struct {
	exportDir  string
	clusterDir string // optional cluster path prefix to filter (e.g., "/photos")
	watcher    *fsnotify.Watcher
	ignore     *syncmap.SyncMap[string, time.Time] // full path -> expiry
	watched    *syncmap.SyncMap[string, struct{}]

	fs     types.FileSystemLike
	logger *log.Logger

	importDir      string
	excludeDirs    map[string]bool // directories to exclude during import
	lastImport     time.Time
	importInterval time.Duration

	setCurrentFile func(string) // callback to set current file for monitoring
}

// SetCurrentFileCallback sets the callback to update current file for monitoring
func (e *Syncer) SetCurrentFileCallback(fn func(string)) {
	e.setCurrentFile = fn
}

// NewFileSyncer creates a new syncer with export and import directories.
func NewFileSyncer(exportDir string, importDir string, clusterDir string, excludeDirs string, logger *log.Logger, fs types.FileSystemLike) (*Syncer, error) {
	if exportDir == "" && importDir == "" {
		return nil, fmt.Errorf("either export or import directory must be specified")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}
	if fs == nil {
		return nil, fmt.Errorf("file system cannot be nil")
	}
	if exportDir != "" {
		if err := os.MkdirAll(exportDir, 0o755); err != nil {
			return nil, err
		}
	}
	// Normalize cluster directory path
	if clusterDir != "" {
		// Ensure it starts with / and doesn't end with / (unless it's root)
		if !strings.HasPrefix(clusterDir, "/") {
			clusterDir = "/" + clusterDir
		}
		if len(clusterDir) > 1 && strings.HasSuffix(clusterDir, "/") {
			clusterDir = clusterDir[:len(clusterDir)-1]
		}
	}
	// Parse exclude directories as full paths
	excludeDirMap := make(map[string]bool)
	if excludeDirs != "" {
		for _, dir := range strings.Split(excludeDirs, ",") {
			dir = strings.TrimSpace(dir)
			if dir != "" {
				// Convert to absolute path
				absDir, err := filepath.Abs(dir)
				if err != nil {
					return nil, fmt.Errorf("could not resolve exclude path %s: %v", dir, err)
				}
				excludeDirMap[absDir] = true
			}
		}
	}
	return &Syncer{
		exportDir:      exportDir,
		clusterDir:     clusterDir,
		importDir:      importDir,
		excludeDirs:    excludeDirMap,
		ignore:         syncmap.NewSyncMap[string, time.Time](),
		watched:        syncmap.NewSyncMap[string, struct{}](),
		fs:             fs,
		logger:         logger,
		importInterval: 24 * time.Hour,
	}, nil
}

// shouldExportPath checks if the given cluster path should be exported based on the cluster directory filter
func (e *Syncer) shouldExportPath(clusterPath string) bool {
	if e.clusterDir == "" {
		// No prefix filter - export everything except root
		return clusterPath != "/"
	}
	// With prefix filter - only export paths within the cluster directory
	return clusterPath == e.clusterDir || strings.HasPrefix(clusterPath, e.clusterDir+"/")
}

func (e *Syncer) pathFor(clusterPath string) string {
	// clusterPath starts with '/'
	rel := clusterPath
	if len(rel) > 0 && rel[0] == '/' {
		rel = rel[1:]
	}

	// If we have a cluster directory filter, strip it from the path
	if e.clusterDir != "" {
		clusterDirTrimmed := strings.TrimPrefix(e.clusterDir, "/")
		if strings.HasPrefix(rel, clusterDirTrimmed+"/") {
			// Remove the cluster directory prefix
			rel = strings.TrimPrefix(rel, clusterDirTrimmed+"/")
		} else if rel == clusterDirTrimmed {
			// This is the cluster directory itself
			rel = ""
		}
	}

	return filepath.Join(e.exportDir, filepath.FromSlash(rel))
}

func (e *Syncer) Mkdir(clusterPath string) error {
	return e.MkdirWithModTime(clusterPath, time.Now())
}

func (e *Syncer) MkdirWithModTime(clusterPath string, modTime time.Time) error {
	if !e.shouldExportPath(clusterPath) {
		return nil // Skip paths outside the cluster directory filter
	}
	full := e.pathFor(clusterPath)
	if err := os.MkdirAll(full, 0o755); err != nil {
		return err
	}
	// Set directory times best-effort
	_ = os.Chtimes(full, modTime, modTime)
	return nil
}

func (e *Syncer) WriteFile(clusterPath string, data []byte, modTime time.Time) error {
	if !e.shouldExportPath(clusterPath) {
		return nil // Skip paths outside the cluster directory filter
	}
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

func (e *Syncer) RemoveFile(clusterPath string) error {
	if !e.shouldExportPath(clusterPath) {
		return nil // Skip paths outside the cluster directory filter
	}
	full := e.pathFor(clusterPath)
	e.markIgnore(full)
	if err := os.Remove(full); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (e *Syncer) RemoveDir(clusterPath string) error {
	if !e.shouldExportPath(clusterPath) {
		return nil // Skip paths outside the cluster directory filter
	}
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

// Run performs export and import operations based on configuration.
func (e *Syncer) Run(ctx context.Context) {
	// Export mode: watch for changes in export dir
	if e.exportDir != "" {
		// Initial import from export dir to cluster
		if err := e.importAll(ctx); err != nil {
			e.logger.Printf("[EXPORT] Initial import error: %v", err)
		}

		// Start watcher
		go func() {
			if err := e.startWatcher(ctx); err != nil {
				e.logger.Printf("[EXPORT] Watcher error: %v", err)
			}
		}()
	}

	// Import mode: periodic import from import dir to cluster
	if e.importDir != "" {
		// Check import directory exists
		if _, err := os.Stat(e.importDir); err != nil {
			e.logger.Printf("[IMPORT] Import directory %s not accessible: %v", e.importDir, err)
			return
		}

		// Initial import
		if err := e.performImport(ctx); err != nil {
			e.logger.Printf("[IMPORT] Initial import error: %v", err)
		}

		// Start periodic import
		e.startPeriodicImport(ctx)
	}
}

func (e *Syncer) startWatcher(ctx context.Context) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	e.watcher = w

	// Add watchers recursively
	if err := e.addWatchRecursive(e.exportDir); err != nil {
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
							if err := e.fs.StoreFileWithModTime(ctx, clusterPath, data, ct, modT); err != nil {
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
					if err := e.fs.DeleteFile(ctx, clusterPath); err != nil {
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

func (e *Syncer) addWatchRecursive(dir string) error {
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

func (e *Syncer) addWatch(path string) error {
	if _, loaded := e.watched.LoadOrStore(path, struct{}{}); loaded {
		return nil
	}

	if err := e.watcher.Add(path); err != nil {
		e.watched.Delete(path)
		return err
	}
	return nil
}

func (e *Syncer) isWatchedDir(path string) bool {
	_, ok := e.watched.Load(path)
	return ok
}

func (e *Syncer) removeWatchRecursive(dir string) {
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

// clusterPathFor converts local export dir path to cluster path, returns (clusterPath, true) for files or ("", false) for errors
func (e *Syncer) clusterPathFor(full string) (string, bool) {
	// Ensure path is under base
	rel, err := filepath.Rel(e.exportDir, full)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", false
	}
	rel = filepath.ToSlash(rel)

	// If we have a cluster directory filter, prepend it to the path
	if e.clusterDir != "" {
		if rel == "." || rel == "" {
			// This is the base directory, which maps to the cluster directory
			return e.clusterDir, true
		}
		// Prepend the cluster directory to the relative path
		clusterDirTrimmed := strings.TrimPrefix(e.clusterDir, "/")
		return "/" + clusterDirTrimmed + "/" + rel, true
	}

	// No cluster directory filter - use original logic
	if rel == "." || rel == "" {
		return "/", true
	}
	return "/" + rel, true
}

func (e *Syncer) importAll(ctx context.Context) error {
	return filepath.WalkDir(e.exportDir, func(p string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return nil
		}
		clusterPath, ok := e.clusterPathFor(p)
		if !ok {
			return nil
		}

		// If we have a cluster directory filter, verify the path is within it
		if !e.shouldExportPath(clusterPath) {
			return nil
		}

		if d.IsDir() {
			// ensure directory exists in cluster
			if meta, err := e.fs.MetadataForPath(clusterPath); err == nil && meta.IsDirectory {
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
		_ = e.fs.StoreFileWithModTime(ctx, clusterPath, data, ct, st.ModTime())
		return nil
	})
}

func metadataMatches(meta types.FileMetadata, size int64, modTime time.Time) bool {
	if meta.IsDirectory {
		return false
	}
	if meta.Size != size {
		return false
	}
	if meta.ModifiedAt.IsZero() {
		panic("no")
	}
	diff := meta.ModifiedAt.Sub(modTime)
	if diff < 0 {
		diff = -diff
	}

	return diff.Milliseconds() < 1000
}

func (e *Syncer) markIgnore(full string) {
	e.ignore.Store(full, time.Now().Add(2*time.Second))
}

func (e *Syncer) shouldIgnore(full string) bool {
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

// performImport imports files from importDir to cluster and removes files from cluster not in importDir
func (e *Syncer) performImport(ctx context.Context) error {
	e.logger.Printf("[IMPORT] Starting import from %s to cluster prefix %s", e.importDir, e.clusterDir)

	// Import all files from importDir
	if err := e.importFromDir(ctx); err != nil {
		e.logger.Printf("[IMPORT] Error importing from directory: %v", err)
		return err
	}

	// Remove files from cluster that don't exist in importDir
	if err := e.cleanupClusterFiles(ctx); err != nil {
		e.logger.Printf("[IMPORT] Error cleaning up cluster files: %v", err)
		return err
	}

	e.lastImport = time.Now()
	e.logger.Printf("[IMPORT] Import completed successfully")
	return nil
}

// importFromDir walks importDir and uploads files to cluster
func (e *Syncer) importFromDir(ctx context.Context) error {
	var throttle = make(chan struct{}, 30) // limit concurrency
	return filepath.WalkDir(e.importDir, func(p string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return nil
		}

		// Check if this path should be excluded
		if d.IsDir() {
			absPath, err := filepath.Abs(p)
			if err == nil && e.excludeDirs[absPath] {
				e.logger.Printf("[IMPORT] Skipping excluded directory: %s", p)
				return filepath.SkipDir
			}
		}

		// Convert local path to cluster path
		clusterPath, ok := e.importPathToClusterPath(p)
		if !ok {
			return nil
		}

		if d.IsDir() {
			return nil
		}

		// Handle file
		st, err := os.Stat(p)
		if err != nil {
			return nil
		}

		throttle <- struct{}{}

		go uploadSyncfile(ctx, e, p, clusterPath, st, throttle)

		return nil
	})
}

func uploadSyncfile(ctx context.Context, e *Syncer, p, clusterPath string, st fs.FileInfo, throttle chan struct{}) {
	defer func() {
		if e.setCurrentFile != nil {
			e.setCurrentFile("")
		}
		<-throttle
	}()

	if e.setCurrentFile != nil {
		e.setCurrentFile(clusterPath)
	}

	// Check if file already exists with same content
	if meta, err := e.fs.MetadataForPath(clusterPath); err == nil {
		if metadataMatches(meta, st.Size(), st.ModTime()) {
			e.logger.Printf("[IMPORT] Skipping synchronised file %s", clusterPath)
			return
		} else {
			e.logger.Printf(`[IMPORT] Updating existing file "%s" in cluster: %+v, local size: %v, local time %v`, clusterPath, meta, st.Size(), st.ModTime())

		}
	}

	// Read and upload file
	data, err := os.ReadFile(p)
	if err != nil {
		return
	}

	ct := contentTypeFromExt(p)
	err = e.fs.StoreFileWithModTime(ctx, clusterPath, data, ct, st.ModTime())
	if err != nil {
		e.logger.Printf("[IMPORT] Synchronisation failed for %v: %v", clusterPath, err)
	} else {
		e.logger.Printf("[IMPORT] Synchronised %s", clusterPath)
	}
}

// importPathToClusterPath converts a local file path to cluster path, returns (clusterPath, true) for files or ("", false) for directories
func (e *Syncer) importPathToClusterPath(localPath string) (string, bool) {
	rel, err := filepath.Rel(e.importDir, localPath)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", false
	}
	rel = filepath.ToSlash(rel)

	// Map to cluster directory
	if rel == "." || rel == "" {
		return "", false
	}

	if e.clusterDir == "/" {
		return "/" + rel, true
	}
	return e.clusterDir + "/" + rel, true
}

// cleanupClusterFiles logs that cleanup is not implemented and returns nil
func (e *Syncer) cleanupClusterFiles(ctx context.Context) error {
	e.logger.Printf("[IMPORT] Cleanup of cluster files not yet implemented")
	return nil
}

// startPeriodicImport runs import every 24 hours
func (e *Syncer) startPeriodicImport(ctx context.Context) {
	ticker := time.NewTicker(e.importInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := os.Stat(e.importDir); err != nil {
				e.logger.Printf("[IMPORT] Import directory %s not accessible, skipping: %v", e.importDir, err)
				continue
			}

			if err := e.performImport(ctx); err != nil {
				e.logger.Printf("[IMPORT] Periodic import failed: %v", err)
			}
		}
	}
}
