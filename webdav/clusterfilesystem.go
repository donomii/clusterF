// Package webdav provides WebDAV server functionality for the cluster filesystem
package webdav

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/donomii/clusterF/types"
	webdav "github.com/donomii/go-webdav"
)

// ClusterFileSystem implements webdav.FileSystem for the cluster
type ClusterFileSystem struct {
	fs         types.FileSystemLike
	clusterDir string // cluster path prefix to serve (e.g., "/photos")
}

// debugf logs debug messages (placeholder - will use a logger if available)
func (cfs *ClusterFileSystem) debugf(format string, args ...interface{}) {
	// For now, this is a no-op. In a real implementation, this would
	// use a logger passed in during construction
	fmt.Printf("[DEBUG ClusterFileSystem] "+format+"\n", args...)
}

type clusterFileWriter struct {
	cfs         *ClusterFileSystem
	clusterPath string
	buffer      bytes.Buffer
	closed      bool
}

func (w *clusterFileWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("write after close")
	}
	return w.buffer.Write(p)
}

func (w *clusterFileWriter) Close() error {
	if w.closed {
		return nil
	}

	data := w.buffer.Bytes()
	contentType := ""
	if len(data) > 0 {
		contentType = http.DetectContentType(data)
	}
	if contentType == "" || contentType == "application/octet-stream" {
		if ext := path.Ext(w.clusterPath); ext != "" {
			if ct := contentTypeFromExt(ext); ct != "" {
				contentType = ct
			}
		}
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// FIXME context?
	if _, err := w.cfs.fs.StoreFileWithModTime(context.Background(), w.clusterPath, data, contentType, time.Now()); err != nil {
		return webdav.NewHTTPError(http.StatusInternalServerError, err)
	}

	w.closed = true
	return nil
}

// NewClusterFileSystem creates a new WebDAV filesystem backed by the cluster
func NewClusterFileSystem(fs types.FileSystemLike, clusterDir string) *ClusterFileSystem {
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

	return &ClusterFileSystem{
		fs:         fs,
		clusterDir: clusterDir,
	}
}

// clusterPath converts a WebDAV path to a cluster path
func (cfs *ClusterFileSystem) clusterPath(name string) string {
	// Clean the path
	name = path.Clean(name)

	// If we have a cluster directory filter, prepend it
	if cfs.clusterDir != "" {
		if name == "/" {
			return cfs.clusterDir
		}
		return cfs.clusterDir + name
	}

	// No filter - use path as-is
	return name
}

// webdavPath converts a cluster path back to a WebDAV path
func (cfs *ClusterFileSystem) webdavPath(clusterPath string) string {
	// If we have a cluster directory filter, strip it
	if cfs.clusterDir != "" {
		if clusterPath == cfs.clusterDir {
			return "/"
		}
		if strings.HasPrefix(clusterPath, cfs.clusterDir+"/") {
			return strings.TrimPrefix(clusterPath, cfs.clusterDir)
		}
		// Path is outside our cluster directory - shouldn't happen
		return "/"
	}

	// No filter - use path as-is
	return clusterPath
}

// shouldServePath checks if the given cluster path should be served based on the cluster directory filter
func (cfs *ClusterFileSystem) shouldServePath(clusterPath string) bool {
	if cfs.clusterDir == "" {
		// No filter - serve everything
		return true
	}
	// With filter - only serve paths within the cluster directory
	return clusterPath == cfs.clusterDir || strings.HasPrefix(clusterPath, cfs.clusterDir)
}

// Open opens a file for reading
func (cfs *ClusterFileSystem) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	cfs.debugf("Open() called with name='%s'", name)
	cfs.debugf("[WEBDAV] Open called: name=%s", name)
	clusterPath := cfs.clusterPath(name)
	cfs.debugf("[WEBDAV] Open: mapped to cluster path=%s", clusterPath)

	if !cfs.shouldServePath(clusterPath) {
		cfs.debugf("[WEBDAV] Open: path not allowed, returning 404")
		return nil, webdav.NewHTTPError(http.StatusNotFound, fmt.Errorf("path not found"))
	}

	data, _, err := cfs.fs.GetFile(clusterPath)
	if err != nil {
		cfs.debugf("[WEBDAV] Open: GetFile failed: %v", err)
		return nil, webdav.NewHTTPError(http.StatusNotFound, err)
	}

	cfs.debugf("[WEBDAV] Open: successfully read %d bytes", len(data))
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Stat returns file information
func (cfs *ClusterFileSystem) Stat(ctx context.Context, name string) (*webdav.FileInfo, error) {
	cfs.debugf("Stat() called with name='%s'", name)
	cfs.debugf("[WEBDAV] Stat called: name=%s", name)
	clusterPath := cfs.clusterPath(name)
	cfs.debugf("[WEBDAV] Stat: mapped to cluster path=%s", clusterPath)

	if !cfs.shouldServePath(clusterPath) {
		cfs.debugf("[WEBDAV] Stat: path not allowed, returning 404")
		return nil, webdav.NewHTTPError(http.StatusNotFound, fmt.Errorf("path not found"))
	}

	// First try to get basic metadata
	metaS, err := cfs.fs.MetadataForPath(clusterPath)
	meta := &metaS
	if err != nil {
		cfs.debugf("[WEBDAV] Stat: MetadataForPath failed: %v", err)
		return cfs.fakeStatIfNotFound(clusterPath)
	}

	cfs.debugf("[WEBDAV] Stat: found metadata - size=%d, isDir=%t", meta.Size, meta.IsDirectory)

	// Convert cluster metadata to WebDAV FileInfo
	modTime := meta.ModifiedAt
	if modTime.IsZero() {
		modTime = time.Now()
	}

	// Use checksum as ETag if available, otherwise fallback to timestamp+size
	etag := fmt.Sprintf("%x%x", modTime.UnixNano(), meta.Size)
	if meta.Checksum != "" {
		etag = fmt.Sprintf("\"%s\"", meta.Checksum)
	} else {
		panic("no")
	}

	// Determine content type
	contentType := "application/octet-stream"
	if !meta.IsDirectory {
		// For files, try to get full metadata to get content type
		if _, fullMeta, err := cfs.fs.GetFile(clusterPath); err == nil {
			if fullMeta.ContentType != "" {
				contentType = fullMeta.ContentType
			}
			// Also use checksum from full metadata if not available in basic meta
			if meta.Checksum == "" && fullMeta.Checksum != "" {
				etag = fmt.Sprintf("\"%s\"", fullMeta.Checksum)
			}
		}
		// Fallback to extension-based detection
		if contentType == "application/octet-stream" {
			if ext := path.Ext(name); ext != "" {
				if ct := contentTypeFromExt(ext); ct != "" {
					contentType = ct
				}
			}
		}
	}

	cfs.debugf("[WEBDAV] Stat: returning FileInfo - contentType=%s, etag=%s", contentType, etag)
	return &webdav.FileInfo{
		Path:     name,
		Size:     meta.Size,
		ModTime:  modTime,
		IsDir:    meta.IsDirectory,
		MIMEType: contentType,
		ETag:     etag,
	}, nil
}

func (cfs *ClusterFileSystem) fakeStatIfNotFound(clusterPath string) (*webdav.FileInfo, error) {
	if !strings.HasSuffix(clusterPath, "/") {
		return nil, webdav.NewHTTPError(http.StatusNotFound, fmt.Errorf("path not found"))
	}
	// If the path doesn't exist, we can fake a directory if it looks like one
	dps, err := cfs.fs.ListDirectory(clusterPath)
	if err != nil {
		return nil, webdav.NewHTTPError(http.StatusNotFound, err)
	}

	if len(dps) == 0 {
		return nil, webdav.NewHTTPError(http.StatusNotFound, fmt.Errorf("path not found"))
	}

	if dps != nil {
		return &webdav.FileInfo{
			Path:     clusterPath,
			Size:     0,
			ModTime:  dps[0].ModifiedAt, //FIXME use the latest modified time from the list
			IsDir:    true,
			MIMEType: "application/directory",
			ETag:     fmt.Sprintf("%x", dps[0].ModifiedAt.UnixNano()),
		}, nil

	}
	return nil, webdav.NewHTTPError(http.StatusNotFound, fmt.Errorf("path not found"))
}

// ReadDir lists directory contents
func (cfs *ClusterFileSystem) ReadDir(ctx context.Context, name string, recursive bool) ([]webdav.FileInfo, error) {
	cfs.debugf("ReadDir() called with name='%s', recursive=%t", name, recursive)
	cfs.debugf("[WEBDAV] ReadDir called: name=%s, recursive=%t", name, recursive)
	clusterPath := cfs.clusterPath(name)
	cfs.debugf("[WEBDAV] ReadDir: mapped to cluster path=%s", clusterPath)

	if !cfs.shouldServePath(clusterPath) {
		cfs.debugf("[WEBDAV] ReadDir: path not allowed, returning 404")
		return nil, webdav.NewHTTPError(http.StatusNotFound, fmt.Errorf("path not found"))
	}

	entries, err := cfs.fs.ListDirectory(clusterPath)
	if err != nil {
		cfs.debugf("[WEBDAV] ReadDir: ListDirectory failed: %v", err)
		return nil, webdav.NewHTTPError(http.StatusNotFound, err)
	}

	cfs.debugf("[WEBDAV] ReadDir: found %d entries", len(entries))

	var result []webdav.FileInfo
	for _, entry := range entries {
		// Only include entries that should be served
		if !cfs.shouldServePath(entry.Path) {
			cfs.debugf("[WEBDAV] ReadDir: skipping entry %s (not allowed)", entry.Path)
			continue
		}

		/*
			TODO: Handle recursive listing properly?
				// Skip non-direct children if not recursive
				if !recursive {
					// Check if this entry is a direct child
					relativePath := strings.TrimPrefix(entry.Path, clusterPath)
					relativePath = strings.TrimPrefix(relativePath, "/")
					if strings.Contains(relativePath, "/") {
						cfs.debugf("[WEBDAV] ReadDir: skipping nested entry %s (rel: %s)", entry.Path, relativePath)
						continue // Skip nested entries
					}
				}
		*/

		webdavPath := cfs.webdavPath(entry.Path)
		cfs.debugf("[WEBDAV] ReadDir: mapped to WebDAV path=%s", webdavPath)

		modTime := entry.ModifiedAt
		if modTime.IsZero() {
			modTime = time.Now()
		}

		// Use checksum as ETag if available, otherwise fallback to timestamp+size
		etag := fmt.Sprintf("%x%x", modTime.UnixNano(), entry.Size)
		if entry.Checksum != "" {
			etag = fmt.Sprintf("\"%s\"", entry.Checksum)
		}

		// Determine content type
		contentType := entry.ContentType
		if contentType == "" && !entry.IsDirectory {
			if ext := path.Ext(entry.Path); ext != "" {
				if ct := contentTypeFromExt(ext); ct != "" {
					contentType = ct
				}
			}
			if contentType == "" {
				contentType = "application/octet-stream"
			}
		}

		fileInfo := webdav.FileInfo{
			Path:     webdavPath,
			Size:     entry.Size,
			ModTime:  modTime,
			IsDir:    entry.IsDirectory,
			MIMEType: contentType,
			ETag:     etag,
		}

		result = append(result, fileInfo)
	}

	cfs.debugf("[WEBDAV] ReadDir: returning %d results", len(result))
	return result, nil
}

// Create creates or updates a file
func (cfs *ClusterFileSystem) Create(ctx context.Context, name string) (io.WriteCloser, error) {
	cfs.debugf("Create() called with name='%s'", name)
	clusterPath := cfs.clusterPath(name)
	cfs.debugf("Create: mapped to cluster path=%s", clusterPath)

	if !cfs.shouldServePath(clusterPath) {
		return nil, webdav.NewHTTPError(http.StatusForbidden, fmt.Errorf("path not allowed"))
	}

	if meta, err := cfs.fs.MetadataForPath(clusterPath); err != nil {
		if !errors.Is(err, types.ErrFileNotFound) {
			return nil, webdav.NewHTTPError(http.StatusInternalServerError, err)
		}
	} else if meta.IsDirectory {
		return nil, webdav.NewHTTPError(http.StatusMethodNotAllowed, fmt.Errorf("cannot overwrite directory"))
	}

	return &clusterFileWriter{
		cfs:         cfs,
		clusterPath: clusterPath,
	}, nil
}

// RemoveAll removes a file or directory
func (cfs *ClusterFileSystem) RemoveAll(ctx context.Context, name string) error {
	cfs.debugf("RemoveAll() called with name='%s'", name)
	clusterPath := cfs.clusterPath(name)
	cfs.debugf("RemoveAll: mapped to cluster path=%s", clusterPath)

	if !cfs.shouldServePath(clusterPath) {
		return webdav.NewHTTPError(http.StatusNotFound, fmt.Errorf("path not found"))
	}

	// Check if the path exists first
	_, err := cfs.fs.MetadataForPath(clusterPath)
	if err != nil {
		return webdav.NewHTTPError(http.StatusNotFound, err)
	}

	// Delete the file/directory
	if err := cfs.fs.DeleteFile(ctx, clusterPath); err != nil {
		return webdav.NewHTTPError(http.StatusInternalServerError, err)
	}

	return nil
}

// Mkdir creates a directory
func (cfs *ClusterFileSystem) Mkdir(ctx context.Context, name string) error {
	cfs.debugf("Mkdir() called with name='%s'", name)
	clusterPath := cfs.clusterPath(name)
	cfs.debugf("Mkdir: mapped to cluster path=%s", clusterPath)

	if !cfs.shouldServePath(clusterPath) {
		return webdav.NewHTTPError(http.StatusForbidden, fmt.Errorf("path not allowed"))
	}

	// Check if directory already exists
	if _, err := cfs.fs.MetadataForPath(clusterPath); err == nil {
		return webdav.NewHTTPError(http.StatusMethodNotAllowed, fmt.Errorf("directory already exists"))
	}

	// Create the directory
	if err := cfs.fs.CreateDirectory(clusterPath); err != nil {
		return webdav.NewHTTPError(http.StatusInternalServerError, err)
	}

	return nil
}

// Copy copies a file or directory
func (cfs *ClusterFileSystem) Copy(ctx context.Context, src, dst string, options *webdav.CopyOptions) (created bool, err error) {
	cfs.debugf("Copy() called with src='%s', dst='%s'", src, dst)
	srcClusterPath := cfs.clusterPath(src)
	dstClusterPath := cfs.clusterPath(dst)
	cfs.debugf("Copy: src mapped to %s, dst mapped to %s", srcClusterPath, dstClusterPath)

	if !cfs.shouldServePath(srcClusterPath) {
		return false, webdav.NewHTTPError(http.StatusNotFound, fmt.Errorf("source path not found"))
	}

	if !cfs.shouldServePath(dstClusterPath) {
		return false, webdav.NewHTTPError(http.StatusForbidden, fmt.Errorf("destination path not allowed"))
	}

	// Check if source exists
	srcMeta, err := cfs.fs.MetadataForPath(srcClusterPath)
	if err != nil {
		return false, webdav.NewHTTPError(http.StatusNotFound, fmt.Errorf("source not found"))
	}

	// Check if destination exists
	_, dstErr := cfs.fs.MetadataForPath(dstClusterPath)
	created = dstErr != nil

	if !created && options != nil && options.NoOverwrite {
		return false, webdav.NewHTTPError(http.StatusPreconditionFailed, fmt.Errorf("destination exists"))
	}

	if srcMeta.IsDirectory {
		// Copy directory (simplified - just create empty directory)
		if err := cfs.fs.CreateDirectory(dstClusterPath); err != nil {
			return false, webdav.NewHTTPError(http.StatusInternalServerError, err)
		}

		// If recursive, copy contents
		if options == nil || !options.NoRecursive {
			entries, err := cfs.fs.ListDirectory(srcClusterPath)
			if err != nil {
				return false, webdav.NewHTTPError(http.StatusInternalServerError, err)
			}

			for _, entry := range entries {
				if !strings.HasPrefix(entry.Path, srcClusterPath+"/") {
					continue
				}

				relativePath := strings.TrimPrefix(entry.Path, srcClusterPath)
				newDstPath := dstClusterPath + relativePath

				if entry.ModifiedAt.IsZero() {
					panic("no")
				}
				if entry.IsDirectory {
					cfs.fs.CreateDirectory(newDstPath)
				} else {
					// Copy file
					data, _, err := cfs.fs.GetFile(entry.Path)
					if err != nil {
						continue // Skip files that can't be read
					}
					_, _ = cfs.fs.StoreFileWithModTime(ctx, newDstPath, data, entry.ContentType, entry.ModifiedAt)
				}
			}
		}
	} else {
		// Copy file
		data, fullMeta, err := cfs.fs.GetFile(srcClusterPath)
		if err != nil {
			return false, webdav.NewHTTPError(http.StatusInternalServerError, err)
		}

		contentType := fullMeta.ContentType
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		if fullMeta.ModifiedAt.IsZero() {
			panic("no")
		}
		if _, err := cfs.fs.StoreFileWithModTime(ctx, dstClusterPath, data, contentType, fullMeta.ModifiedAt); err != nil {
			return false, webdav.NewHTTPError(http.StatusInternalServerError, err)
		}
	}

	return created, nil
}

// Move moves a file or directory
func (cfs *ClusterFileSystem) Move(ctx context.Context, src, dst string, options *webdav.MoveOptions) (created bool, err error) {
	cfs.debugf("Move() called with src='%s', dst='%s'", src, dst)
	srcClusterPath := cfs.clusterPath(src)
	dstClusterPath := cfs.clusterPath(dst)
	cfs.debugf("Move: src mapped to %s, dst mapped to %s", srcClusterPath, dstClusterPath)

	if !cfs.shouldServePath(srcClusterPath) {
		return false, webdav.NewHTTPError(http.StatusNotFound, fmt.Errorf("source path not found"))
	}

	if !cfs.shouldServePath(dstClusterPath) {
		return false, webdav.NewHTTPError(http.StatusForbidden, fmt.Errorf("destination path not allowed"))
	}

	// Check if source exists
	_, err = cfs.fs.MetadataForPath(srcClusterPath)
	if err != nil {
		return false, webdav.NewHTTPError(http.StatusNotFound, fmt.Errorf("source not found"))
	}

	// Check if destination exists
	_, dstErr := cfs.fs.MetadataForPath(dstClusterPath)
	created = dstErr != nil

	if !created && options != nil && options.NoOverwrite {
		return false, webdav.NewHTTPError(http.StatusPreconditionFailed, fmt.Errorf("destination exists"))
	}

	// For simplicity, implement move as copy + delete
	copyOpts := &webdav.CopyOptions{
		NoOverwrite: false, // We already checked above
		NoRecursive: false, // Always recursive for move
	}

	_, err = cfs.Copy(ctx, src, dst, copyOpts)
	if err != nil {
		return false, err
	}

	// Delete source
	if err := cfs.RemoveAll(ctx, src); err != nil {
		// Try to clean up the destination if source delete failed
		cfs.RemoveAll(ctx, dst)
		return false, err
	}

	return created, nil
}

// contentTypeFromExt returns content type based on file extension
func contentTypeFromExt(ext string) string {
	ext = strings.ToLower(ext)
	switch ext {
	case ".txt", ".log", ".md":
		return "text/plain"
	case ".json":
		return "application/json"
	case ".html", ".htm":
		return "text/html"
	case ".css":
		return "text/css"
	case ".js":
		return "application/javascript"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".svg":
		return "image/svg+xml"
	case ".pdf":
		return "application/pdf"
	case ".zip":
		return "application/zip"
	case ".tar":
		return "application/x-tar"
	case ".gz":
		return "application/gzip"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".mp4":
		return "video/mp4"
	case ".avi":
		return "video/x-msvideo"
	case ".mov":
		return "video/quicktime"
	default:
		return "application/octet-stream"
	}
}
