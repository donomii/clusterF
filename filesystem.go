// filesystem.go - Distributed file system layer on top of partition system
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/donomii/clusterF/discovery"
	exporter "github.com/donomii/clusterF/exporter"
	"github.com/donomii/clusterF/urlutil"
)

const (
	MaxFileNameLen = 255
	MaxPathLen     = 4096
)

// FileMetadata represents metadata for a file stored in the cluster
type FileMetadata struct {
	Name        string    `json:"name"`
	Path        string    `json:"path"` // Full path like "/docs/readme.txt"
	Size        int64     `json:"size"` // Total file size in bytes
	ContentType string    `json:"content_type"`
	CreatedAt   time.Time `json:"created_at"`
	ModifiedAt  time.Time `json:"modified_at"`
	IsDirectory bool      `json:"is_directory"`
	Children    []string  `json:"children,omitempty"` // For directories
}

// ClusterFileSystem provides a file system interface over the cluster
type ClusterFileSystem struct {
	cluster *Cluster
}

// NewClusterFileSystem creates a new distributed file system
func NewClusterFileSystem(cluster *Cluster) *ClusterFileSystem {
	return &ClusterFileSystem{
		cluster: cluster,
	}
}

// StoreFile requires explicit metadata; callers must provide modification time via StoreFileWithModTime.
func (fs *ClusterFileSystem) StoreFile(path string, content []byte, contentType string) error {
	return fmt.Errorf("StoreFile requires explicit modification time; use StoreFileWithModTime")
}

func decodeForwardedMetadata(metadataJSON []byte) (time.Time, int64, error) {
	var meta map[string]interface{}
	if err := json.Unmarshal(metadataJSON, &meta); err != nil {
		return time.Time{}, 0, err
	}
	var modTime time.Time
	if raw, ok := meta["version"]; ok {
		switch v := raw.(type) {
		case float64:
			if v != 0 {
				modTime = time.Unix(0, int64(v))
			}
		case string:
			if v != "" {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					modTime = time.Unix(0, n)
				} else if t, err := parseTimestamp(v); err == nil {
					modTime = t
				}
			}
		}
	}
	if modTime.IsZero() {
		if raw, ok := meta["modified_at"]; ok {
			switch v := raw.(type) {
			case float64:
				modTime = time.Unix(int64(v), 0)
			case string:
				if v != "" {
					if t, err := parseTimestamp(v); err == nil {
						modTime = t
					}
				}
			}
		}
	}
	size := int64(0)
	if raw, ok := meta["size"]; ok {
		switch v := raw.(type) {
		case float64:
			size = int64(v)
		case string:
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				size = n
			}
		case int64:
			size = v
		case int:
			size = int64(v)
		}
	}
	if modTime.IsZero() {
		return time.Time{}, size, fmt.Errorf("forwarded metadata missing mod time")
	}
	return modTime, size, nil
}

// StoreFileWithModTime stores a file using an explicit modification time
func (fs *ClusterFileSystem) StoreFileWithModTime(path string, content []byte, contentType string, modTime time.Time) error {
	if err := fs.validatePath(path); err != nil {
		return err
	}

	// Create file metadata for the file system layer
	metadata := FileMetadata{
		Name:        filepath.Base(path),
		Path:        path,
		Size:        int64(len(content)),
		ContentType: contentType,
		CreatedAt:   modTime,
		ModifiedAt:  modTime,
		IsDirectory: false,
	}

	// Store file and metadata together in partition system
	// Create enhanced metadata for CRDT
	enhancedMetadata := map[string]interface{}{
		"name":         metadata.Name,
		"path":         metadata.Path,
		"size":         metadata.Size,
		"content_type": metadata.ContentType,
		"created_at":   metadata.CreatedAt.Format(time.RFC3339Nano),
		"modified_at":  modTime.Format(time.RFC3339Nano),
		"is_directory": metadata.IsDirectory,
		"version":      float64(modTime.UnixNano()),
		"deleted":      false,
	}
	metadataJSON, _ := json.Marshal(enhancedMetadata)

	// For no-store clients, forward uploads to storage nodes
	if fs.cluster.NoStore {
		return fs.forwardUploadToStorageNode(path, metadataJSON, content, contentType)
	}

	if err := fs.cluster.PartitionManager.StoreFileInPartition(path, metadataJSON, content); err != nil {
		return logerrf("failed to store file: %v", err)
	}

	// Mirror to OS export directory if configured
	if fs.cluster != nil && fs.cluster.exporter != nil {
		if err := fs.cluster.exporter.WriteFile(path, content, modTime); err != nil {
			fs.cluster.Logger.Printf("[EXPORT] WriteFile mirror failed for %s: %v", path, err)
		}
	}

	return nil
}

// forwardUploadToStorageNode forwards file uploads from no-store clients to storage nodes
func (fs *ClusterFileSystem) forwardUploadToStorageNode(path string, metadataJSON []byte, content []byte, contentType string) error {
	// Find storage nodes that can hold this file
	peers := fs.cluster.DiscoveryManager.GetPeers()
	var storageNodes []*discovery.PeerInfo

	// Only forward to peers that are not no-store clients
	// We can't easily detect if a peer is no-store, so try all peers
	storageNodes = append(storageNodes, peers...)

	if len(storageNodes) == 0 {
		return fmt.Errorf("no storage nodes available to forward upload")
	}

	// Try forwarding to storage nodes until one succeeds
	var lastErr error
	forwarded := false
	skippedPeers := 0

	modTime, size, metaErr := decodeForwardedMetadata(metadataJSON)

	for _, peer := range storageNodes {
		if metaErr == nil {
			upToDate, err := fs.peerHasUpToDateFile(peer, path, modTime, size)
			if err == nil && upToDate {
				fs.cluster.Logger.Printf("[FILES] Peer %s already has %s (mod >= %s); skipping forward", peer.NodeID, path, modTime.Format(time.RFC3339Nano))
				skippedPeers++
				continue
			}
			if err != nil {
				fs.cluster.Logger.Printf("[FILES] HEAD check failed for %s on %s: %v", path, peer.NodeID, err)
			}
		}

		fileURL, err := urlutil.BuildFilesURL(peer.Address, peer.HTTPPort, path)
		if err != nil {
			lastErr = err
			continue
		}

		req, err := http.NewRequest(http.MethodPut, fileURL, bytes.NewReader(content))
		if err != nil {
			lastErr = err
			continue
		}

		req.Header.Set("Content-Type", contentType)
		req.Header.Set("X-Forwarded-From", string(fs.cluster.ID))
		req.Header.Set("X-ClusterF-Metadata", base64.StdEncoding.EncodeToString(metadataJSON))

		resp, err := fs.cluster.httpDataClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusCreated {
			fs.cluster.Logger.Printf("[FILES] Forwarded upload %s to %s", path, peer.NodeID)
			forwarded = true
			return nil // Success
		}

		lastErr = fmt.Errorf("peer %s returned %d", peer.NodeID, resp.StatusCode)
	}

	if forwarded || skippedPeers == len(storageNodes) {
		return nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no storage nodes accepted upload")
	}

	return fmt.Errorf("failed to forward upload to any storage node: %v", lastErr)
}

// GetFileWithContentType retrieves a file and returns an io.ReadCloser with content type
func (fs *ClusterFileSystem) GetFileWithContentType(path string) (io.ReadCloser, string, error) {
	content, metadata, err := fs.GetFile(path)
	if err != nil {
		return nil, "", err
	}
	
	return io.NopCloser(bytes.NewReader(content)), metadata.ContentType, nil
}

// GetFile retrieves a file from the partition system
func (fs *ClusterFileSystem) GetFile(path string) ([]byte, *FileMetadata, error) {
	// Get file content and metadata together
	content, metadataMap, err := fs.cluster.PartitionManager.GetFileAndMetaFromPartition(path)
	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			return nil, nil, ErrFileNotFound
		}
		return nil, nil, err
	}

	// Check if file is marked as deleted
	if deleted, ok := metadataMap["deleted"].(bool); ok && deleted {
		return nil, nil, ErrFileNotFound
	}

	// Convert metadata map to struct
	metadata := &FileMetadata{}
	if name, ok := metadataMap["name"].(string); ok {
		metadata.Name = name
	}
	if path, ok := metadataMap["path"].(string); ok {
		metadata.Path = path
	}
	if sizeFloat, ok := metadataMap["size"].(float64); ok {
		metadata.Size = int64(sizeFloat)
	}
	if contentType, ok := metadataMap["content_type"].(string); ok {
		metadata.ContentType = contentType
	}
	if createdVal, ok := metadataMap["created_at"]; ok {
		switch v := createdVal.(type) {
		case string:
			if t, err := parseTimestamp(v); err == nil {
				metadata.CreatedAt = t
			}
		case float64:
			metadata.CreatedAt = time.Unix(int64(v), 0)
		}
	}
	if modifiedVal, ok := metadataMap["modified_at"]; ok {
		switch v := modifiedVal.(type) {
		case string:
			if t, err := parseTimestamp(v); err == nil {
				metadata.ModifiedAt = t
			}
		case float64:
			metadata.ModifiedAt = time.Unix(int64(v), 0)
		}
	}
	if isDir, ok := metadataMap["is_directory"].(bool); ok {
		metadata.IsDirectory = isDir
	}
	if childrenIface, ok := metadataMap["children"].([]interface{}); ok {
		for _, c := range childrenIface {
			if cstr, ok := c.(string); ok {
				metadata.Children = append(metadata.Children, cstr)
			}
		}
	}

	if metadata.IsDirectory {
		return nil, nil, ErrIsDirectory
	}

	return content, metadata, nil
}

// ListDirectory lists the contents of a directory using search API
func (fs *ClusterFileSystem) ListDirectory(path string) ([]*FileMetadata, error) {
	return fs.cluster.ListDirectoryUsingSearch(path)
}

// DeleteFile removes a file from the cluster
func (fs *ClusterFileSystem) DeleteFile(path string) error {
	metadata, err := fs.getMetadata(path)
	if err != nil {
		return err
	}

	if metadata.IsDirectory {
		// Check if directory is empty
		children, err := fs.ListDirectory(path)
		if err != nil {
			return err
		}
		if len(children) > 0 {
			return fmt.Errorf("directory not empty")
		}
	}

	// Delete from partition system
	if err := fs.cluster.PartitionManager.DeleteFileFromPartition(path); err != nil {
		return fmt.Errorf("failed to delete file: %v", err)
	}

	// Directory updates are no longer needed since we use search-based directory listing

	// Mirror delete to OS export directory if configured
	if fs.cluster != nil && fs.cluster.exporter != nil {
		if metadata.IsDirectory {
			if err := fs.cluster.exporter.RemoveDir(path); err != nil {
				fs.cluster.Logger.Printf("[EXPORT] RemoveDir mirror failed for %s: %v", path, err)
			}
		} else {
			if err := fs.cluster.exporter.RemoveFile(path); err != nil {
				fs.cluster.Logger.Printf("[EXPORT] RemoveFile mirror failed for %s: %v", path, err)
			}
		}
	}

	return nil
}

// Helper functions

func parseTimestamp(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}
	if secs, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Unix(secs, 0), nil
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp: %s", value)
}

func (fs *ClusterFileSystem) validatePath(path string) error {
	if len(path) > MaxPathLen {
		return fmt.Errorf("path too long")
	}
	if strings.Contains(path, "..") {
		return fmt.Errorf("invalid path")
	}
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("path must be absolute")
	}
	name := filepath.Base(path)
	if len(name) > MaxFileNameLen {
		return fmt.Errorf("filename too long")
	}
	return nil
}

func (fs *ClusterFileSystem) getMetadata(path string) (*FileMetadata, error) {
	// Try to get metadata from partition system
	metadataMap, err := fs.cluster.PartitionManager.GetMetadataFromPartition(path)
	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			return nil, ErrFileNotFound
		}
		return nil, err
	}

	// Check if file is marked as deleted
	if deleted, ok := metadataMap["deleted"].(bool); ok && deleted {
		return nil, ErrFileNotFound
	}

	// Convert metadata map to struct
	var metadata FileMetadata
	metadata.Name, _ = metadataMap["name"].(string)
	metadata.Path, _ = metadataMap["path"].(string)
	if sizeFloat, ok := metadataMap["size"].(float64); ok {
		metadata.Size = int64(sizeFloat)
	}
	metadata.ContentType, _ = metadataMap["content_type"].(string)
	if createdVal, ok := metadataMap["created_at"]; ok {
		switch v := createdVal.(type) {
		case string:
			if t, err := parseTimestamp(v); err == nil {
				metadata.CreatedAt = t
			}
		case float64:
			metadata.CreatedAt = time.Unix(int64(v), 0)
		}
	}
	if modifiedVal, ok := metadataMap["modified_at"]; ok {
		switch v := modifiedVal.(type) {
		case string:
			if t, err := parseTimestamp(v); err == nil {
				metadata.ModifiedAt = t
			}
		case float64:
			metadata.ModifiedAt = time.Unix(int64(v), 0)
		}
	}
	if isDir, ok := metadataMap["is_directory"].(bool); ok {
		metadata.IsDirectory = isDir
	}
	if childrenIface, ok := metadataMap["children"].([]interface{}); ok {
		for _, c := range childrenIface {
			if cstr, ok := c.(string); ok {
				metadata.Children = append(metadata.Children, cstr)
			}
		}
	}

	return &metadata, nil
}

// MetadataForPath adapts internal metadata to the exporter module's format.
func (fs *ClusterFileSystem) MetadataForPath(path string) (*exporter.Metadata, error) {
	meta, err := fs.getMetadata(path)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, nil
	}
	return &exporter.Metadata{
		Size:        meta.Size,
		ModifiedAt:  meta.ModifiedAt,
		IsDirectory: meta.IsDirectory,
	}, nil
}

// CreateDirectory is a no-op since directories are inferred from file paths
func (fs *ClusterFileSystem) CreateDirectory(path string) error {
	return nil // Directories are inferred from file paths
}

// CreateDirectoryWithModTime is a no-op since directories are inferred from file paths
func (fs *ClusterFileSystem) CreateDirectoryWithModTime(path string, modTime time.Time) error {
	return nil // Directories are inferred from file paths
}

func (fs *ClusterFileSystem) peerHasUpToDateFile(peer *discovery.PeerInfo, path string, modTime time.Time, size int64) (bool, error) {
	fileURL, err := urlutil.BuildFilesURL(peer.Address, peer.HTTPPort, path)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequest(http.MethodHead, fileURL, nil)
	if err != nil {
		return false, err
	}
	resp, err := fs.cluster.httpDataClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound:
		return false, nil
	case http.StatusOK:
		remoteMod := time.Time{}
		if lm := resp.Header.Get("Last-Modified"); lm != "" {
			if t, err := time.Parse(http.TimeFormat, lm); err == nil {
				remoteMod = t
			}
		}
		if remoteMod.IsZero() {
			if alt := resp.Header.Get("X-ClusterF-Created-At"); alt != "" {
				if t, err := time.Parse(time.RFC3339, alt); err == nil {
					remoteMod = t
				}
			}
		}
		remoteSize := int64(-1)
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			if n, err := strconv.ParseInt(cl, 10, 64); err == nil {
				remoteSize = n
			}
		}
		if !remoteMod.IsZero() && !remoteMod.Before(modTime) {
			if size <= 0 || remoteSize == size {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
}
