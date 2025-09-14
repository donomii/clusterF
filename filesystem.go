// filesystem.go - Distributed file system layer on top of partition system
package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
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
	mu      sync.RWMutex
}

// NewClusterFileSystem creates a new distributed file system
func NewClusterFileSystem(cluster *Cluster) *ClusterFileSystem {
	return &ClusterFileSystem{
		cluster: cluster,
	}
}

// StoreFile stores a file in the cluster using the partition system
func (fs *ClusterFileSystem) StoreFile(path string, content []byte, contentType string) error {
	return fs.StoreFileWithModTime(path, content, contentType, time.Now())
}

// StoreFileWithModTime stores a file using an explicit modification time
func (fs *ClusterFileSystem) StoreFileWithModTime(path string, content []byte, contentType string, modTime time.Time) error {
	if err := fs.validatePath(path); err != nil {
		return err
	}

	// Ensure parent directories exist
	if err := fs.ensureDirectoryPath(filepath.Dir(path)); err != nil {
		return err
	}

	// Preserve CreatedAt if file exists
	var createdAt time.Time
	if oldMeta, err := fs.getMetadata(path); err == nil {
		createdAt = oldMeta.CreatedAt
	}
	if createdAt.IsZero() {
		createdAt = modTime
	}

	// Create file metadata for the file system layer
	metadata := FileMetadata{
		Name:        filepath.Base(path),
		Path:        path,
		Size:        int64(len(content)),
		ContentType: contentType,
		CreatedAt:   createdAt,
		ModifiedAt:  modTime,
		IsDirectory: false,
	}

	// Store file and metadata together in partition system
	metadataJSON, _ := json.Marshal(metadata)
	if err := fs.cluster.PartitionManager.storeFileInPartition(path, metadataJSON, content); err != nil {
		return logerrf("failed to store file: %v", err)
	}

	// Update parent directory
	if err := fs.addToDirectory(filepath.Dir(path), filepath.Base(path)); err != nil {
		return logerrf("failed to update directory: %v", err)
	}

	// Mirror to OS export directory if configured
	if fs.cluster != nil && fs.cluster.exporter != nil {
		if err := fs.cluster.exporter.WriteFile(path, content, modTime); err != nil {
			fs.cluster.Logger.Printf("[EXPORT] WriteFile mirror failed for %s: %v", path, err)
		}
	}

	return nil
}

// GetFile retrieves a file from the partition system
func (fs *ClusterFileSystem) GetFile(path string) ([]byte, *FileMetadata, error) {
	// Get file content and metadata together
	content, metadataMap, err := fs.cluster.PartitionManager.getFileAndMetaFromPartition(path)
	if err != nil {
		return nil, nil, err
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
	if createdStr, ok := metadataMap["created_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdStr); err == nil {
			metadata.CreatedAt = t
		}
	}
	if modifiedStr, ok := metadataMap["modified_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, modifiedStr); err == nil {
			metadata.ModifiedAt = t
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
		return nil, nil, fmt.Errorf("path is a directory")
	}

	return content, metadata, nil
}

// ListDirectory lists the contents of a directory
func (fs *ClusterFileSystem) ListDirectory(path string) ([]*FileMetadata, error) {
	if path == "" {
		path = "/"
	}

	// Normalize path
	path = filepath.Clean(path)
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	// Get directory metadata
	metadata, err := fs.getMetadata(path)
	if err != nil {
		// If root directory doesn't exist, return empty listing
		if path == "/" {
			return []*FileMetadata{}, nil
		}
		return nil, err
	}

	if !metadata.IsDirectory {
		return nil, fmt.Errorf("path is not a directory")
	}

	// Get metadata for each child
	var results []*FileMetadata
	for _, childName := range metadata.Children {
		childPath := filepath.Join(path, childName)
		childMeta, err := fs.getMetadata(childPath)
		if err != nil {
			// Skip missing children
			continue
		}
		results = append(results, childMeta)
	}

	// Sort by name
	sort.Slice(results, func(i, j int) bool {
		// Directories first, then files
		if results[i].IsDirectory != results[j].IsDirectory {
			return results[i].IsDirectory
		}
		return results[i].Name < results[j].Name
	})

	return results, nil
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
	if err := fs.cluster.PartitionManager.deleteFileFromPartition(path); err != nil {
		return fmt.Errorf("failed to delete file: %v", err)
	}

	// Remove from parent directory
	if err := fs.removeFromDirectory(filepath.Dir(path), filepath.Base(path)); err != nil {
		return logerrf("failed to update directory: %v", err)
	}

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

// CreateDirectory creates a new directory
func (fs *ClusterFileSystem) CreateDirectory(path string) error {
	return fs.CreateDirectoryWithModTime(path, time.Now())
}

// CreateDirectoryWithModTime creates a directory with specified modification time
func (fs *ClusterFileSystem) CreateDirectoryWithModTime(path string, modTime time.Time) error {
	if err := fs.validatePath(path); err != nil {
		return err
	}

	// Check if already exists
	if _, err := fs.getMetadata(path); err == nil {
		return fmt.Errorf("path already exists")
	}

	// Ensure parent directories exist
	if err := fs.ensureDirectoryPath(filepath.Dir(path)); err != nil {
		return err
	}

	// Create directory metadata
	metadata := FileMetadata{
		Name:        filepath.Base(path),
		Path:        path,
		Size:        0,
		IsDirectory: true,
		Children:    []string{},
		CreatedAt:   modTime,
		ModifiedAt:  modTime,
	}

	// Store directory metadata in partition system
	metadataJSON, _ := json.Marshal(metadata)
	if err := fs.cluster.PartitionManager.storeFileInPartition(path, metadataJSON, []byte{}); err != nil {
		return logerrf("failed to store directory metadata: %v", err)
	}

	// Update parent directory
	if err := fs.addToDirectory(filepath.Dir(path), filepath.Base(path)); err != nil {
		return logerrf("failed to update parent directory: %v", err)
	}

	// Mirror to OS export directory if configured
	if fs.cluster != nil && fs.cluster.exporter != nil {
		if err := fs.cluster.exporter.MkdirWithModTime(path, modTime); err != nil {
			fs.cluster.Logger.Printf("[EXPORT] Mkdir mirror failed for %s: %v", path, err)
		}
	}

	return nil
}

// Helper functions

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
	_, metadataMap, err := fs.cluster.PartitionManager.getFileAndMetaFromPartition(path)
	if err != nil {
		return nil, fmt.Errorf("file not found")
	}

	// Convert metadata map to struct
	var metadata FileMetadata
	metadata.Name, _ = metadataMap["name"].(string)
	metadata.Path, _ = metadataMap["path"].(string)
	if sizeFloat, ok := metadataMap["size"].(float64); ok {
		metadata.Size = int64(sizeFloat)
	}
	metadata.ContentType, _ = metadataMap["content_type"].(string)
	if createdStr, ok := metadataMap["created_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdStr); err == nil {
			metadata.CreatedAt = t
		}
	}
	if modifiedStr, ok := metadataMap["modified_at"].(string); ok {
		if t, err := time.Parse(time.RFC3339, modifiedStr); err == nil {
			metadata.ModifiedAt = t
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

func (fs *ClusterFileSystem) ensureDirectoryPath(path string) error {
	if path == "/" || path == "." {
		return fs.ensureRootDirectory()
	}

	// Check if directory exists
	if _, err := fs.getMetadata(path); err == nil {
		return nil // Already exists
	}

	// Ensure parent exists first
	parent := filepath.Dir(path)
	if err := fs.ensureDirectoryPath(parent); err != nil {
		return err
	}

	// Create this directory
	return fs.CreateDirectory(path)
}

func (fs *ClusterFileSystem) ensureRootDirectory() error {
	if _, err := fs.getMetadata("/"); err == nil {
		return nil // Root exists
	}

	// Create root directory
	metadata := FileMetadata{
		Name:        "/",
		Path:        "/",
		Size:        0,
		IsDirectory: true,
		Children:    []string{},
		CreatedAt:   time.Now(),
		ModifiedAt:  time.Now(),
	}

	// Store root metadata in partition system
	metadataJSON, _ := json.Marshal(metadata)
	return fs.cluster.PartitionManager.storeFileInPartition("/", metadataJSON, []byte{})
}

func (fs *ClusterFileSystem) addToDirectory(dirPath, childName string) error {
	if dirPath == "." {
		dirPath = "/"
	}

	metadata, err := fs.getMetadata(dirPath)
	if err != nil {
		return err
	}

	if !metadata.IsDirectory {
		return fmt.Errorf("not a directory")
	}

	// Add child if not already present
	for _, existing := range metadata.Children {
		if existing == childName {
			return nil // Already present
		}
	}

	metadata.Children = append(metadata.Children, childName)
	metadata.ModifiedAt = time.Now()

	// Update directory metadata in partition system
	metadataJSON, _ := json.Marshal(*metadata)
	return fs.cluster.PartitionManager.storeFileInPartition(dirPath, metadataJSON, []byte{})
}

func (fs *ClusterFileSystem) removeFromDirectory(dirPath, childName string) error {
	if dirPath == "." {
		dirPath = "/"
	}

	metadata, err := fs.getMetadata(dirPath)
	if err != nil {
		return err
	}

	if !metadata.IsDirectory {
		return fmt.Errorf("not a directory")
	}

	// Remove child
	var newChildren []string
	for _, existing := range metadata.Children {
		if existing != childName {
			newChildren = append(newChildren, existing)
		}
	}

	metadata.Children = newChildren
	metadata.ModifiedAt = time.Now()

	// Update directory metadata in partition system
	metadataJSON, _ := json.Marshal(*metadata)
	return fs.cluster.PartitionManager.storeFileInPartition(dirPath, metadataJSON, []byte{})
}
