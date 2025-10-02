package types

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"
)

// Shared sentinel errors for filesystem operations.
var (
	// ErrFileNotFound indicates that the requested file does not exist in any known partition.
	ErrFileNotFound = errors.New("file not found")
	ErrIsDirectory  = errors.New("path is a directory")
)

type ClusterLike interface {
	PartitionManager() PartitionManagerLike
	DiscoveryManager() DiscoveryManagerLike
	Exporter() ExporterLike
	Logger() *log.Logger
	NoStore() bool
	ListDirectoryUsingSearch(path string) ([]*FileMetadata, error)
	DataClient() *http.Client
	ID() NodeID
	GetAllNodes() map[NodeID]*NodeData
	GetNodesForPartition(partitionName string) []NodeID
	GetNodeInfo(nodeID NodeID) *NodeData
}

type PartitionManagerLike interface {
	StoreFileInPartition(path string, metadataJSON []byte, fileContent []byte) error
	GetFileAndMetaFromPartition(path string) ([]byte, map[string]interface{}, error)
	DeleteFileFromPartition(path string) error
	GetMetadataFromPartition(path string) (map[string]interface{}, error)
	CalculatePartitionName(path string) string
}

type DiscoveryManagerLike interface {

	// GetPeers returns a list of all known peers
	GetPeers() []*PeerInfo

	SetTimings(broadcastInterval, peerTimeout time.Duration)

	UpdateNodeInfo(nodeID string, httpPort int)

	// Start begins the discovery process
	Start() error

	// Stop halts the discovery process
	Stop()

	GetPeerCount() int
}

type ExporterLike interface {
	WriteFile(clusterPath string, data []byte, modTime time.Time) error
	RemoveDir(clusterPath string) error
	RemoveFile(clusterPath string) error
	Run(ctx context.Context)
}

// FileSystem defines the file-system operations the exporter relies on.
type FileSystemLike interface {
	CreateDirectory(path string) error
	CreateDirectoryWithModTime(path string, modTime time.Time) error
	StoreFileWithModTime(path string, data []byte, contentType string, modTime time.Time) error
	DeleteFile(path string) error
	MetadataForPath(path string) (*FileMetadata, error)
	// Additional methods for WebDAV support
	GetFile(path string) ([]byte, *FileMetadata, error)
	ListDirectory(path string) ([]*FileMetadata, error)
}

// PeerInfo represents information about a discovered peer
type PeerInfo struct {
	NodeID      string    `json:"node_id"`
	HTTPPort    int       `json:"http_port"`
	Address     string    `json:"address"`
	LastSeen    time.Time `json:"last_seen"`
	BytesStored int64     `json:"bytes_stored,omitempty"`
	DiskSize    int64     `json:"disk_size,omitempty"`
	DiskFree    int64     `json:"disk_free,omitempty"`
	Available   bool      `json:"available,omitempty"`
	IsStorage   bool      `json:"is_storage,omitempty"`
}

type NodeInfo struct {
	NodeID   string
	Address  string
	HTTPPort int
}

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
	Checksum    string    `json:"checksum,omitempty"` // SHA-256 hash in hex format
}

type NodeData struct {
	NodeID      string `json:"node_id"`
	Address     string `json:"address"`
	HTTPPort    int    `json:"http_port"`
	LastSeen    int64  `json:"last_seen"`
	Available   bool   `json:"available"`
	BytesStored int64  `json:"bytes_stored"`
	DiskSize    int64  `json:"disk_size"`
	DiskFree    int64  `json:"disk_free"`
	IsStorage   bool   `json:"is_storage"`
}

type NodeID string

// types.SearchResult represents a search result entry
type SearchResult struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	Size        int64  `json:"size,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	ModifiedAt  int64  `json:"modified_at,omitempty"`
	Checksum    string `json:"checksum,omitempty"`
}

// collapsetypes.SearchResults collapses search results into directories and files
func CollapseSearchResults(raw_results []SearchResult, basePath string) []SearchResult {
	results := make([]SearchResult, 0, len(raw_results))
	seen := make(map[string]bool)
	for _, res := range raw_results {
		// Clip off the prefix path
		relPath := strings.TrimPrefix(res.Path, basePath)
		//Take the string up to the first /
		parts := strings.SplitN(relPath, "/", 2)
		if len(parts) > 1 {
			if parts[0] != "" {
				dir := parts[0] + "/"
				//c.debugf("[SEARCH] Processing result: %s (rel: %s) is a directory", dir, relPath)
				if !seen[dir] {
					seen[dir] = true
					results = append(results, SearchResult{
						Name: dir,
						Path: basePath + dir,
					})
				}
			}
		} else {
			// It's a file in the current directory
			//c.debugf("[SEARCH] Processing result: %s (rel: %s) is a file", relPath, relPath)
			if !seen[relPath] {
			seen[relPath] = true //Could get multiple files with same name from different peers
			results = append(results, SearchResult{
			Name:        relPath,
			Path:        res.Path,
			Size:        res.Size,
			ContentType: res.ContentType,
			ModifiedAt:  res.ModifiedAt,
			 Checksum:    res.Checksum,
			 })
				}
		}
	}
	return results
}
