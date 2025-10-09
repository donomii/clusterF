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

// A cluster node
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

// The partition manager, everything needed to access partitions and files
// The FileStore should not be accessed directly, except by partitionManager
type PartitionManagerLike interface {
	StoreFileInPartition(path string, metadataJSON []byte, fileContent []byte) error // Store file in appropriate partition based on path, does not send to network
	GetFileAndMetaFromPartition(path string) ([]byte, FileMetadata, error)           // Get file and metadata from partition, including from other nodes
	DeleteFileFromPartition(path string) error                                       // Delete file from partition, does not send to network
	GetMetadataFromPartition(path string) (FileMetadata, error)                      // Get file metadata from partition, including from other nodes
	CalculatePartitionName(path string) string                                       // Calculate partition name for a given path
	ScanAllFiles(fn func(filePath string, metadata FileMetadata) error) error        // Scan all files in all partitions, calling fn for each file

}

// Handles discovering peers on the network
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

// Handles importing and exporting data files to/from disk
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
	MetadataForPath(path string) (FileMetadata, error)
	// Additional methods for WebDAV support
	GetFile(path string) ([]byte, FileMetadata, error)
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
	Checksum    string    `json:"checksum,omitempty"` // SHA-256 hash in hex format
	Deleted     bool      `json:"deleted,omitempty"`
	DeletedAt   time.Time `json:"deleted_at,omitempty"`
}

type NodeData struct {
	NodeID      string    `json:"node_id"`
	Address     string    `json:"address"`
	HTTPPort    int       `json:"http_port"`
	LastSeen    time.Time `json:"last_seen"`
	Available   bool      `json:"available"`
	BytesStored int64     `json:"bytes_stored"`
	DiskSize    int64     `json:"disk_size"`
	DiskFree    int64     `json:"disk_free"`
	IsStorage   bool      `json:"is_storage"`
}

type NodeID string

// SearchResult is a search result entry
type SearchResult struct {
	Name        string    `json:"name"`
	Path        string    `json:"path"`
	Size        int64     `json:"size,omitempty"`
	ContentType string    `json:"content_type,omitempty"`
	ModifiedAt  time.Time `json:"modified_at,omitempty"`
	CreatedAt   time.Time `json:"created_at,omitempty"`
	Checksum    string    `json:"checksum,omitempty"`
}

// Sent to the browser for display
type PartitionStatistics struct {
	Local_partitions      int `json:"local_partitions"`
	Total_partitions      int `json:"total_partitions"`
	Under_replicated      int `json:"under_replicated"`
	Pending_sync          int `json:"pending_sync"`
	Replication_factor    int `json:"replication_factor"`
	Total_files           int `json:"total_files"`
	Partition_count_limit int `json:"partition_count_limit"`
}

// Sent to the browser for display
type NodeStatus struct {
	Node_id            string              `json:"node_id"`
	Data_dir           string              `json:"data_dir"`
	Http_port          int                 `json:"http_port"`
	Replication_factor int                 `json:"replication_factor"`
	Partition_stats    PartitionStatistics `json:"partition_stats"`
	Current_file       string              `json:"current_file"`
	Discovery_port     int                 `json:"discovery_port"`
	Timestamp          time.Time           `json:"timestamp"`
	Peer_list          []PeerInfo          `json:"peer_list"`
}

// Monitor transcoding
type TranscodeStatistics struct {
	TotalEntries int    `json:"total_entries"`
	TotalSize    int    `json:"total_size"`
	MaxSize      int    `json:"max_size"`
	InProgress   int    `json:"in_progress"`
	CacheDir     string `json:"cache_dir"`
}

func CollapseToDirectory(relPath, basePath string) string {
	// Clip off the prefix path
	relPath = strings.TrimPrefix(relPath, basePath)
	//Take the string up to the first /
	parts := strings.SplitN(relPath, "/", 2)
	if len(parts) > 1 {
		if parts[0] != "" {
			dir := parts[0] + "/"
			return dir
		}
	}

	return relPath
}

// CollapseSearchResults collapses search results into directories and files
func CollapseSearchResults(raw_results []SearchResult, searchPath string) []SearchResult {
	results := make(map[string]SearchResult, len(raw_results))
	for _, res := range raw_results {
		norm_res := CollapseToDirectory(res.Path, searchPath)
		res.Name = norm_res
		AddResultToMap(res, results, searchPath)
	}

	resultList := make([]SearchResult, 0, len(results))
	for _, result := range results {
		resultList = append(resultList, result)
	}
	return resultList
}

func AddResultToMap(result SearchResult, resultMap map[string]SearchResult, searchPath string) {
	normPath := CollapseToDirectory(result.Path, searchPath)
	var newResult SearchResult
	if strings.HasSuffix(normPath, "/") {
		// It's a directory
		newResult = SearchResult{
			Name:       normPath,
			Path:       searchPath + normPath,
			ModifiedAt: result.ModifiedAt,
		}
	} else {
		// It's a file in the current directory
		newResult = SearchResult{
			Name:        normPath,
			Path:        result.Path,
			Size:        result.Size,
			ContentType: result.ContentType,
			ModifiedAt:  result.ModifiedAt,
			Checksum:    result.Checksum,
		}
	}
	if newResult.ModifiedAt.IsZero() {
		panic("no")
	}
	if existing, ok := resultMap[normPath]; ok {
		// Merge logic: keep the one with the latest ModifiedAt
		if newResult.ModifiedAt.After(existing.ModifiedAt) {
			resultMap[normPath] = newResult
		}
	} else {
		resultMap[normPath] = newResult
	}
}
