package types

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/frogpond"
)

const DefaultPartitionSyncIntervalSeconds = 300

type DiskActivityLevel int

const (
	DiskActivityNonEssential DiskActivityLevel = iota
	DiskActivityEssential
)

// Shared sentinel errors for filesystem operations.
var (
	// ErrFileNotFound indicates that the requested file does not exist in any known partition.
	ErrFileNotFound = errors.New("file not found")
	ErrIsDirectory  = errors.New("path is a directory")
)

// A cluster node
type ClusterLike interface {
	PartitionManager() PartitionManagerLike                        // Return the Partition Manager of this Node
	DiscoveryManager() DiscoveryManagerLike                        // Return the Discovery Manager of this Node
	Exporter() ExporterLike                                        // Return the Exporter of this Node
	Logger() *log.Logger                                           // Return the logger for this node
	ReplicationFactor() int                                        // Current replication factor
	NoStore() bool                                                 // Return true if this node does not participate in data storage
	ListDirectoryUsingSearch(path string) ([]*FileMetadata, error) // Search the cluster for a prefix (a directory)
	DataClient() *http.Client                                      // The data transport client, at the moment, there is only the HTTP protocol client
	ID() NodeID                                                    // Node id (a string)
	GetAllNodes() map[NodeID]*NodeData                             // Return a copy of all known nodes
	GetNodesForPartition(partitionName string) []NodeID            // Get all node that hold the given partition
	GetNodeInfo(nodeID NodeID) *NodeData                           // Get info about a specific node
	GetPartitionSyncPaused() bool                                  // Partition sync activity
	AppContext() context.Context                                   //Closed when the application shuts down
	RecordDiskActivity(level DiskActivityLevel)                    // Track essential/non-essential disk activity
	CanRunNonEssentialDiskOp() bool                                // Whether non-essential disk operations are allowed right now
	LoadPeer(id NodeID) (*PeerInfo, bool)                          // Load peer info from CRDT or Discovery
}

// The partition manager, everything needed to access partitions and files
// The FileStore should not be accessed directly, except by partitionManager
type PartitionManagerLike interface {
	StoreFileInPartition(ctx context.Context, path string, metadataJSON []byte, fileContent []byte) error // Store file in appropriate partition based on path, does not send to network
	GetFileAndMetaFromPartition(path string) ([]byte, FileMetadata, error)                                // Get file and metadata from partition, including from other nodes
	DeleteFileFromPartition(ctx context.Context, path string) error                                       // Delete file from partition, does not send to network
	DeleteFileFromPartitionWithTimestamp(ctx context.Context, path string, modTime time.Time) error       // Delete file from partition with explicit timestamp
	GetMetadataFromPartition(path string) (FileMetadata, error)                                           // Get file metadata from partition
	GetMetadataFromPeers(path string) (FileMetadata, error)                                               // Get file metadata from other nodes
	GetFileFromPeers(path string) ([]byte, FileMetadata, error)
	CalculatePartitionName(path string) string                                // Calculate partition name for a given path
	ScanAllFiles(fn func(filePath string, metadata FileMetadata) error) error // Scan all files in all partitions, calling fn for each file
	GetPartitionInfo(partitionID PartitionID) *PartitionInfo
	RunReindex(ctx context.Context)
	MarkForReindex(pId PartitionID)
	RemoveNodeFromPartitionWithTimestamp(nodeID NodeID, partitionName string, backdatedTime time.Time) error // Remove a node from a partition holder list with backdated timestamp
}

// FileStoreLike abstracts the storage layer used by the partition manager.
type FileStoreLike interface {
	Close()
	SetEncryptionKey(key []byte)
	Get(path string) ([]byte, []byte, bool, error) //metadata, content, exists, error
	GetMetadata(path string) ([]byte, error)
	GetContent(path string) ([]byte, error)
	Put(path string, metadata, content []byte) error
	PutMetadata(path string, metadata []byte) error
	Delete(path string) error
	Scan(pathPrefix string, fn func(path string, metadata, content []byte) error) error
	ScanMetadata(pathPrefix string, fn func(path string, metadata []byte) error) error
	ScanMetadataPartition(ctx context.Context, partitionID PartitionID, fn func(path string, metadata []byte) error) error
	CalculatePartitionChecksum(ctx context.Context, partitionID PartitionID) (string, error)
	GetAllPartitionStores() ([]PartitionStore, error)
}

type PartitionInfo struct {
	ID         PartitionID           `json:"id"`
	FileCount  int                   `json:"file_count"`
	Holders    []NodeID              `json:"holders"`
	Checksums  map[NodeID]string     `json:"checksums"`
	HolderData map[NodeID]HolderData `json:"holder_data"`
}

// Handles discovering peers on the network
type DiscoveryManagerLike interface {

	// GetPeers returns a list of all known peers
	GetPeers() []*PeerInfo
	GetPeerMap() *syncmap.SyncMap[string, *PeerInfo]
	// AddPeer adds a new peer to the discovery manager. It will not add if it

	SetTimings(broadcastInterval, peerTimeout time.Duration)

	UpdateNodeInfo(nodeID string, httpPort int)

	// Start begins the discovery process
	Start() error

	// Stop halts the discovery process
	Stop()

	GetPeerCount() int

	GetLocalAddress() string
}

// Handles importing and exporting data files to/from disk
type ExporterLike interface {
	WriteFile(clusterPath string, data []byte, modTime time.Time) error
	RemoveDir(clusterPath string) error
	RemoveFile(clusterPath string) error
	Run(ctx context.Context)
}

// Indexer maintains an in-memory index of files for fast searching
type IndexerLike interface {
	// PrefixSearch returns all files matching a path prefix
	PrefixSearch(prefix string) []SearchResult
	// AddFile adds or updates a file in the index
	AddFile(path string, metadata FileMetadata)
	// DeleteFile removes a file from the index
	DeleteFile(path string)
	// FilesForPartition returns the list of paths tracked for a partition
	FilesForPartition(partitionID PartitionID) []string
	// ImportFilestore imports all files from a FileStoreLike into the index
	ImportFilestore(ctx context.Context, pm PartitionManagerLike) error
}

// FileSystem defines the file-system operations the exporter relies on.
type FileSystemLike interface {
	CreateDirectory(path string) error
	CreateDirectoryWithModTime(path string, modTime time.Time) error
	StoreFileWithModTimeDirect(ctx context.Context, path string, data []byte, contentType string, modTime time.Time) (NodeID, error)
	DeleteFile(ctx context.Context, path string) error
	DeleteFileWithTimestamp(ctx context.Context, path string, modTime time.Time) error
	MetadataForPath(path string) (FileMetadata, error)
	MetadataViaAPI(ctx context.Context, path string) (FileMetadata, error)
	// Additional methods for WebDAV support
	GetFile(path string) ([]byte, FileMetadata, error)
	ListDirectory(path string) ([]*FileMetadata, error)
	InsertFileIntoCluster(ctx context.Context, path string, content []byte, contentType string, modTime time.Time) (NodeID, error)
}

// PeerInfo represents information about a discovered peer
type PeerInfo struct {
	NodeID        NodeID    `json:"node_id"`
	HTTPPort      int       `json:"http_port"`
	Address       string    `json:"address"`
	LastSeen      time.Time `json:"last_seen"`
	BytesStored   int64     `json:"bytes_stored,omitempty"`
	DiskSize      int64     `json:"disk_size,omitempty"`
	DiskFree      int64     `json:"disk_free,omitempty"`
	Available     bool      `json:"available,omitempty"`
	IsStorage     bool      `json:"is_storage,omitempty"`
	DiscoveryPort int       `json:"discovery_port,omitempty"`
	DataDir       string    `json:"data_dir,omitempty"`
	StorageFormat string    `json:"storage_format,omitempty"`
	StorageMinor  string    `json:"storage_minor,omitempty"`
	Program       string    `json:"program,omitempty"`
	Version       string    `json:"version,omitempty"`
	URL           string    `json:"url,omitempty"`
	ExportDir     string    `json:"export_dir,omitempty"`
	ClusterDir    string    `json:"cluster_dir,omitempty"`
	ImportDir     string    `json:"import_dir,omitempty"`
	Debug         bool      `json:"debug,omitempty"`
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
	NodeID        string    `json:"node_id"`
	Address       string    `json:"address"`
	HTTPPort      int       `json:"http_port"`
	DiscoveryPort int       `json:"discovery_port"`
	LastSeen      time.Time `json:"last_seen"`
	Available     bool      `json:"available"`
	BytesStored   int64     `json:"bytes_stored"`
	DiskSize      int64     `json:"disk_size"`
	DiskFree      int64     `json:"disk_free"`
	IsStorage     bool      `json:"is_storage"`
	DataDir       string    `json:"data_dir"`
	StorageFormat string    `json:"storage_format"`
	StorageMinor  string    `json:"storage_minor"`
	Program       string    `json:"program"`
	Version       string    `json:"version"`
	URL           string    `json:"url"`
	ExportDir     string    `json:"export_dir"`
	ClusterDir    string    `json:"cluster_dir"`
	ImportDir     string    `json:"import_dir"`
	Debug         bool      `json:"debug"`
	StartTime     time.Time `json:"start_time"`
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
	Holders     []NodeID  `json:"holders,omitempty"`
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

type HolderData struct {
	File_count int    `json:"file_count"`
	Checksum   string `json:"checksum"`
}

type App struct {
	NodeID                NodeID
	NoStore               bool
	Logger                *log.Logger
	Debugf                func(string, ...interface{})
	FileStore             FileStoreLike
	HttpDataClient        *http.Client
	Discovery             DiscoveryManagerLike
	Cluster               ClusterLike
	Frogpond              *frogpond.Node
	SendUpdatesToPeers    func([]frogpond.DataPoint)
	NotifyFileListChanged func()
	GetCurrentRF          func() int
	Indexer               IndexerLike
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
			CreatedAt:  result.CreatedAt,
			Holders:    uniqueNodeIDs(result.Holders),
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
			CreatedAt:   result.CreatedAt,
			Holders:     uniqueNodeIDs(result.Holders),
		}
	}
	if newResult.ModifiedAt.IsZero() {
		panic("no")
	}
	if existing, ok := resultMap[normPath]; ok {
		resultMap[normPath] = mergeSearchResults(existing, newResult)
	} else {
		resultMap[normPath] = newResult
	}
}

func uniqueNodeIDs(ids []NodeID) []NodeID {
	if len(ids) == 0 {
		return nil
	}

	seen := make(map[NodeID]struct{}, len(ids))
	result := make([]NodeID, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}

	if len(result) == 0 {
		return nil
	}

	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func mergeSearchResults(existing, incoming SearchResult) SearchResult {
	merged := existing
	merged.Holders = mergeNodeIDSlices(existing.Holders, incoming.Holders)

	if incoming.ModifiedAt.After(existing.ModifiedAt) {
		merged.Name = incoming.Name
		merged.Path = incoming.Path
		merged.Size = incoming.Size
		merged.ContentType = incoming.ContentType
		merged.ModifiedAt = incoming.ModifiedAt
		merged.CreatedAt = incoming.CreatedAt
		merged.Checksum = incoming.Checksum
	}

	return merged
}

func mergeNodeIDSlices(current, updates []NodeID) []NodeID {
	if len(current) == 0 {
		return uniqueNodeIDs(updates)
	}
	if len(updates) == 0 {
		return uniqueNodeIDs(current)
	}
	combined := make([]NodeID, 0, len(current)+len(updates))
	combined = append(combined, current...)
	combined = append(combined, updates...)
	return uniqueNodeIDs(combined)
}

type PartitionStore string

const DefaultPartitionCount = 65536 // 2^16 partitions

// PartitionIDForPath calculates the partition identifier for a given cluster path.
func PartitionIDForPath(path string) PartitionID {
	h := crc32.ChecksumIEEE([]byte(path))
	partitionNum := h % DefaultPartitionCount
	return PartitionID(fmt.Sprintf("p%05d", partitionNum))
}

// ExtractPartitionStoreID derives the partition store identifier from a full partition ID.
// Partitions are grouped to avoid maintaining tens of thousands of directories.
func ExtractPartitionStoreID(partition PartitionID) PartitionStore {
	if len(partition) < 3 {
		log.Panicf("invalid partition id provided to ExtractPartitionStoreID: %v", partition)
	}
	return PartitionStore(string(partition)[:3])
}

// ExtractFilePath validates and returns the cluster file path.
func ExtractFilePath(_ PartitionID, path string) string {
	if path == "" {
		log.Panicf("invalid empty path provided to ExtractFilePath")
	}
	if !strings.HasPrefix(path, "/") {
		log.Panicf("path must be absolute in ExtractFilePath: %v", path)
	}
	return path
}

type PartitionID string

// ExtractPartitionID validates and returns the provided partition identifier.
func ExtractPartitionID(partition PartitionID, _ string) PartitionID {
	if partition == "" {
		log.Panicf("invalid empty partition provided to ExtractPartitionID")
	}
	return partition
}
