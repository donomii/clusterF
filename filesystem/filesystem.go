// filesystem.go - Distributed file system layer on top of partition system
package filesystem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
)

const (
	MaxFileNameLen = 255
	MaxPathLen     = 4096
)

// ClusterFileSystem provides a file system interface over the cluster
type ClusterFileSystem struct {
	cluster types.ClusterLike
	Debug   bool
}

// NewClusterFileSystem creates a new distributed file system
func NewClusterFileSystem(cluster types.ClusterLike, debug bool) *ClusterFileSystem {
	return &ClusterFileSystem{
		cluster: cluster,
		Debug:   debug,
	}
}

// calculateChecksum computes SHA-256 hash of file content
func calculateChecksum(content []byte) string {
	hash := sha256.Sum256(content)
	return hex.EncodeToString(hash[:])
}

// verifyChecksum validates file content against its stored checksum
func verifyChecksum(content []byte, expectedChecksum string) error {
	actualChecksum := calculateChecksum(content)
	if actualChecksum != expectedChecksum {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedChecksum, actualChecksum)
	}
	return nil
}

// StoreFile requires explicit metadata; callers must provide modification time via StoreFileWithModTime.
func (fs *ClusterFileSystem) StoreFile(path string, content []byte, contentType string) error {
	return fmt.Errorf("StoreFile requires explicit modification time; use StoreFileWithModTime")
}

// debugf logs a debug message if Debug is enabled
func (c *ClusterFileSystem) debugf(format string, v ...interface{}) {
	if !c.Debug {
		return
	}
	// Use Logger.Output with a call depth so the log shows the
	// caller of debugf (file:line), not this wrapper function.
	// calldepth=2: Output -> debugf -> caller
	msg := fmt.Sprintf(format, v...)
	_ = c.cluster.Logger().Output(2, msg)
}

// logerrf formats an error message with call site information and returns the formatted message
func logerrf(format string, args ...interface{}) error {
	// Format the main error message
	message := fmt.Sprintf(format, args...)

	// Get caller information (1 step up the call stack)
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		return fmt.Errorf("at unknown location, %s", message)
	}

	// Extract just the filename from the full path
	filename := file
	if idx := strings.LastIndex(file, "/"); idx >= 0 {
		filename = file[idx+1:]
	}

	// Format the complete error message with call site
	return fmt.Errorf("at %s:%d, %s", filename, line, message)
}

func decodeForwardedMetadata(metadataJSON []byte) (time.Time, int64, error) {
	var meta types.FileMetadata
	if err := json.Unmarshal(metadataJSON, &meta); err != nil {
		panic("no")
	}
	if meta.ModifiedAt.IsZero() {
		panic("no")
	}
	return meta.ModifiedAt, meta.Size, nil
}

// StoreFileWithModTime stores a file using an explicit modification time
func (fs *ClusterFileSystem) StoreFileWithModTime(ctx context.Context, path string, content []byte, contentType string, modTime time.Time) error {
	if err := fs.validatePath(path); err != nil {
		return err
	}

	// Calculate checksum for file integrity
	checksum := calculateChecksum(content)
	//fs.debugf("[CHECKSUM_DEBUG] Calculated checksum for %s: %s", path, checksum)

	// Create file metadata for the file system layer
	metadata := types.FileMetadata{
		Name:        filepath.Base(path),
		Path:        path,
		Size:        int64(len(content)),
		ContentType: contentType,
		CreatedAt:   modTime,
		ModifiedAt:  modTime,
		IsDirectory: false,
		Checksum:    checksum,
	}

	if metadata.ModifiedAt.IsZero() {
		panic("no")
	}

	//fs.debugf("[CHECKSUM_DEBUG] Enhanced metadata for %s has checksum: %s", path, enhancedMetadata["checksum"])
	metadataJSON, _ := json.Marshal(metadata)

	// For no-store clients, forward uploads to storage nodes
	if fs.cluster.NoStore() {
		return fs.forwardUploadToStorageNode(path, metadataJSON, content, contentType)
	}

	if err := fs.cluster.PartitionManager().StoreFileInPartition(ctx, path, metadataJSON, content); err != nil {
		return logerrf("failed to store file: %v", err)
	}

	return nil
}

// forwardUploadToStorageNode forwards file uploads from no-store clients to storage nodes
func (fs *ClusterFileSystem) forwardUploadToStorageNode(path string, metadataJSON []byte, content []byte, contentType string) error {
	// Calculate partition name for this file
	partitionName := fs.cluster.PartitionManager().CalculatePartitionName(path)

	// Get all nodes from CRDT
	allNodes := fs.cluster.GetAllNodes()

	// Get nodes that hold this partition
	nodesForPartition := fs.cluster.GetNodesForPartition(partitionName)

	// If partition exists on nodes, use those
	var targetNodes []string
	if len(nodesForPartition) > 0 {
		targetNodes = make([]string, len(nodesForPartition))
		for i, nodeID := range nodesForPartition {
			targetNodes[i] = string(nodeID)
		}
	} else {
		// No nodes hold this partition yet - filter out no-store nodes and randomize
		for nodeID, nodeInfo := range allNodes {
			if nodeInfo != nil && nodeInfo.IsStorage {
				targetNodes = append(targetNodes, string(nodeID))
			}
		}

		// Randomize the list
		if len(targetNodes) > 1 {
			for i := len(targetNodes) - 1; i > 0; i-- {
				j := rand.Intn(i + 1)
				targetNodes[i], targetNodes[j] = targetNodes[j], targetNodes[i]
			}
		}
	}

	if len(targetNodes) == 0 {
		return fmt.Errorf("no storage nodes available to forward upload")
	}

	// Get discovery peers to match nodeIDs to addresses
	peers := fs.cluster.DiscoveryManager().GetPeers()
	peerMap := make(map[string]*types.PeerInfo)
	for _, peer := range peers {
		peerMap[peer.NodeID] = peer
	}

	// Try forwarding to target nodes until one succeeds
	var lastErr error
	forwarded := false
	skippedPeers := 0

	modTime, size, metaErr := decodeForwardedMetadata(metadataJSON)

	for _, nodeID := range targetNodes {
		peer, ok := peerMap[nodeID]
		if !ok {
			fs.debugf("[FILES] Node %s not found in discovery peers", nodeID)
			continue
		}

		if metaErr == nil {
			upToDate, err := fs.peerHasUpToDateFile(peer, path, modTime, size)
			if err == nil && upToDate {
				fs.debugf("[FILES] Peer %s already has %s (mod >= %s); skipping forward", peer.NodeID, path, modTime.Format(time.RFC3339))
				skippedPeers++
				continue
			}
			if err != nil {
				fs.debugf("[FILES] HEAD check failed for %s on %s: %v", path, peer.NodeID, err)
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
		req.Header.Set("X-Forwarded-From", string(fs.cluster.ID()))
		req.Header.Set("X-ClusterF-Metadata", base64.StdEncoding.EncodeToString(metadataJSON))

		resp, err := fs.cluster.DataClient().Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusCreated {
			fs.debugf("[FILES] Forwarded upload %s to %s", path, peer.NodeID)
			forwarded = true
			return nil // Success
		}

		lastErr = fmt.Errorf("peer %s returned %d", peer.NodeID, resp.StatusCode)
	}

	if forwarded || skippedPeers == len(targetNodes) {
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
func (fs *ClusterFileSystem) GetFile(path string) ([]byte, types.FileMetadata, error) {
	// Get file content and metadata together
	content, metadata, err := fs.cluster.PartitionManager().GetFileAndMetaFromPartition(path)
	if err != nil {
		if errors.Is(err, types.ErrFileNotFound) {
			return nil, types.FileMetadata{}, types.ErrFileNotFound
		}
		return nil, types.FileMetadata{}, err
	}

	// Check if file is marked as deleted
	if metadata.Deleted {
		return nil, types.FileMetadata{}, types.ErrFileNotFound
	}

	// Verify file integrity using checksum
	if err := verifyChecksum(content, metadata.Checksum); err != nil {
		return nil, types.FileMetadata{}, fmt.Errorf("file integrity check failed for %s: %v", path, err)
	}

	if metadata.ModifiedAt.IsZero() {
		panic("no")
	}

	return content, metadata, nil
}

// ListDirectory lists the contents of a directory using search API
func (fs *ClusterFileSystem) ListDirectory(path string) ([]*types.FileMetadata, error) {
	return fs.cluster.ListDirectoryUsingSearch(path)
}

// DeleteFile removes a file from the cluster
func (fs *ClusterFileSystem) DeleteFile(ctx context.Context, path string) error {
	// Delete from partition system
	if err := fs.cluster.PartitionManager().DeleteFileFromPartition(ctx, path); err != nil {
		return fmt.Errorf("failed to delete file: %v", err)
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

func (fs *ClusterFileSystem) GetMetadata(path string) (types.FileMetadata, error) {
	fs.debugf("Starting GetMetadata for path %v", path)
	defer fs.debugf("Leaving GetMetadata for path %v", path)
	// Try to get metadata from partition system
	metadata, err := fs.cluster.PartitionManager().GetMetadataFromPartition(path)
	if err != nil {
		if errors.Is(err, types.ErrFileNotFound) {
			return types.FileMetadata{}, types.ErrFileNotFound
		}
		return types.FileMetadata{}, err
	}

	// Check if file is marked as deleted
	if metadata.Deleted {
		return types.FileMetadata{}, types.ErrFileNotFound
	}

	if metadata.Checksum == "" {
		panic("fuck ai")
	}

	if metadata.ModifiedAt.IsZero() {
		panic("no")
	}

	fs.debugf("Found metadata: %+v for path %v", metadata, path)
	return metadata, nil
}

// MetadataForPath adapts internal metadata to the exporter module's format.
func (fs *ClusterFileSystem) MetadataForPath(path string) (types.FileMetadata, error) {
	return fs.GetMetadata(path)
}

// CreateDirectory is a no-op since directories are inferred from file paths
func (fs *ClusterFileSystem) CreateDirectory(path string) error {
	return nil // Directories are inferred from file paths
}

// CreateDirectoryWithModTime is a no-op since directories are inferred from file paths
func (fs *ClusterFileSystem) CreateDirectoryWithModTime(path string, modTime time.Time) error {
	return nil // Directories are inferred from file paths
}

func (fs *ClusterFileSystem) peerHasUpToDateFile(peer *types.PeerInfo, path string, modTime time.Time, size int64) (bool, error) {
	fileURL, err := urlutil.BuildFilesURL(peer.Address, peer.HTTPPort, path)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequest(http.MethodHead, fileURL, nil)
	if err != nil {
		return false, err
	}
	resp, err := fs.cluster.DataClient().Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound:
		return false, nil
	case http.StatusOK:
		remoteMod := time.Time{}
		if lm := resp.Header.Get("X-ClusterF-Modified-At"); lm != "" {
			if t, err := time.Parse(time.RFC3339, lm); err == nil {
				remoteMod = t
			}
		}
		if remoteMod.IsZero() {
			panic("no")
		}
		remoteSize := int64(-1)
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			if n, err := strconv.ParseInt(cl, 10, 64); err == nil {
				remoteSize = n
			}
		}
		if !remoteMod.IsZero() && !remoteMod.Before(modTime) {
			if remoteSize == size {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
}
