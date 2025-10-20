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

	"github.com/donomii/clusterF/httpclient"
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

// StoreFileWithModTime stores a file using an explicit modification time and returns the node that handled the write.
func (fs *ClusterFileSystem) StoreFileWithModTime(ctx context.Context, path string, content []byte, contentType string, modTime time.Time) (types.NodeID, error) {
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
		return "", logerrf("failed to store file: %v", err)
	}

	return fs.cluster.ID(), nil
}

// forwardUploadToStorageNode forwards file uploads from no-store clients to storage nodes
func (fs *ClusterFileSystem) forwardUploadToStorageNode(path string, metadataJSON []byte, content []byte, contentType string) (types.NodeID, error) {
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
	}
	// Randomize the list
	if len(targetNodes) > 1 {
		for i := len(targetNodes) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			targetNodes[i], targetNodes[j] = targetNodes[j], targetNodes[i]
		}
	}
	// Select nodes based on rules
	type nodeCandidate struct {
		id       string
		usage    float64
		hasUsage bool
	}

	candidates := make([]nodeCandidate, 0, len(targetNodes))
	for _, id := range targetNodes {
		info, ok := allNodes[types.NodeID(id)]
		var usage float64
		hasUsage := ok && info != nil && info.DiskSize > 0
		if hasUsage {
			used := info.DiskSize - info.DiskFree
			if used < 0 {
				used = 0
			}
			usage = float64(used) / float64(info.DiskSize)
			if usage >= 0.9 {
				continue // Drop nodes that are 90% full or higher
			}
		}
		// Track candidate usage so we can prioritise low-usage nodes
		candidates = append(candidates, nodeCandidate{
			id:       id,
			usage:    usage,
			hasUsage: hasUsage,
		})
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("no storage nodes available to forward upload (after filtering)")
	}

	/*  Need a better system, or we just load one node
	minIdx := -1
	minUsage := 2.0 // Higher than any valid usage (which is <= 1.0)
	for i, candidate := range candidates {
		if !candidate.hasUsage {
			continue
		}
		if candidate.usage < minUsage {
			minUsage = candidate.usage
			minIdx = i
		}
	}
	if minIdx > 0 {
		// Move the lowest-usage node to the front so it is tried first
		lowest := candidates[minIdx]
		copy(candidates[1:minIdx+1], candidates[0:minIdx])
		candidates[0] = lowest
	}
	*/
	// Occasionally rotate the first two nodes to avoid hot-spotting on a single peer
	if len(candidates) > 1 && rand.Float64() < 0.25 {
		candidates[0], candidates[1] = candidates[1], candidates[0]
	}

	targetNodes = targetNodes[:0]
	for _, candidate := range candidates {
		targetNodes = append(targetNodes, candidate.id)
	}

	if len(targetNodes) == 0 {
		return "", fmt.Errorf("no storage nodes available to forward upload")
	}

	// Get discovery peers to match nodeIDs to addresses
	peerMap := fs.cluster.DiscoveryManager().GetPeerMap()

	// Try forwarding to target nodes until one succeeds
	var lastErr error
	skippedPeers := 0

	modTime, size, metaErr := decodeForwardedMetadata(metadataJSON)

	//fs.debugf("Uploading %v to one of %v nodes(%v)", path, len(targetNodes), targetNodes)
	for _, nodeID := range targetNodes {
		//fs.debugf("Uploading %v to %v", path, nodeID)
		peer, ok := peerMap.Load(nodeID)
		if !ok {
			fs.debugf("[FILES] Node %s not found in discovery peers", nodeID)
			lastErr = fmt.Errorf("[FILES] Node %s not found in discovery peers", nodeID)
			continue
		}

		if metaErr == nil {
			upToDate, err := fs.peerHasUpToDateFile(peer, path, modTime, size)
			if err == nil && upToDate {
				msg := fmt.Sprintf("[FILES] Peer %s already has %s (mod >= %s); skipping forward", peer.NodeID, path, modTime.Format(time.RFC3339))
				skippedPeers++
				lastErr = fmt.Errorf("%v", msg)
				fs.debugf("%v", msg)
				continue
			}
			if err != nil {
				fs.debugf("[FILES] HEAD check failed for %s on %s: %v", path, peer.NodeID, err)
			}
		}

		fileURL, err := urlutil.BuildInternalFilesURL(peer.Address, peer.HTTPPort, path)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := httpclient.Put(context.Background(), fs.cluster.DataClient(), fileURL, bytes.NewReader(content),
			httpclient.WithHeader("Content-Type", contentType),
			httpclient.WithHeader("X-Forwarded-From", string(fs.cluster.ID())),
			httpclient.WithHeader("X-ClusterF-Metadata", base64.StdEncoding.EncodeToString(metadataJSON)),
		)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			resp.Close()
			fs.debugf("[FILES] Forwarded upload %s to %s", path, peer.NodeID)
			return types.NodeID(nodeID), nil
		}
		respBody, _ := resp.ReadAllAndClose()
		lastErr = fmt.Errorf("peer (%v) store query returned %v(%v) '%v'", peer.NodeID, resp.StatusCode, resp.Status, respBody)

	}

	if skippedPeers == len(targetNodes) {
		fs.debugf("[FILES] All peers already had %s; nothing to forward", path)
		return "", nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no storage nodes accepted upload (skipped=%v)", skippedPeers)
	}

	return "", fmt.Errorf("failed to forward upload to any storage node: %v", lastErr)
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
		return nil, types.FileMetadata{}, fmt.Errorf("file not found locally: %v", path)
	}

	// Check if file is marked as deleted
	if metadata.Deleted {
		return nil, types.FileMetadata{}, fmt.Errorf("file was deleted: %v", path)
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

func (fs *ClusterFileSystem) GetMetadata(path string) (types.FileMetadata, error) {
	//fs.debugf("Starting GetMetadata for path %v", path)
	//defer fs.debugf("Leaving GetMetadata for path %v", path)
	// Try to get metadata from partition system
	metadata, err := fs.cluster.PartitionManager().GetMetadataFromPartition(path)
	if err != nil {
		if errors.Is(err, types.ErrFileNotFound) {
			return fs.cluster.PartitionManager().GetMetadataFromPeers(path)
		}
		return fs.cluster.PartitionManager().GetMetadataFromPeers(path)
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
	fileURL, err := urlutil.BuildInternalFilesURL(peer.Address, peer.HTTPPort, path)
	if err != nil {
		return false, err
	}
	resp, err := httpclient.Head(context.Background(), fs.cluster.DataClient(), fileURL)
	if err != nil {
		return false, err
	}
	defer resp.Close()
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
