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
	"sync"
	"time"

	"github.com/donomii/clusterF/httpclient"
	"github.com/donomii/clusterF/syncmap"
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

	desiredReplicas := fs.cluster.ReplicationFactor()
	if desiredReplicas < 1 {
		desiredReplicas = 1
	}

	// Build the list of nodes that must receive the upload (holders first, otherwise RF candidates).
	targetNodes := make([]string, 0)
	targetSet := make(map[string]bool)
	// When we already host the partition we must push the update to every current holder.
	if len(nodesForPartition) > 0 {
		// Existing holders already replicate this partition; include every holder so they all receive the update.
		// Walk the holder list so we capture a deduplicated slice of nodes to push to.
		for _, nodeID := range nodesForPartition {
			id := string(nodeID)
			if id == "" {
				continue
			}
			if targetSet[id] {
				continue
			}
			targetSet[id] = true
			targetNodes = append(targetNodes, id)
		}
	} else {
		// No holders yet, so choose fresh storage nodes that can accept the data without being over capacity.
		candidates := make([]string, 0, len(allNodes))
		// Inspect every known storage node to assemble the list of initial replica targets.
		for nodeID, nodeInfo := range allNodes {
			// Ignore peers that are not active storage hosts so uploads target real capacity.
			if nodeInfo != nil && nodeInfo.IsStorage {
				// Skip peers that are dangerously full so we do not overload their disks with new replicas.
				if nodeInfo.DiskSize > 0 {
					used := nodeInfo.DiskSize - nodeInfo.DiskFree
					if used < 0 {
						used = 0
					}
					usage := float64(used) / float64(nodeInfo.DiskSize)
					if usage >= 0.9 {
						continue
					}
				}
				candidates = append(candidates, string(nodeID))
			}
		}
		if len(candidates) == 0 {
			return "", fmt.Errorf("no storage nodes available to forward upload")
		}
		if len(candidates) > 1 {
			rand.Shuffle(len(candidates), func(i, j int) {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			})
		}
		// Copy the filtered candidates into the target set without adding duplicates.
		for _, id := range candidates {
			if targetSet[id] {
				continue
			}
			targetSet[id] = true
			targetNodes = append(targetNodes, id)
		}
	}

	if len(targetNodes) == 0 {
		return "", fmt.Errorf("no storage nodes available to forward upload")
	}

	// Get discovery peers to match nodeIDs to addresses
	peerMap := fs.cluster.DiscoveryManager().GetPeerMap()

	modTime, size, metaErr := decodeForwardedMetadata(metadataJSON)

	requiredSuccesses := len(nodesForPartition)
	// When we have no holders, fall back to the configured replication factor as our success target.
	if requiredSuccesses == 0 {
		requiredSuccesses = desiredReplicas
		if requiredSuccesses > len(targetNodes) {
			requiredSuccesses = len(targetNodes)
		}
	}

	// Bail out early if we still have nobody to write to after all the filtering above.
	if requiredSuccesses == 0 {
		return "", fmt.Errorf("no eligible storage nodes available for upload")
	}

	pending := append([]string(nil), targetNodes...)
	successSet := make(map[string]bool)
	successOrder := make([]types.NodeID, 0)
	attemptedAll := make(map[string]bool)
	attemptCounts := make(map[string]int)

	maxBatch := desiredReplicas
	if maxBatch <= 0 {
		maxBatch = len(pending)
	}

	// Iterate in batches so we can fan out uploads while respecting the replication limit.
	for len(pending) > 0 && len(successSet) < requiredSuccesses {
		batchSize := maxBatch
		if batchSize <= 0 || batchSize > len(pending) {
			batchSize = len(pending)
		}
		batch := append([]string(nil), pending[:batchSize]...)
		pending = pending[batchSize:]

		attemptedBatch, _, successes, err, allSkipped := fs.tryForwardToNodes(context.Background(), path, metadataJSON, content, contentType, batch, peerMap, modTime, size, metaErr)

		successMap := make(map[string]bool, len(successes))
		// Record which peers actually accepted this batch so we do not queue them again.
		for _, s := range successes {
			sid := string(s)
			successMap[sid] = true
			if !successSet[sid] {
				successSet[sid] = true
				successOrder = append(successOrder, s)
			}
		}

		// Track per-node contact counts to enforce the retry limit.
		for id := range attemptedBatch {
			attemptedAll[id] = true
			attemptCounts[id]++
		}

		// If every peer said it already has the file, count them as satisfied without retrying elsewhere.
		if allSkipped && len(successes) == 0 {
			// Mark every peer in the batch as satisfied because they already had the data.
			for id := range attemptedBatch {
				if !successSet[id] {
					successSet[id] = true
					successOrder = append(successOrder, types.NodeID(id))
				}
			}
			continue
		}

		// On failure, requeue the peers that might still accept the data, but stop after a few tries.
		if err != nil {
			retry := make([]string, 0)
			// Iterate over the batch so we only retry nodes that actually failed.
			for id := range attemptedBatch {
				if successMap[id] {
					continue
				}
				if attemptCounts[id] >= 3 {
					return "", fmt.Errorf("failed to forward upload to %s after %d attempts: %v", id, attemptCounts[id], err)
				}
				retry = append(retry, id)
			}
			if len(retry) == 0 {
				return "", err
			}
			rand.Shuffle(len(retry), func(i, j int) {
				retry[i], retry[j] = retry[j], retry[i]
			})
			pending = append(pending, retry...)
		}

		// If we run out of candidates before hitting the target, pull in additional storage nodes.
		if len(pending) == 0 && len(successSet) < requiredSuccesses {
			extra := make([]string, 0)
			// Sweep remaining storage peers so we can widen the search for replica targets.
			for nodeID, nodeInfo := range allNodes {
				if nodeInfo == nil || !nodeInfo.IsStorage {
					continue
				}
				id := string(nodeID)
				if successSet[id] || attemptedAll[id] {
					continue
				}
				// Avoid fetching help from nodes that are already low on disk space.
				if nodeInfo.DiskSize > 0 {
					used := nodeInfo.DiskSize - nodeInfo.DiskFree
					if used < 0 {
						used = 0
					}
					usage := float64(used) / float64(nodeInfo.DiskSize)
					if usage >= 0.9 {
						continue
					}
				}
				extra = append(extra, id)
			}
			if len(extra) > 0 {
				rand.Shuffle(len(extra), func(i, j int) {
					extra[i], extra[j] = extra[j], extra[i]
				})
				pending = append(pending, extra...)
			}
		}
	}

	if len(successSet) < requiredSuccesses {
		return "", fmt.Errorf("failed to reach required replicas (needed %d, achieved %d)", requiredSuccesses, len(successSet))
	}

	var chosen types.NodeID
	if len(successOrder) > 0 {
		chosen = successOrder[0]
	}

	return chosen, nil
}

func (fs *ClusterFileSystem) tryForwardToNodes(ctx context.Context, path string, metadataJSON []byte, content []byte, contentType string, targets []string, peerMap *syncmap.SyncMap[string, *types.PeerInfo], modTime time.Time, size int64, metaErr error) (map[string]bool, int, []types.NodeID, error, bool) {
	if len(targets) == 0 {
		return map[string]bool{}, 0, nil, fmt.Errorf("no storage nodes available to forward upload"), false
	}

	// Every goroutine reports its outcome via this channel-friendly struct.
	type forwardResult struct {
		node    types.NodeID
		skipped bool
		err     error
	}

	skipped := 0
	attempted := make(map[string]bool, len(targets))
	metaHeader := base64.StdEncoding.EncodeToString(metadataJSON)

	results := make(chan forwardResult, len(targets))
	var wg sync.WaitGroup

	// Fan the upload out to each target concurrently so we don't serialize network writes.
	for _, nodeID := range targets {
		attempted[nodeID] = true

		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()

			peer, ok := peerMap.Load(nodeID)
			if !ok {
				msg := fmt.Errorf("[FILES] Node %s not found in discovery peers", nodeID)
				fs.debugf("%v", msg)
				results <- forwardResult{node: types.NodeID(nodeID), err: msg}
				return
			}

			// Skip redundant uploads when we already know the peer has a fresh copy.
			if metaErr == nil {
				upToDate, err := fs.peerHasUpToDateFile(peer, path, modTime, size)
				if err == nil && upToDate {
					fs.debugf("[FILES] Peer %s already has %s (mod >= %s); skipping forward", peer.NodeID, path, modTime.Format(time.RFC3339))
					results <- forwardResult{node: types.NodeID(nodeID), skipped: true}
					return
				}
				if err != nil {
					fs.debugf("[FILES] HEAD check failed for %s on %s: %v", path, peer.NodeID, err)
				}
			}

			fileURL, err := urlutil.BuildInternalFilesURL(peer.Address, peer.HTTPPort, path)
			if err != nil {
				results <- forwardResult{node: types.NodeID(nodeID), err: err}
				return
			}

			respBody, _, status, err := httpclient.SimplePut(ctx, fs.cluster.DataClient(), fileURL, bytes.NewReader(content),
				httpclient.WithHeader("Content-Type", contentType),
				httpclient.WithHeader("X-Forwarded-From", string(fs.cluster.ID())),
				httpclient.WithHeader("X-ClusterF-Metadata", metaHeader),
			)
			if err != nil {
				results <- forwardResult{node: types.NodeID(nodeID), err: err}
				return
			}
			// Treat only 2xx responses as a successful replica write.
			if status >= 200 && status < 300 {
				fs.debugf("[FILES] Forwarded upload %s to %s", path, peer.NodeID)
				results <- forwardResult{node: types.NodeID(nodeID)}
				return
			}
			results <- forwardResult{
				node: types.NodeID(nodeID),
				err:  fmt.Errorf("peer %s returned %d for PUT %s: %s", peer.NodeID, status, path, strings.TrimSpace(string(respBody))),
			}
		}(nodeID)
	}

	wg.Wait()
	// The channel is buffered to len(targets); close it once all senders are done.
	close(results)

	var errorMessages []string
	successes := make([]types.NodeID, 0, len(targets))

	// Drain the results channel once all uploads finished to aggregate successes and failures.
	for res := range results {
		if res.skipped {
			skipped++
			continue
		}
		if res.err != nil {
			errorMessages = append(errorMessages, res.err.Error())
			continue
		}
		successes = append(successes, res.node)
	}

	if len(errorMessages) == 0 {
		return attempted, skipped, successes, nil, skipped == len(targets)
	}

	combinedErr := errors.New(strings.Join(errorMessages, "; "))
	return attempted, skipped, successes, combinedErr, false
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
	// Bubble errors from the partition manager so callers know whether the lookup failed locally or globally.
	if err != nil {
		if errors.Is(err, types.ErrFileNotFound) {
			return nil, types.FileMetadata{}, err
		}
		return nil, types.FileMetadata{}, fmt.Errorf("partition lookup failed for %s: %w", path, err)
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
	// Translate partition-layer failures into the filesystem contract for metadata lookups.
	if err != nil {
		if errors.Is(err, types.ErrFileNotFound) {
			return types.FileMetadata{}, types.ErrFileNotFound
		}
		return types.FileMetadata{}, types.ErrFileNotFound
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

// MetadataViaAPI fetches metadata via the external HTTP API, ensuring fan-out across the cluster.
func (fs *ClusterFileSystem) MetadataViaAPI(ctx context.Context, path string) (types.FileMetadata, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	type httpPortProvider interface {
		HTTPPort() int
	}
	portSource, ok := fs.cluster.(httpPortProvider)
	if !ok {
		return types.FileMetadata{}, fmt.Errorf("cluster does not expose HTTPPort")
	}

	address := "localhost"
	if dm := fs.cluster.DiscoveryManager(); dm != nil {
		if addr := dm.GetLocalAddress(); addr != "" {
			address = addr
		}
	}

	normalized := urlutil.NormalizeAbsolutePath(path)
	metadataURL, err := urlutil.BuildHTTPURL(address, portSource.HTTPPort(), "/api/metadata"+normalized)
	if err != nil {
		return types.FileMetadata{}, err
	}

	body, _, status, err := httpclient.SimpleGet(ctx, fs.cluster.DataClient(), metadataURL)
	if err != nil {
		return types.FileMetadata{}, err
	}

	switch status {
	case http.StatusOK:
		var metadata types.FileMetadata
		if err := json.Unmarshal(body, &metadata); err != nil {
			return types.FileMetadata{}, fmt.Errorf("failed to decode metadata response: %w", err)
		}
		return metadata, nil
	case http.StatusNotFound:
		return types.FileMetadata{}, types.ErrFileNotFound
	default:
		return types.FileMetadata{}, fmt.Errorf("metadata API returned %d: %s", status, strings.TrimSpace(string(body)))
	}
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
	headers, status, err := httpclient.SimpleHead(context.Background(), fs.cluster.DataClient(), fileURL)
	if err != nil {
		return false, err
	}
	switch status {
	case http.StatusNotFound:
		return false, nil
	case http.StatusOK:
		remoteMod := time.Time{}
		if lm := headers.Get("X-ClusterF-Modified-At"); lm != "" {
			if t, err := time.Parse(time.RFC3339, lm); err == nil {
				remoteMod = t
			}
		}
		if remoteMod.IsZero() {
			panic("no")
		}
		remoteSize := int64(-1)
		if cl := headers.Get("Content-Length"); cl != "" {
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
		return false, fmt.Errorf("unexpected status %d", status)
	}
}
