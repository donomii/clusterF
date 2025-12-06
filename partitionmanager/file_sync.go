package partitionmanager

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/donomii/clusterF/httpclient"
	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
	"github.com/tchap/go-patricia/patricia"
)

const fileSyncInterval = 30 * time.Second

// MarkFileForSync tracks a file that needs to be synchronised between holders.
func (pm *PartitionManager) MarkFileForSync(path, reason string) {
	if path == "" {
		return
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	pm.fileSyncMu.Lock()
	pm.fileSyncTrie.Set(patricia.Prefix(path), struct{}{})
	pm.fileSyncMu.Unlock()

	pm.logf("[MarkFileForSync] Marked file %s for sync, because %s", path, reason)
}

func (pm *PartitionManager) drainFileSyncQueue() []string {
	pm.fileSyncMu.Lock()
	defer pm.fileSyncMu.Unlock()

	var paths []string
	pm.fileSyncTrie.Visit(func(prefix patricia.Prefix, _ patricia.Item) error {
		paths = append(paths, string(prefix))
		return nil
	})
	for _, p := range paths {
		pm.fileSyncTrie.Delete(patricia.Prefix(p))
	}

	sort.Strings(paths)
	return paths
}

// RunFileSync is a long-running worker that processes queued file sync requests.
func (pm *PartitionManager) RunFileSync(ctx context.Context) {
	if pm.deps.Cluster.NoStore() {
		<-ctx.Done()
		return
	}

	ticker := time.NewTicker(fileSyncInterval)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return
		}

		pm.processFileSyncQueue(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (pm *PartitionManager) processFileSyncQueue(ctx context.Context) {
	paths := pm.drainFileSyncQueue()
	if len(paths) == 0 {
		return
	}

	for _, path := range paths {
		if ctx.Err() != nil {
			return
		}
		if err := pm.SyncFile(ctx, path); err != nil {
			pm.logf("[FILE SYNC] Failed to sync %s: %v", path, err)
			pm.MarkFileForSync(path, fmt.Sprintf("retry after error: %v", err))
		}
	}
}

type fileHolderState struct {
	holder types.NodeID
	meta   types.FileMetadata
	ok     bool
	err    error
}

func (pm *PartitionManager) SyncFile(ctx context.Context, path string) error {
	partitionID := HashToPartition(path)
	partInfo := pm.GetPartitionInfo(partitionID)
	if partInfo == nil {
		return fmt.Errorf("no partition info for %s", partitionID)
	}

	holders := dedupeHolders(partInfo.Holders, pm.deps.NodeID)
	if len(holders) == 0 {
		return fmt.Errorf("no holders recorded for %s", path)
	}

	states := make([]fileHolderState, 0, len(holders))
	for _, holder := range holders {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		meta, ok, err := pm.fetchMetadataForHolder(ctx, holder, path)
		states = append(states, fileHolderState{
			holder: holder,
			meta:   meta,
			ok:     ok,
			err:    err,
		})
		if err != nil {
			pm.debugf("[FILE SYNC] Failed to fetch metadata for %s from %s: %v", path, holder, err)
		}
	}

	bestIdx := -1
	for i, state := range states {
		if !state.ok {
			continue
		}
		if bestIdx == -1 || metadataIsNewer(state.meta, states[bestIdx].meta) {
			bestIdx = i
		}
	}

	if bestIdx == -1 {
		return fmt.Errorf("no metadata available for %s", path)
	}

	best := states[bestIdx]
	if best.meta.Path == "" {
		best.meta.Path = path
	}
	if best.meta.ModifiedAt.IsZero() {
		return fmt.Errorf("metadata for %s missing ModifiedAt timestamp", path)
	}
	if !best.meta.Deleted && best.meta.Checksum == "" {
		return fmt.Errorf("metadata for %s missing checksum", path)
	}
	if best.meta.IsDirectory {
		pm.debugf("[FILE SYNC] Skipping directory %s", path)
		return nil
	}

	var content []byte
	if !best.meta.Deleted {
		data, err := pm.fetchFileContentForHolder(ctx, best.holder, path)
		if err != nil {
			return fmt.Errorf("failed to fetch content for %s from %s: %w", path, best.holder, err)
		}
		if err := pm.verifyFileChecksum(data, best.meta.Checksum, path, best.holder); err != nil {
			return err
		}
		content = data
	}

	for _, state := range states {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if sameMetadataVersion(state.meta, best.meta) && state.ok {
			continue
		}

		if err := pm.applyFileUpdate(ctx, state.holder, best.meta, content); err != nil {
			pm.debugf("[FILE SYNC] Failed to update %s on %s: %v", path, state.holder, err)
		}
	}

	return nil
}

func dedupeHolders(holders []types.NodeID, self types.NodeID) []types.NodeID {
	seen := make(map[types.NodeID]bool)
	var result []types.NodeID
	for _, h := range holders {
		if h == "" || seen[h] {
			continue
		}
		seen[h] = true
		result = append(result, h)
	}
	if self != "" && !seen[self] {
		result = append(result, self)
	}
	return result
}

func metadataIsNewer(candidate, current types.FileMetadata) bool {
	if current.ModifiedAt.IsZero() {
		return true
	}
	if candidate.ModifiedAt.After(current.ModifiedAt) {
		return true
	}
	if candidate.ModifiedAt.Equal(current.ModifiedAt) {
		if candidate.Checksum != "" && current.Checksum != "" && candidate.Checksum != current.Checksum {
			return candidate.Checksum > current.Checksum
		}
		if candidate.Deleted && !current.Deleted {
			return true
		}
	}
	return false
}

func sameMetadataVersion(a, b types.FileMetadata) bool {
	if a.ModifiedAt.IsZero() && b.ModifiedAt.IsZero() {
		return true
	}
	if !a.ModifiedAt.Equal(b.ModifiedAt) {
		return false
	}
	if a.Deleted != b.Deleted {
		return false
	}
	return a.Checksum == b.Checksum
}

func (pm *PartitionManager) fetchMetadataForHolder(ctx context.Context, holder types.NodeID, path string) (types.FileMetadata, bool, error) {
	if holder == pm.deps.NodeID {
		data, err := pm.deps.FileStore.GetMetadata(path)
		if err != nil {
			return types.FileMetadata{}, false, err
		}
		var meta types.FileMetadata
		if err := json.Unmarshal(data, &meta); err != nil {
			return types.FileMetadata{}, false, err
		}
		if meta.Path == "" {
			meta.Path = path
		}
		return meta, true, nil
	}

	peer := pm.getPeer(holder)
	if peer == nil {
		return types.FileMetadata{}, false, fmt.Errorf("peer %s not found", holder)
	}

	meta, err := pm.fetchMetadataFromPeer(peer, path)
	if err != nil {
		return types.FileMetadata{}, false, err
	}
	if meta.Path == "" {
		meta.Path = path
	}
	return meta, true, nil
}

func (pm *PartitionManager) fetchFileContentForHolder(ctx context.Context, holder types.NodeID, path string) ([]byte, error) {
	if holder == pm.deps.NodeID {
		_, content, exists, err := pm.deps.FileStore.Get(path)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("local file %s missing", path)
		}
		return content, nil
	}

	peer := pm.getPeer(holder)
	if peer == nil {
		return nil, fmt.Errorf("peer %s not found", holder)
	}
	return pm.fetchFileFromPeer(peer, path)
}

func (pm *PartitionManager) applyFileUpdate(ctx context.Context, holder types.NodeID, meta types.FileMetadata, content []byte) error {
	if holder == pm.deps.NodeID {
		entry := PartitionSyncEntry{
			Path:     meta.Path,
			Metadata: meta,
		}
		if meta.Deleted {
			return pm.storeEntryMetadata(entry)
		}
		return pm.storeEntryMetadataAndContent(entry, content)
	}

	peer := pm.getPeer(holder)
	if peer == nil {
		return fmt.Errorf("peer %s not found", holder)
	}

	if meta.Deleted {
		return pm.pushDeleteToPeer(ctx, peer, meta)
	}
	return pm.pushFileToPeer(ctx, peer, meta, content)
}

func (pm *PartitionManager) pushFileToPeer(ctx context.Context, peer *types.PeerInfo, meta types.FileMetadata, content []byte) error {
	if len(content) == 0 {
		return fmt.Errorf("no content available for %s", meta.Path)
	}

	fileURL, err := urlutil.BuildInternalFilesURL(peer.Address, peer.HTTPPort, meta.Path)
	if err != nil {
		return err
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	encodedMeta := base64.StdEncoding.EncodeToString(metaJSON)
	contentType := meta.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	body := bytes.NewReader(content)
	_, _, status, err := httpclient.SimplePut(ctx, pm.httpClient(), fileURL, body,
		httpclient.WithHeader("X-ClusterF-Internal", "1"),
		httpclient.WithHeader("X-Forwarded-From", string(pm.deps.NodeID)),
		httpclient.WithHeader("X-ClusterF-Metadata", encodedMeta),
		httpclient.WithHeader("X-ClusterF-Modified-At", meta.ModifiedAt.Format(time.RFC3339)),
		httpclient.WithHeader("Content-Type", contentType),
	)
	if err != nil {
		return err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return fmt.Errorf("peer %s returned %d for PUT %s", peer.NodeID, status, meta.Path)
	}
	return nil
}

func (pm *PartitionManager) pushDeleteToPeer(ctx context.Context, peer *types.PeerInfo, meta types.FileMetadata) error {
	fileURL, err := urlutil.BuildInternalFilesURL(peer.Address, peer.HTTPPort, meta.Path)
	if err != nil {
		return err
	}

	modified := meta.ModifiedAt
	if modified.IsZero() {
		modified = time.Now()
	}

	_, _, status, err := httpclient.SimpleDelete(ctx, pm.httpClient(), fileURL,
		httpclient.WithHeader("X-ClusterF-Internal", "1"),
		httpclient.WithHeader("X-ClusterF-Modified-At", modified.Format(time.RFC3339)),
	)
	if err != nil {
		return err
	}

	if status != http.StatusNoContent && status != http.StatusOK {
		return fmt.Errorf("peer %s returned %d for DELETE %s", peer.NodeID, status, meta.Path)
	}
	return nil
}
