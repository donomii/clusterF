package partitionmanager

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/donomii/clusterF/httpclient"
	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
	"github.com/tchap/go-patricia/patricia"
)

const fileSyncInterval = 30 * time.Second

type streamFileStore interface {
	PutStream(path string, metadata []byte, content io.Reader, size int64) error
}

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
	err    error
}

func (pm *PartitionManager) SyncFile(ctx context.Context, path string) error {
	partitionID := HashToPartition(path)
	partInfo := pm.GetPartitionInfo(partitionID)
	if partInfo == nil {
		return fmt.Errorf("no partition info for %s", partitionID)
	}

	holders := partInfo.Holders
	if len(holders) == 0 {
		return fmt.Errorf("no holders recorded for %s", path)
	}

	states := make([]fileHolderState, 0, len(holders))
	for _, holder := range holders {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		peerInfo := pm.getPeer(holder)
		if peerInfo == nil {
			pm.debugf("[FILE SYNC] No peer info for %s", holder)
			continue
		}

		meta, err := pm.fetchMetadataFromPeer(peerInfo, path)

		if err != nil {
			pm.debugf("[FILE SYNC] Failed to fetch metadata for %s from %s: %v", path, holder, err)
		} else {
			states = append(states, fileHolderState{
				holder: holder,
				meta:   meta,
				err:    err,
			})
		}
	}

	bestIdx := -1
	for i, state := range states {
		if state.err != nil {
			continue
		}
		if bestIdx == -1 || metadataIsNewer(state.meta, states[bestIdx].meta) {
			bestIdx = i
		}
	}

	if bestIdx == -1 {
		return fmt.Errorf("no metadata available for %s, holders: %v, states: %+v", path, holders, states)
	}

	best := states[bestIdx]
	types.Assertf(best.meta.Path != "", "best.meta.Path can never be empty")
	types.Assertf(!best.meta.ModifiedAt.IsZero(), "metadata for %s missing ModifiedAt timestamp", path)
	types.Assertf(!(best.meta.Deleted && best.meta.Checksum == ""), "metadata for %s missing checksum", path)

	var tempPath string
	var size int64
	if !best.meta.Deleted {
		var err error
		tempPath, size, err = pm.fetchFileToTemp(ctx, best.holder, path, best.meta.Checksum)
		if err != nil {
			return fmt.Errorf("failed to fetch content for %s from %s: %w", path, best.holder, err)
		}

	}
	types.Assertf(tempPath != "", "no temp path for %s", path)
	defer func() {

		os.Remove(tempPath)

	}()

	for _, state := range states {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if sameMetadataVersion(state.meta, best.meta) {
			continue
		}

		if err := pm.applyFileUpdateFromTemp(ctx, state.holder, best.meta, tempPath, size); err != nil {
			pm.debugf("[FILE SYNC] Failed to update %s on %s: %v", path, state.holder, err)
		}
	}

	return nil
}

func metadataIsNewer(candidate, current types.FileMetadata) bool {
	if current.ModifiedAt.IsZero() {
		return true
	}
	if candidate.ModifiedAt.After(current.ModifiedAt) {
		return true
	}
	if candidate.ModifiedAt.Equal(current.ModifiedAt) {
		types.Assertf(candidate.Checksum != "" && current.Checksum != "", "checksum must be set when comparing metadata versions for %s", candidate.Path)
		if candidate.Checksum != current.Checksum {
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

func (pm *PartitionManager) applyFileUpdateFromTemp(ctx context.Context, holder types.NodeID, meta types.FileMetadata, tempPath string, size int64) error {

	peer := pm.getPeer(holder)
	if peer == nil {
		return fmt.Errorf("peer %s not found", holder)
	}

	if meta.Deleted {
		return pm.pushDeleteToPeer(ctx, peer, meta)
	}
	return pm.pushFileToPeerFromFile(ctx, peer, meta, tempPath, size)
}

func (pm *PartitionManager) pushFileToPeerFromFile(ctx context.Context, peer *types.PeerInfo, meta types.FileMetadata, tempPath string, size int64) error {
	types.Assertf(tempPath != "", "no content available for %s", meta.Path)

	fileURL, err := urlutil.BuildInternalFilesURL(peer.Address, peer.HTTPPort, meta.Path)
	if err != nil {
		return err
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	encodedMeta := base64.StdEncoding.EncodeToString(metaJSON)
	types.Assertf(meta.ContentType != "", "content type must be provided for %s", meta.Path)

	file, err := os.Open(tempPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, _, status, err := httpclient.SimplePut(ctx, pm.httpClient(), fileURL, file,
		httpclient.WithHeader("X-ClusterF-Internal", "1"),
		httpclient.WithHeader("X-Forwarded-From", string(pm.deps.NodeID)),
		httpclient.WithHeader("X-ClusterF-Metadata", encodedMeta),
		httpclient.WithHeader("X-ClusterF-Modified-At", meta.ModifiedAt.Format(time.RFC3339)),
		httpclient.WithHeader("Content-Type", meta.ContentType),
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
	types.Assertf(err == nil, "failed to build URL for %s: %v", meta.Path, err)

	modified := meta.ModifiedAt
	types.Assertf(!modified.IsZero(), "modified time must not be zero for %s", meta.Path)

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

func (pm *PartitionManager) fetchFileToTemp(ctx context.Context, holder types.NodeID, path string, expectedChecksum string) (string, int64, error) {
	tmp, err := os.CreateTemp("", "clusterf-sync-*")
	if err != nil {
		return "", 0, err
	}
	defer func() {
		if err != nil {
			os.Remove(tmp.Name())
		}
	}()

	hasher := sha256.New()

	peer := pm.getPeer(holder)
	types.Assertf(peer != nil, "peer %v not found", holder)

	reader, err := pm.fetchFileStreamFromPeer(ctx, peer, path)
	if err != nil {
		return "", 0, err
	}
	defer reader.Close()

	n64, err := io.Copy(io.MultiWriter(tmp, hasher), reader)
	types.Assertf(err == nil, "failed to copy file %s: %v", path, err)

	closeErr := tmp.Close()
	types.Assertf(closeErr == nil, "failed to close temp file for %s: %v", path, closeErr)

	return tmp.Name(), n64, nil
}
