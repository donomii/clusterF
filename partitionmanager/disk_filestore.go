package partitionmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdFs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/donomii/clusterF/metrics"
	"github.com/donomii/clusterF/types"
)

// DiskFileStore persists metadata and file contents directly on disk using
// two top-level directories: metadata/ and contents/. Each stored key is
// mapped to a relative path underneath those directories.
type DiskFileStore struct {
	baseDir     string // Holds the metadata and content directories
	metadataDir string // Holds the partitions directories containing metadata
	contentDir  string // Holds the partitions directories containing content
	controlDir  string // Holds per-partition control files (timestamps, etc)

	encryptionKey []byte // XOR encryption key (nil = no encryption)
}

const (
	lastReindexTimestampFile = "last_reindex_start.txt"
	lastSyncTimestampFile    = "last_sync_start.txt"
	lastUpdateTimestampFile  = "last_update.txt"
)

// NewDiskFileStore creates a new disk-backed filestore rooted at baseDir.
func NewDiskFileStore(baseDir string) *DiskFileStore {
	store := &DiskFileStore{
		baseDir:     baseDir,
		metadataDir: filepath.Join(baseDir, "metadata"),
		contentDir:  filepath.Join(baseDir, "contents"),
		controlDir:  filepath.Join(baseDir, "controlfiles"),
	}
	_ = os.MkdirAll(store.metadataDir, 0o755)
	_ = os.MkdirAll(store.contentDir, 0o755)
	_ = os.MkdirAll(store.controlDir, 0o755)
	return store
}

// Close implements FileStoreLike; nothing to clean up for plain disk storage.
func (fs *DiskFileStore) Close() {}

// SetEncryptionKey configures the XOR encryption key.
func (fs *DiskFileStore) SetEncryptionKey(key []byte) {
	fs.encryptionKey = key
}

// xorEncrypt performs XOR encryption/decryption on data.
func (fs *DiskFileStore) xorEncrypt(data []byte) []byte {
	if len(fs.encryptionKey) == 0 || len(data) == 0 {
		return data
	}
	result := make([]byte, len(data))
	for i := range data {
		result[i] = data[i] ^ fs.encryptionKey[i%len(fs.encryptionKey)]
	}
	return result
}

func (fs *DiskFileStore) encrypt(metadata, content []byte) ([]byte, []byte) {
	if len(fs.encryptionKey) == 0 {
		return metadata, content
	}
	return fs.xorEncrypt(metadata), fs.xorEncrypt(content)
}

func (fs *DiskFileStore) decrypt(metadata, content []byte) ([]byte, []byte) {
	if len(fs.encryptionKey) == 0 {
		return metadata, content
	}
	return fs.xorEncrypt(metadata), fs.xorEncrypt(content)
}

func (fs *DiskFileStore) partitionTimestampPath(partitionID types.PartitionID, filename string) (string, error) {
	partitionPath, err := partitionDirectoryPath(partitionID)
	if err != nil {
		return "", err
	}
	return filepath.Join(fs.controlDir, partitionPath, filename), nil
}

func (fs *DiskFileStore) writePartitionTimestamp(partitionID types.PartitionID, filename string, ts time.Time) error {
	path, err := fs.partitionTimestampPath(partitionID, filename)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(ts.Format(time.RFC3339Nano)+"\n"), 0o644)
}

func (fs *DiskFileStore) readPartitionTimestamp(partitionID types.PartitionID, filename string) (time.Time, error) {
	path, err := fs.partitionTimestampPath(partitionID, filename)
	if err != nil {
		return time.Time{}, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(b)))
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}

func (fs *DiskFileStore) recordPartitionUpdateTimestamp(path string, ts time.Time) error {
	partitionID := types.PartitionIDForPath(path)
	return fs.writePartitionTimestamp(partitionID, lastUpdateTimestampFile, ts)
}

// Get loads both metadata and content for a path.
func (fs *DiskFileStore) Get(path string) ([]byte, []byte, bool, error) {
	defer metrics.StartGlobalTimer("disk_filestore.get")()
	metrics.IncrementGlobalCounter("disk_filestore.get.calls")

	metaPath, err := fs.metadataPath(path)
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.get.errors")
		return nil, nil, false, err
	}
	contentPath, err := fs.contentPath(path)
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.get.errors")
		return nil, nil, false, err
	}

	metadata, metaErr := os.ReadFile(metaPath)
	content, contentErr := os.ReadFile(contentPath)

	if errors.Is(metaErr, os.ErrNotExist) && errors.Is(contentErr, os.ErrNotExist) {
		metrics.IncrementGlobalCounter("disk_filestore.get.notfound")
		return nil, nil, false, fmt.Errorf("not found: %s", path)
	}

	if metaErr != nil && !errors.Is(metaErr, os.ErrNotExist) {
		metrics.IncrementGlobalCounter("disk_filestore.get.errors")
		return nil, nil, false, metaErr
	}
	if contentErr != nil && !errors.Is(contentErr, os.ErrNotExist) {
		metrics.IncrementGlobalCounter("disk_filestore.get.errors")
		return nil, nil, false, contentErr
	}

	metadata, content = fs.decrypt(metadata, content)
	metrics.IncrementGlobalCounter("disk_filestore.get.success")
	metrics.AddGlobalCounter("disk_filestore.get.metadata_bytes", int64(len(metadata)))
	metrics.AddGlobalCounter("disk_filestore.get.content_bytes", int64(len(content)))

	return metadata, content, true, nil
}

// GetMetadata loads only metadata for a path.
func (fs *DiskFileStore) GetMetadata(path string) ([]byte, error) {
	defer metrics.StartGlobalTimer("disk_filestore.get_metadata")()
	metrics.IncrementGlobalCounter("disk_filestore.get_metadata.calls")

	metaPath, err := fs.metadataPath(path)
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.get_metadata.errors")
		return nil, err
	}
	data, readErr := os.ReadFile(metaPath)
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			metrics.IncrementGlobalCounter("disk_filestore.get_metadata.notfound")
			return nil, fmt.Errorf("not found: %s", path)
		}
		metrics.IncrementGlobalCounter("disk_filestore.get_metadata.errors")
		return nil, readErr
	}
	metadata, _ := fs.decrypt(data, nil)
	metrics.IncrementGlobalCounter("disk_filestore.get_metadata.success")
	metrics.AddGlobalCounter("disk_filestore.get_metadata.bytes", int64(len(metadata)))
	return metadata, nil
}

// GetContent loads only content for a path.
func (fs *DiskFileStore) GetContent(path string) ([]byte, error) {
	defer metrics.StartGlobalTimer("disk_filestore.get_content")()
	metrics.IncrementGlobalCounter("disk_filestore.get_content.calls")

	contentPath, err := fs.contentPath(path)
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.get_content.errors")
		return nil, err
	}
	data, readErr := os.ReadFile(contentPath)
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			metrics.IncrementGlobalCounter("disk_filestore.get_content.notfound")
			return nil, fmt.Errorf("not found: %s", path)
		}
		metrics.IncrementGlobalCounter("disk_filestore.get_content.errors")
		return nil, readErr
	}
	_, content := fs.decrypt(nil, data)
	metrics.IncrementGlobalCounter("disk_filestore.get_content.success")
	metrics.AddGlobalCounter("disk_filestore.get_content.bytes", int64(len(content)))
	return content, nil
}

// Put stores both metadata and content.
func (fs *DiskFileStore) Put(path string, metadata, content []byte) error {
	defer metrics.StartGlobalTimer("disk_filestore.put")()
	metrics.IncrementGlobalCounter("disk_filestore.put.calls")

	metaPath, err := fs.metadataPath(path)
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.put.errors")
		return err
	}
	contentPath, err := fs.contentPath(path)
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.put.errors")
		return err
	}

	partitionID := types.PartitionIDForPath(path)

	if err := ensureParentDir(metaPath); err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.put.errors")
		return err
	}
	if err := ensureParentDir(contentPath); err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.put.errors")
		return err
	}

	var meta types.FileMetadata
	if err := json.Unmarshal(metadata, &meta); err != nil {
		meta = types.FileMetadata{}
	}
	modTime := meta.ModifiedAt

	encMetadata, encContent := fs.encrypt(metadata, content)

	if err := os.WriteFile(metaPath, encMetadata, 0o644); err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.put.errors")
		return err
	}
	if !modTime.IsZero() {
		_ = os.Chtimes(metaPath, modTime, modTime)
	}
	if err := os.WriteFile(contentPath, encContent, 0o644); err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.put.errors")
		return err
	}
	if !modTime.IsZero() {
		_ = os.Chtimes(contentPath, modTime, modTime)
	}
	writeTime := time.Now()
	if err := fs.writePartitionTimestamp(partitionID, lastUpdateTimestampFile, writeTime); err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.put.errors")
		return err
	}
	metrics.IncrementGlobalCounter("disk_filestore.put.success")
	metrics.AddGlobalCounter("disk_filestore.put.metadata_bytes", int64(len(metadata)))
	metrics.AddGlobalCounter("disk_filestore.put.content_bytes", int64(len(content)))
	return nil
}

// PutMetadata stores metadata only.
func (fs *DiskFileStore) PutMetadata(path string, metadata []byte) error {
	defer metrics.StartGlobalTimer("disk_filestore.put_metadata")()
	metrics.IncrementGlobalCounter("disk_filestore.put_metadata.calls")

	metaPath, err := fs.metadataPath(path)
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.put_metadata.errors")
		return err
	}
	partitionID := types.PartitionIDForPath(path)
	if err := ensureParentDir(metaPath); err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.put_metadata.errors")
		return err
	}
	encMetadata, _ := fs.encrypt(metadata, nil)
	if err := os.WriteFile(metaPath, encMetadata, 0o644); err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.put_metadata.errors")
		return err
	}
	if err := fs.writePartitionTimestamp(partitionID, lastUpdateTimestampFile, time.Now()); err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.put_metadata.errors")
		return err
	}

	var meta types.FileMetadata
	if err := json.Unmarshal(metadata, &meta); err != nil {
		return nil
	}
	if meta.ModifiedAt.IsZero() {
		return nil
	}
	_ = os.Chtimes(metaPath, meta.ModifiedAt, meta.ModifiedAt)
	metrics.IncrementGlobalCounter("disk_filestore.put_metadata.success")
	metrics.AddGlobalCounter("disk_filestore.put_metadata.bytes", int64(len(metadata)))
	return nil
}

// Delete removes metadata and content for a path.
func (fs *DiskFileStore) Delete(path string) error {
	defer metrics.StartGlobalTimer("disk_filestore.delete")()
	metrics.IncrementGlobalCounter("disk_filestore.delete.calls")

	metaPath, err := fs.metadataPath(path)
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.delete.errors")
		return err
	}
	contentPath, err := fs.contentPath(path)
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.delete.errors")
		return err
	}
	partitionID := types.PartitionIDForPath(path)

	if removeErr := os.Remove(metaPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		metrics.IncrementGlobalCounter("disk_filestore.delete.errors")
		return removeErr
	}
	if removeErr := os.Remove(contentPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		metrics.IncrementGlobalCounter("disk_filestore.delete.errors")
		return removeErr
	}
	if err := fs.writePartitionTimestamp(partitionID, lastUpdateTimestampFile, time.Now()); err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.delete.errors")
		return err
	}
	metrics.IncrementGlobalCounter("disk_filestore.delete.success")
	return nil
}

// Scan iterates all entries matching the provided filters and provides metadata and content.
func (fs *DiskFileStore) Scan(pathPrefix string, fn func(path string, metadata, content []byte) error) error {
	checkForRecursiveScan()

	defer metrics.StartGlobalTimer("disk_filestore.scan")()
	metrics.IncrementGlobalCounter("disk_filestore.scan.calls")

	err := fs.walkMetadataFiles(func(path, metaPath string) error {
		if pathPrefix != "" && !strings.HasPrefix(path, pathPrefix) {
			return nil
		}

		metadata, err := os.ReadFile(metaPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}

		contentPath, pathErr := fs.contentPath(path)
		if pathErr != nil {
			return pathErr
		}

		content, err := os.ReadFile(contentPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		metadata, content = fs.decrypt(metadata, content)
		metrics.AddGlobalCounter("disk_filestore.scan.entries", 1)
		metrics.AddGlobalCounter("disk_filestore.scan.metadata_bytes", int64(len(metadata)))
		metrics.AddGlobalCounter("disk_filestore.scan.content_bytes", int64(len(content)))
		if err := fn(path, metadata, content); err != nil {
			metrics.IncrementGlobalCounter("disk_filestore.scan.errors")
			return err
		}
		return nil
	})
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.scan.errors")
		return err
	}

	metrics.IncrementGlobalCounter("disk_filestore.scan.success")
	return nil
}

// ScanMetadata iterates metadata entries and passes the path to fn.
func (fs *DiskFileStore) ScanMetadata(pathPrefix string, fn func(path string, metadata []byte) error) error {
	checkForRecursiveScan()

	defer metrics.StartGlobalTimer("disk_filestore.scan_metadata")()
	metrics.IncrementGlobalCounter("disk_filestore.scan_metadata.calls")

	err := fs.walkMetadataFiles(func(path, metaPath string) error {
		if pathPrefix != "" && !strings.HasPrefix(path, pathPrefix) {
			return nil
		}

		metadata, err := os.ReadFile(metaPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}

		metadata, _ = fs.decrypt(metadata, nil)
		metrics.AddGlobalCounter("disk_filestore.scan_metadata.entries", 1)
		metrics.AddGlobalCounter("disk_filestore.scan_metadata.bytes", int64(len(metadata)))
		if err := fn(path, metadata); err != nil {
			metrics.IncrementGlobalCounter("disk_filestore.scan_metadata.errors")
			return err
		}
		return nil
	})
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.scan_metadata.errors")
		return err
	}

	metrics.IncrementGlobalCounter("disk_filestore.scan_metadata.success")
	return nil
}

// ScanMetadataPartition scans only files belonging to a specific partition
func (fs *DiskFileStore) ScanMetadataPartition(ctx context.Context, partitionID types.PartitionID, fn func(path string, metadata []byte) error) error {
	checkForRecursiveScan()
	defer metrics.StartGlobalTimer("disk_filestore.scan_metadata_partition")()
	metrics.IncrementGlobalCounter("disk_filestore.scan_metadata_partition.calls")

	// Get the partition directory path
	partitionPath, err := partitionDirectoryPath(partitionID)
	if err != nil {
		metrics.IncrementGlobalCounter("disk_filestore.scan_metadata_partition.errors")
		return err
	}
	partitionDir := filepath.Join(fs.metadataDir, partitionPath)

	// Check if partition directory exists
	if _, err := os.Stat(partitionDir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			metrics.IncrementGlobalCounter("disk_filestore.scan_metadata_partition.success")
			return nil
		}
		metrics.IncrementGlobalCounter("disk_filestore.scan_metadata_partition.errors")
		return err
	}

	// Walk only this specific partition directory
	err = filepath.WalkDir(partitionDir, func(filePath string, d stdFs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			metrics.IncrementGlobalCounter("disk_filestore.scan_metadata_partition.errors")
			return walkErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if d.IsDir() {
			return nil
		}

		// Get relative path from partition directory to reconstruct original path
		rel, err := filepath.Rel(partitionDir, filePath)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		// Reconstruct the original cluster path
		originalPath := "/" + strings.ReplaceAll(rel, string(filepath.Separator), "/")

		metadata, err := os.ReadFile(filePath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			metrics.IncrementGlobalCounter("disk_filestore.scan_metadata_partition.errors")
			return err
		}

		metadata, _ = fs.decrypt(metadata, nil)
		metrics.AddGlobalCounter("disk_filestore.scan_metadata_partition.entries", 1)
		metrics.AddGlobalCounter("disk_filestore.scan_metadata_partition.bytes", int64(len(metadata)))
		if err := fn(originalPath, metadata); err != nil {
			metrics.IncrementGlobalCounter("disk_filestore.scan_metadata_partition.errors")
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	metrics.IncrementGlobalCounter("disk_filestore.scan_metadata_partition.success")
	return nil
}

// GetAllPartitionStores returns all known partition IDs.
func (fs *DiskFileStore) GetAllPartitionStores() ([]types.PartitionStore, error) {
	defer metrics.StartGlobalTimer("disk_filestore.list_partitions")()
	metrics.IncrementGlobalCounter("disk_filestore.list_partitions.calls")

	partitionSet := make(map[types.PartitionStore]struct{})

	// Walk the hierarchical directory structure: p1/p2/p3/p12345/
	p1Entries, err := os.ReadDir(fs.metadataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			metrics.IncrementGlobalCounter("disk_filestore.list_partitions.success")
			return []types.PartitionStore{}, nil
		}
		metrics.IncrementGlobalCounter("disk_filestore.list_partitions.errors")
		return nil, err
	}

	for _, p1 := range p1Entries {
		if !p1.IsDir() || len(p1.Name()) != 1 {
			continue
		}

		p2Dir := filepath.Join(fs.metadataDir, p1.Name())
		p2Entries, err := os.ReadDir(p2Dir)
		if err != nil {
			continue
		}

		for _, p2 := range p2Entries {
			if !p2.IsDir() || len(p2.Name()) != 1 {
				continue
			}

			p3Dir := filepath.Join(p2Dir, p2.Name())
			p3Entries, err := os.ReadDir(p3Dir)
			if err != nil {
				continue
			}

			for _, p3 := range p3Entries {
				if !p3.IsDir() || len(p3.Name()) != 1 {
					continue
				}

				partitionDir := filepath.Join(p3Dir, p3.Name())
				partitionEntries, err := os.ReadDir(partitionDir)
				if err != nil {
					continue
				}

				for _, partition := range partitionEntries {
					if !partition.IsDir() || !strings.HasPrefix(partition.Name(), "p") {
						continue
					}

					// The partition directory name IS the partition store ID
					partitionSet[types.PartitionStore(partition.Name())] = struct{}{}
				}
			}
		}
	}

	partitions := make([]types.PartitionStore, 0, len(partitionSet))
	for p := range partitionSet {
		partitions = append(partitions, p)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	metrics.IncrementGlobalCounter("disk_filestore.list_partitions.success")
	metrics.AddGlobalCounter("disk_filestore.list_partitions.count", int64(len(partitions)))
	return partitions, nil
}

// walkMetadataFiles executes fn for every metadata file.
func (fs *DiskFileStore) walkMetadataFiles(fn func(path, metaPath string) error) error {
	info, err := os.Stat(fs.metadataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("metadata path is not a directory: %s", fs.metadataDir)
	}

	return filepath.WalkDir(fs.metadataDir, func(path string, d stdFs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}

		if d.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(fs.metadataDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		// Extract original file path from hierarchical structure
		// Path format: p1/p2/p3/p12345/original/file/path
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) < 5 {
			return fmt.Errorf("malformed partition path %s: expected at least 5 parts, got %d", rel, len(parts))
		}

		// Enforce hierarchical structure (p1/p2/p3/p12345/...)
		if len(parts[0]) != 1 || len(parts[1]) != 1 || len(parts[2]) != 1 || !strings.HasPrefix(parts[3], "p") {
			return fmt.Errorf("malformed partition path %s: expected format p1/p2/p3/p12345/...", rel)
		}

		// Reconstruct original file path from parts[4:]
		originalPath := "/" + strings.Join(parts[4:], "/")

		return fn(originalPath, path)
	})
}

// metadataPath returns the absolute path for a metadata entry.
func (fs *DiskFileStore) metadataPath(path string) (string, error) {
	relative, err := relativePathFromCluster(path)
	if err != nil {
		return "", err
	}
	return filepath.Join(fs.metadataDir, relative), nil
}

// contentPath returns the absolute path for a content entry.
func (fs *DiskFileStore) contentPath(path string) (string, error) {
	relative, err := relativePathFromCluster(path)
	if err != nil {
		return "", err
	}
	return filepath.Join(fs.contentDir, relative), nil
}

// relativePathFromCluster sanitises cluster paths and produces a hierarchical partition-based path.
func relativePathFromCluster(filePath string) (string, error) {
	if filePath == "" {
		return "", fmt.Errorf("empty file path")
	}
	if !strings.HasPrefix(filePath, "/") {
		return "", fmt.Errorf("path must be absolute: %s", filePath)
	}

	cleaned := filepath.Clean(filePath)
	cleaned = strings.TrimPrefix(cleaned, "/")
	cleaned = strings.TrimPrefix(cleaned, string(filepath.Separator))
	if cleaned == "" || cleaned == "." {
		return "", fmt.Errorf("invalid file path: %s", filePath)
	}

	segments := strings.Split(filepath.ToSlash(cleaned), "/")
	builder := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("invalid relative path derived from %s", filePath)
		}
		builder = append(builder, segment)
	}

	// Calculate partition for the file
	partitionID := types.PartitionIDForPath(filePath)

	// Get partition directory path
	partitionPath, err := partitionDirectoryPath(partitionID)
	if err != nil {
		return "", err
	}

	// Build hierarchical path: p1/2/3/p12345/original/file/path
	hierarchicalPath := filepath.Join(partitionPath, strings.Join(builder, "/"))

	return hierarchicalPath, nil
}

// partitionDirectoryPath returns the directory path for a specific partition
func partitionDirectoryPath(partitionID types.PartitionID) (string, error) {
	if len(partitionID) < 6 { // Should be like "p12345"
		return "", fmt.Errorf("invalid partition ID: %s", partitionID)
	}

	// Extract digits: p12345 -> 1, 2, 3, p12345
	p1 := string(partitionID[1])
	p2 := string(partitionID[2])
	p3 := string(partitionID[3])

	// Build hierarchical path
	return filepath.Join(p1, p2, p3, string(partitionID)), nil
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	return os.MkdirAll(dir, 0o755)
}
