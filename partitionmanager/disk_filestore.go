package partitionmanager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	stdFs "io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/donomii/clusterF/types"
)

// DiskFileStore persists metadata and file contents directly on disk using
// two top-level directories: metadata/ and contents/. Each stored key is
// mapped to a relative path underneath those directories.
type DiskFileStore struct {
	baseDir     string
	metadataDir string
	contentDir  string

	encryptionKey []byte // XOR encryption key (nil = no encryption)
}

// NewDiskFileStore creates a new disk-backed filestore rooted at baseDir.
func NewDiskFileStore(baseDir string) *DiskFileStore {
	store := &DiskFileStore{
		baseDir:     baseDir,
		metadataDir: filepath.Join(baseDir, "metadata"),
		contentDir:  filepath.Join(baseDir, "contents"),
	}
	_ = os.MkdirAll(store.metadataDir, 0o755)
	_ = os.MkdirAll(store.contentDir, 0o755)
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

// Get loads both metadata and content for a partition/path pair.
func (fs *DiskFileStore) Get(partition types.PartitionID, path string) (*types.FileData, error) {
	if err := ensurePartitionMatchesPath(partition, path); err != nil {
		return nil, err
	}

	metaPath, err := fs.metadataPath(path)
	if err != nil {
		return nil, err
	}
	contentPath, err := fs.contentPath(path)
	if err != nil {
		return nil, err
	}

	metadata, metaErr := os.ReadFile(metaPath)
	content, contentErr := os.ReadFile(contentPath)

	if errors.Is(metaErr, os.ErrNotExist) && errors.Is(contentErr, os.ErrNotExist) {
		return &types.FileData{Partition: partition, Path: path, Exists: false}, fmt.Errorf("not found: %s", path)
	}

	if metaErr != nil && !errors.Is(metaErr, os.ErrNotExist) {
		return nil, metaErr
	}
	if contentErr != nil && !errors.Is(contentErr, os.ErrNotExist) {
		return nil, contentErr
	}

	metadata, content = fs.decrypt(metadata, content)

	return &types.FileData{
		Partition: partition,
		Path:      path,
		Metadata:  metadata,
		Content:   content,
		Exists:    true,
	}, nil
}

// GetMetadata loads only metadata for a partition/path pair.
func (fs *DiskFileStore) GetMetadata(partition types.PartitionID, path string) ([]byte, error) {
	if err := ensurePartitionMatchesPath(partition, path); err != nil {
		return nil, err
	}

	metaPath, err := fs.metadataPath(path)
	if err != nil {
		return nil, err
	}
	data, readErr := os.ReadFile(metaPath)
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			return nil, fmt.Errorf("not found: %s", path)
		}
		return nil, readErr
	}
	return fs.xorEncrypt(data), nil
}

// GetContent loads only content for a partition/path pair.
func (fs *DiskFileStore) GetContent(partition types.PartitionID, path string) ([]byte, error) {
	if err := ensurePartitionMatchesPath(partition, path); err != nil {
		return nil, err
	}

	contentPath, err := fs.contentPath(path)
	if err != nil {
		return nil, err
	}
	data, readErr := os.ReadFile(contentPath)
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			return nil, fmt.Errorf("not found: %s", path)
		}
		return nil, readErr
	}
	return fs.xorEncrypt(data), nil
}

// Put stores both metadata and content.
func (fs *DiskFileStore) Put(partition types.PartitionID, path string, metadata, content []byte) error {
	if err := ensurePartitionMatchesPath(partition, path); err != nil {
		return err
	}

	metaPath, err := fs.metadataPath(path)
	if err != nil {
		return err
	}
	contentPath, err := fs.contentPath(path)
	if err != nil {
		return err
	}

	if err := ensureParentDir(metaPath); err != nil {
		return err
	}
	if err := ensureParentDir(contentPath); err != nil {
		return err
	}

	var meta types.FileMetadata
	if err := json.Unmarshal(metadata, &meta); err != nil {
		meta = types.FileMetadata{}
	}
	modTime := meta.ModifiedAt

	encMetadata, encContent := fs.encrypt(metadata, content)

	if err := os.WriteFile(metaPath, encMetadata, 0o644); err != nil {
		return err
	}
	if !modTime.IsZero() {
		_ = os.Chtimes(metaPath, modTime, modTime)
	}
	if err := os.WriteFile(contentPath, encContent, 0o644); err != nil {
		return err
	}
	if !modTime.IsZero() {
		_ = os.Chtimes(contentPath, modTime, modTime)
	}
	return nil
}

// PutMetadata stores metadata only.
func (fs *DiskFileStore) PutMetadata(partition types.PartitionID, path string, metadata []byte) error {
	if err := ensurePartitionMatchesPath(partition, path); err != nil {
		return err
	}

	metaPath, err := fs.metadataPath(path)
	if err != nil {
		return err
	}
	if err := ensureParentDir(metaPath); err != nil {
		return err
	}
	encMetadata, _ := fs.encrypt(metadata, nil)
	if err := os.WriteFile(metaPath, encMetadata, 0o644); err != nil {
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
	return nil
}

// Delete removes metadata and content for a partition/path pair.
func (fs *DiskFileStore) Delete(partition types.PartitionID, path string) error {
	if err := ensurePartitionMatchesPath(partition, path); err != nil {
		return err
	}

	metaPath, err := fs.metadataPath(path)
	if err != nil {
		return err
	}
	contentPath, err := fs.contentPath(path)
	if err != nil {
		return err
	}

	if removeErr := os.Remove(metaPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return removeErr
	}
	if removeErr := os.Remove(contentPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return removeErr
	}
	return nil
}

// Scan iterates all entries matching the provided filters and provides metadata and content.
func (fs *DiskFileStore) Scan(partition types.PartitionID, pathPrefix string, fn func(partition types.PartitionID, path string, metadata, content []byte) error) error {
	checkForRecursiveScan()

	return fs.walkMetadataFiles(func(path, metaPath string) error {
		if pathPrefix != "" && !strings.HasPrefix(path, pathPrefix) {
			return nil
		}

		part := HashToPartition(path)
		if partition != "" && part != partition {
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
		return fn(part, path, metadata, content)
	})
}

// ScanMetadata iterates metadata entries and passes the partition/path to fn.
func (fs *DiskFileStore) ScanMetadata(partition types.PartitionID, pathPrefix string, fn func(partition types.PartitionID, path string, metadata []byte) error) error {
	checkForRecursiveScan()

	return fs.walkMetadataFiles(func(path, metaPath string) error {
		if pathPrefix != "" && !strings.HasPrefix(path, pathPrefix) {
			return nil
		}

		part := HashToPartition(path)
		if partition != "" && part != partition {
			return nil
		}

		metadata, err := os.ReadFile(metaPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}

		return fn(part, path, fs.xorEncrypt(metadata))
	})
}

// ScanMetadataFullKeys iterates metadata entries and passes partition/path to fn.
func (fs *DiskFileStore) ScanMetadataFullKeys(partition types.PartitionID, pathPrefix string, fn func(partition types.PartitionID, path string, metadata []byte) error) error {
	return fs.ScanMetadata(partition, pathPrefix, fn)
}

// ScanPartitionMetaData iterates metadata for a specific partition store.
func (fs *DiskFileStore) ScanPartitionMetaData(partitionStore types.PartitionStore, fn func(partition types.PartitionID, path string, metadata []byte) error) error {
	return fs.walkMetadataFiles(func(path, metaPath string) error {
		partitionID := HashToPartition(path)
		if types.ExtractPartitionStoreID(partitionID) != partitionStore {
			return nil
		}

		metadata, err := os.ReadFile(metaPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}

		return fn(partitionID, path, fs.xorEncrypt(metadata))
	})
}

// getAllPartitionStores returns all known partition store IDs.
func (fs *DiskFileStore) getAllPartitionStores(partition types.PartitionID) ([]types.PartitionStore, error) {
	if partition != "" {
		if store := types.ExtractPartitionStoreID(partition); store != "" {
			return []types.PartitionStore{store}, nil
		}
	}

	partitionSet := make(map[types.PartitionStore]struct{})
	err := fs.walkMetadataFiles(func(path, _ string) error {
		partitionID := HashToPartition(path)
		partitionSet[types.ExtractPartitionStoreID(partitionID)] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}

	partitions := make([]types.PartitionStore, 0, len(partitionSet))
	for p := range partitionSet {
		partitions = append(partitions, p)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	return partitions, nil
}

// CalculatePartitionChecksum reproduces the checksum calculation used by the existing store.
func (fs *DiskFileStore) CalculatePartitionChecksum(ctx context.Context, partition types.PartitionID, pathPrefix string) (string, error) {
	type entry struct {
		partition types.PartitionID
		path      string
		metadata  []byte
		content   []byte
	}

	var entries []entry

	err := fs.Scan(partition, pathPrefix, func(part types.PartitionID, path string, metadata, content []byte) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var parsedMetadata types.FileMetadata
		if err := json.Unmarshal(metadata, &parsedMetadata); err == nil {
			if !parsedMetadata.Deleted {
				metaCopy := append([]byte(nil), metadata...)
				contentCopy := append([]byte(nil), content...)
				entries = append(entries, entry{
					partition: part,
					path:      path,
					metadata:  metaCopy,
					content:   contentCopy,
				})
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].partition == entries[j].partition {
			return entries[i].path < entries[j].path
		}
		return entries[i].partition < entries[j].partition
	})

	hash := sha256.New()
	for _, e := range entries {
		hash.Write([]byte(e.partition))
		hash.Write([]byte{storeKeySeparator})
		hash.Write([]byte(e.path))
		hash.Write(e.metadata)
		hash.Write(e.content)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
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

		filePath := "/" + filepath.ToSlash(rel)
		return fn(filePath, path)
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

// relativePathFromCluster sanitises cluster paths and produces a safe relative path.
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

	return filepath.FromSlash(strings.Join(builder, "/")), nil
}

func ensurePartitionMatchesPath(partition types.PartitionID, path string) error {
	if partition == "" {
		return fmt.Errorf("empty partition id for path %s", path)
	}
	if path == "" {
		return fmt.Errorf("empty path for partition %s", partition)
	}
	expected := HashToPartition(path)
	if expected != partition {
		return fmt.Errorf("partition mismatch for path %s: expected %s, got %s", path, expected, partition)
	}
	return nil
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	return os.MkdirAll(dir, 0o755)
}
