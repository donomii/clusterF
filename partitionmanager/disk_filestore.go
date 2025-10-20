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

	// Keep method signature compatibility; encryption is not applied in this implementation.
	encryptionKey []byte
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

// SetEncryptionKey accepts the interface method but does not apply encryption.
func (fs *DiskFileStore) SetEncryptionKey(key []byte) {
	fs.encryptionKey = key
}

// Get loads both metadata and content for a key.
func (fs *DiskFileStore) Get(key string) (*types.FileData, error) {
	metaPath, err := fs.metadataPath(key)
	if err != nil {
		return nil, err
	}
	contentPath, err := fs.contentPath(key)
	if err != nil {
		return nil, err
	}

	metadata, metaErr := os.ReadFile(metaPath)
	content, contentErr := os.ReadFile(contentPath)

	if errors.Is(metaErr, os.ErrNotExist) && errors.Is(contentErr, os.ErrNotExist) {
		return &types.FileData{Key: key, Exists: false}, fmt.Errorf("not found: %s", key)
	}

	if metaErr != nil && !errors.Is(metaErr, os.ErrNotExist) {
		return nil, metaErr
	}
	if contentErr != nil && !errors.Is(contentErr, os.ErrNotExist) {
		return nil, contentErr
	}

	return &types.FileData{
		Key:      key,
		Metadata: metadata,
		Content:  content,
		Exists:   true,
	}, nil
}

// GetMetadata loads only metadata for a key.
func (fs *DiskFileStore) GetMetadata(key string) ([]byte, error) {
	metaPath, err := fs.metadataPath(key)
	if err != nil {
		return nil, err
	}
	data, readErr := os.ReadFile(metaPath)
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			return nil, fmt.Errorf("not found: %s", key)
		}
		return nil, readErr
	}
	return data, nil
}

// GetContent loads only content for a key.
func (fs *DiskFileStore) GetContent(key string) ([]byte, error) {
	contentPath, err := fs.contentPath(key)
	if err != nil {
		return nil, err
	}
	data, readErr := os.ReadFile(contentPath)
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			return nil, fmt.Errorf("not found: %s", key)
		}
		return nil, readErr
	}
	return data, nil
}

// Put stores both metadata and content.
func (fs *DiskFileStore) Put(key string, metadata, content []byte) error {
	metaPath, err := fs.metadataPath(key)
	if err != nil {
		return err
	}
	contentPath, err := fs.contentPath(key)
	if err != nil {
		return err
	}

	if err := ensureParentDir(metaPath); err != nil {
		return err
	}
	if err := ensureParentDir(contentPath); err != nil {
		return err
	}

	if err := os.WriteFile(metaPath, metadata, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(contentPath, content, 0o644); err != nil {
		return err
	}
	return nil
}

// PutMetadata stores metadata only.
func (fs *DiskFileStore) PutMetadata(key string, metadata []byte) error {
	metaPath, err := fs.metadataPath(key)
	if err != nil {
		return err
	}
	if err := ensureParentDir(metaPath); err != nil {
		return err
	}
	return os.WriteFile(metaPath, metadata, 0o644)
}

// Delete removes metadata and content for a key.
func (fs *DiskFileStore) Delete(key string) error {
	metaPath, err := fs.metadataPath(key)
	if err != nil {
		return err
	}
	contentPath, err := fs.contentPath(key)
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

// Scan iterates all keys matching prefix and provides metadata and content.
func (fs *DiskFileStore) Scan(prefix string, fn func(key string, metadata, content []byte) error) error {
	checkForRecursiveScan()

	return fs.walkMetadataFiles(func(key, metaPath string) error {
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}

		metadata, err := os.ReadFile(metaPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}

		contentPath, pathErr := fs.contentPath(key)
		if pathErr != nil {
			return pathErr
		}

		content, err := os.ReadFile(contentPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		return fn(key, metadata, content)
	})
}

// ScanMetadata iterates metadata entries and passes the file path (without partition prefix) to fn.
func (fs *DiskFileStore) ScanMetadata(prefix string, fn func(key string, metadata []byte) error) error {
	checkForRecursiveScan()

	return fs.walkMetadataFiles(func(key, metaPath string) error {
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}

		metadata, err := os.ReadFile(metaPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}

		return fn(types.ExtractFilePath(key), metadata)
	})
}

// ScanMetadataFullKeys iterates metadata entries and passes the full key to fn.
func (fs *DiskFileStore) ScanMetadataFullKeys(prefix string, fn func(key string, metadata []byte) error) error {
	checkForRecursiveScan()

	return fs.walkMetadataFiles(func(key, metaPath string) error {
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}

		metadata, err := os.ReadFile(metaPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}

		return fn(key, metadata)
	})
}

// ScanPartitionMetaData iterates metadata for a specific partition store.
func (fs *DiskFileStore) ScanPartitionMetaData(partitionStore types.PartitionStore, fn func(key []byte, metadata []byte) error) error {
	return fs.walkMetadataFiles(func(key, metaPath string) error {
		if types.ExtractPartitionStoreID(key) != partitionStore {
			return nil
		}

		metadata, err := os.ReadFile(metaPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}

		return fn([]byte(key), metadata)
	})
}

// getAllPartitionStores returns all known partition store IDs.
func (fs *DiskFileStore) getAllPartitionStores(prefix string) ([]types.PartitionStore, error) {
	if prefix != "" {
		if store := types.ExtractPartitionStoreID(prefix); store != "" {
			return []types.PartitionStore{store}, nil
		}
	}

	partitionSet := make(map[types.PartitionStore]struct{})
	err := fs.walkMetadataFiles(func(key, _ string) error {
		partitionSet[types.ExtractPartitionStoreID(key)] = struct{}{}
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
func (fs *DiskFileStore) CalculatePartitionChecksum(ctx context.Context, prefix string) (string, error) {
	type entry struct {
		key      string
		metadata []byte
		content  []byte
	}

	var entries []entry

	err := fs.Scan(prefix, func(key string, metadata, content []byte) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !strings.HasPrefix(key, prefix) || !strings.Contains(key, ":file:") {
			return nil
		}

		var parsedMetadata types.FileMetadata
		if err := json.Unmarshal(metadata, &parsedMetadata); err == nil {
			if !parsedMetadata.Deleted {
				metaCopy := append([]byte(nil), metadata...)
				contentCopy := append([]byte(nil), content...)
				entries = append(entries, entry{
					key:      key,
					metadata: metaCopy,
					content:  contentCopy,
				})
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	hash := sha256.New()
	for _, e := range entries {
		hash.Write([]byte(e.key))
		hash.Write(e.metadata)
		hash.Write(e.content)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// walkMetadataFiles executes fn for every metadata file.
func (fs *DiskFileStore) walkMetadataFiles(fn func(key, metaPath string) error) error {
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
		key := makeKey(filePath)
		return fn(key, path)
	})
}

// metadataPath returns the absolute path for a metadata entry.
func (fs *DiskFileStore) metadataPath(key string) (string, error) {
	relative, err := keyRelativePath(key)
	if err != nil {
		return "", err
	}
	return filepath.Join(fs.metadataDir, relative), nil
}

// contentPath returns the absolute path for a content entry.
func (fs *DiskFileStore) contentPath(key string) (string, error) {
	relative, err := keyRelativePath(key)
	if err != nil {
		return "", err
	}
	return filepath.Join(fs.contentDir, relative), nil
}

// keyRelativePath sanitises keys and produces a safe relative path.
func keyRelativePath(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("empty key")
	}

	filePath := types.ExtractFilePath(key)
	if filePath == "" {
		return "", fmt.Errorf("empty file path for key %s", key)
	}

	cleaned := filepath.Clean(filePath)
	cleaned = strings.TrimPrefix(cleaned, "/")
	cleaned = strings.TrimPrefix(cleaned, string(filepath.Separator))
	if cleaned == "" || cleaned == "." {
		return "", fmt.Errorf("invalid file path derived from key %s", key)
	}

	segments := strings.Split(filepath.ToSlash(cleaned), "/")
	builder := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return "", fmt.Errorf("invalid relative path derived from key %s", key)
		}
		builder = append(builder, segment)
	}

	return filepath.FromSlash(strings.Join(builder, "/")), nil
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	return os.MkdirAll(dir, 0o755)
}
