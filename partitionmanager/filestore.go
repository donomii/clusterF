package partitionmanager

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"

	ensemblekv "github.com/donomii/ensemblekv"
)

// FileStore provides atomic access to file metadata and content across two KV stores
type FileStore struct {
	metadataKV ensemblekv.KvLike
	contentKV  ensemblekv.KvLike
	mutex      sync.RWMutex
}

// checkForRecursiveScan panics if we detect a recursive scan call
func checkForRecursiveScan() {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	stack := string(buf[:n])

	// Count how many times Scan or ScanMetadata appears in the stack
	scanCount := strings.Count(stack, "(*FileStore).Scan(") + strings.Count(stack, "(*FileStore).ScanMetadata(")

	if scanCount > 1 {
		panic(fmt.Sprintf("RECURSIVE SCAN DETECTED - FileStore scan methods called recursively!\n\nStack trace:\n%s", stack))
	}
}

// FileData represents a complete file entry
type FileData struct {
	Key      string
	Metadata []byte
	Content  []byte
	Exists   bool
}

// NewFileStore creates a new FileStore
func NewFileStore(metadataKV, contentKV ensemblekv.KvLike) *FileStore {
	return &FileStore{
		metadataKV: metadataKV,
		contentKV:  contentKV,
	}
}

// Get retrieves both metadata and content atomically
func (fs *FileStore) Get(key string) (*FileData, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	keyBytes := []byte(key)

	metadata, metaErr := fs.metadataKV.Get(keyBytes)
	content, contentErr := fs.contentKV.Get(keyBytes)

	// If neither exists, file doesn't exist
	if metaErr != nil && contentErr != nil {
		return &FileData{
			Key:    key,
			Exists: false,
		}, nil
	}

	return &FileData{
		Key:      key,
		Metadata: metadata,
		Content:  content,
		Exists:   true,
	}, nil
}

// GetMetadata retrieves only metadata
func (fs *FileStore) GetMetadata(key string) ([]byte, error) {

	return fs.metadataKV.Get([]byte(key))
}

// GetContent retrieves only content
func (fs *FileStore) GetContent(key string) ([]byte, error) {

	return fs.contentKV.Get([]byte(key))
}

// Put stores both metadata and content atomically
func (fs *FileStore) Put(key string, metadata, content []byte) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	keyBytes := []byte(key)

	if err := fs.metadataKV.Put(keyBytes, metadata); err != nil {
		return fmt.Errorf("failed to store metadata: %v", err)
	}

	if err := fs.contentKV.Put(keyBytes, content); err != nil {
		// Try to rollback metadata
		fs.metadataKV.Delete(keyBytes)
		return fmt.Errorf("failed to store content: %v", err)
	}

	return nil
}

// PutMetadata stores only metadata
func (fs *FileStore) PutMetadata(key string, metadata []byte) error {
	return fs.metadataKV.Put([]byte(key), metadata)
}

// Delete removes both metadata and content atomically
func (fs *FileStore) Delete(key string) error {
	checkForRecursiveScan()
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	keyBytes := []byte(key)

	// Delete from both stores - ignore individual errors
	fs.metadataKV.Delete(keyBytes)
	fs.contentKV.Delete(keyBytes)

	return nil
}

// Scan calls fn for each file with the given prefix
func (fs *FileStore) Scan(prefix string, fn func(key string, metadata, content []byte) error) error {
	checkForRecursiveScan()
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	// First collect all matching keys from metadata store
	var keys []string
	var metadataValues [][]byte

	_, err := fs.metadataKV.MapFunc(func(k, v []byte) error {
		keyStr := string(k)
		if prefix == "" || keyStr[:len(prefix)] == prefix {
			keys = append(keys, keyStr)
			metaCopy := make([]byte, len(v))
			copy(metaCopy, v)
			metadataValues = append(metadataValues, metaCopy)
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Then get content for each key and call fn
	for i, key := range keys {
		content, _ := fs.contentKV.Get([]byte(key))

		if err := fn(key, metadataValues[i], content); err != nil {
			return err
		}
	}

	return nil
}

// ScanMetadata calls fn for each metadata entry with the given prefix
func (fs *FileStore) ScanMetadata(prefix string, fn func(key string, metadata []byte) error) error {
	checkForRecursiveScan()
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	_, err := fs.metadataKV.MapFunc(func(k, v []byte) error {
		keyStr := string(k)
		if prefix == "" || keyStr[:len(prefix)] == prefix {
			return fn(keyStr, v)
		}
		return nil
	})
	return err
}

// GetSnapshot creates a consistent snapshot of all files matching prefix
func (fs *FileStore) GetSnapshot(prefix string) ([]*FileData, error) {

	var files []*FileData

	err := fs.Scan(prefix, func(key string, metadata, content []byte) error {
		files = append(files, &FileData{
			Key:      key,
			Metadata: metadata,
			Content:  content,
			Exists:   true,
		})
		return nil
	})

	return files, err
}

// CalculatePartitionChecksum computes a consistent checksum for all files in a partition
func (fs *FileStore) CalculatePartitionChecksum(prefix string) (string, error) {

	// Collect all non-deleted entries atomically
	type entry struct {
		key      string
		metadata []byte
		content  []byte
	}
	var entries []entry

	err := fs.Scan(prefix, func(key string, metadata, content []byte) error {
		if !strings.HasPrefix(key, prefix) || !strings.Contains(key, ":file:") {
			return nil
		}

		var parsedMetadata map[string]interface{}
		if err := json.Unmarshal(metadata, &parsedMetadata); err == nil {
			if deleted, ok := parsedMetadata["deleted"].(bool); !ok || !deleted {
				entries = append(entries, entry{
					key:      key,
					metadata: metadata,
					content:  content,
				})
			}
		}
		return nil
	})

	fs.mutex.RLock()
	defer fs.mutex.RUnlock()
	if err != nil {
		return "", err
	}

	// Sort entries by key for deterministic ordering
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	// Hash in sorted order
	hash := sha256.New()
	for _, e := range entries {
		hash.Write([]byte(e.key))
		hash.Write(e.metadata)
		hash.Write(e.content)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
