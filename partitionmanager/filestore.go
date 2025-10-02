package partitionmanager

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	ensemblekv "github.com/donomii/ensemblekv"
)

// FileStore provides atomic access to file metadata and content with per-partition locking
type FileStore struct {
	baseDir        string
	partitionLocks sync.Map // map[string]*sync.RWMutex - per-partition locks
	debugLog       bool
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

// NewFileStore creates a new FileStore with per-partition storage
func NewFileStore(baseDir string) *FileStore {
	return &FileStore{
		baseDir:  baseDir,
		debugLog: true, // Enable debug logging
	}
}

func (fs *FileStore) debugf(format string, args ...interface{}) {
	if !fs.debugLog {
		return
	}
	_, file, line, _ := runtime.Caller(1)
	log.Printf("[FILESTORE %s:%d] %s", filepath.Base(file), line, fmt.Sprintf(format, args...))
}

// getPartitionLock gets or creates a lock for a specific partition
func (fs *FileStore) getPartitionLock(partitionID string) *sync.RWMutex {
	lock, _ := fs.partitionLocks.LoadOrStore(partitionID, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

// openPartitionStores opens both metadata and content stores for a partition
func (fs *FileStore) openPartitionStores(partitionID string) (ensemblekv.KvLike, ensemblekv.KvLike, error) {
	partitionDir := filepath.Join(fs.baseDir, partitionID)
	if err := os.MkdirAll(partitionDir, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create partition directory: %v", err)
	}

	metadataPath := filepath.Join(partitionDir, "metadata")
	contentPath := filepath.Join(partitionDir, "content")

	// Ensure the paths exist before opening stores
	if err := os.MkdirAll(metadataPath, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create metadata directory: %v", err)
	}
	if err := os.MkdirAll(contentPath, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create content directory: %v", err)
	}

	//FIXME make storage type configurable
	metadataKV := ensemblekv.SimpleEnsembleCreator("extent", "", metadataPath, 20*1024*1024, 50, 256*1024*1024)
	contentKV := ensemblekv.SimpleEnsembleCreator("extent", "", contentPath, 20*1024*1024, 50, 64*1024*1024)

	if metadataKV == nil {
		return nil, nil, fmt.Errorf("failed to create metadata store")
	}
	if contentKV == nil {
		metadataKV.Close()
		return nil, nil, fmt.Errorf("failed to create content store")
	}

	return metadataKV, contentKV, nil
}

// closePartitionStores closes both stores
func (fs *FileStore) closePartitionStores(metadataKV, contentKV ensemblekv.KvLike) {
	if metadataKV != nil {
		metadataKV.Close()
	}
	if contentKV != nil {
		contentKV.Close()
	}
}

// extractPartitionID extracts the partition ID from a key
func extractPartitionID(key string) string {
	// Key format: partition:p12345:file:/path/to/file
	parts := strings.Split(key, ":")
	if len(parts) >= 2 && parts[0] == "partition" {
		return parts[1]
	}
	return ""
}

// Get retrieves both metadata and content atomically
func (fs *FileStore) Get(key string) (*FileData, error) {
	partitionID := extractPartitionID(key)
	if partitionID == "" {
		return &FileData{Key: key, Exists: false}, nil
	}

	fs.debugf("Get: acquiring read lock for partition %s, key %s", partitionID, key)
	start := time.Now()
	lock := fs.getPartitionLock(partitionID)
	lock.RLock()
	fs.debugf("Get: acquired read lock for partition %s after %v", partitionID, time.Since(start))
	defer func() {
		lock.RUnlock()
		fs.debugf("Get: released read lock for partition %s", partitionID)
	}()

	metadataKV, contentKV, err := fs.openPartitionStores(partitionID)
	if err != nil {
		return &FileData{Key: key, Exists: false}, nil
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	keyBytes := []byte(key)

	metadata, metaErr := metadataKV.Get(keyBytes)
	content, contentErr := contentKV.Get(keyBytes)

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
	partitionID := extractPartitionID(key)
	if partitionID == "" {
		return nil, fmt.Errorf("invalid key format")
	}

	fs.debugf("GetMetadata: acquiring read lock for partition %s, key %s", partitionID, key)
	start := time.Now()
	lock := fs.getPartitionLock(partitionID)
	lock.RLock()
	fs.debugf("GetMetadata: acquired read lock for partition %s after %v", partitionID, time.Since(start))
	defer func() {
		lock.RUnlock()
		fs.debugf("GetMetadata: released read lock for partition %s", partitionID)
	}()

	metadataKV, contentKV, err := fs.openPartitionStores(partitionID)
	if err != nil {
		return nil, err
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	return metadataKV.Get([]byte(key))
}

// GetContent retrieves only content
func (fs *FileStore) GetContent(key string) ([]byte, error) {
	partitionID := extractPartitionID(key)
	if partitionID == "" {
		return nil, fmt.Errorf("invalid key format")
	}

	lock := fs.getPartitionLock(partitionID)
	lock.RLock()
	defer lock.RUnlock()

	metadataKV, contentKV, err := fs.openPartitionStores(partitionID)
	if err != nil {
		return nil, err
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	return contentKV.Get([]byte(key))
}

// Put stores both metadata and content atomically
func (fs *FileStore) Put(key string, metadata, content []byte) error {
	partitionID := extractPartitionID(key)
	if partitionID == "" {
		return fmt.Errorf("invalid key format")
	}

	fs.debugf("Put: acquiring write lock for partition %s, key %s", partitionID, key)
	start := time.Now()
	lock := fs.getPartitionLock(partitionID)
	lock.Lock()
	fs.debugf("Put: acquired write lock for partition %s after %v", partitionID, time.Since(start))
	defer func() {
		lock.Unlock()
		fs.debugf("Put: released write lock for partition %s", partitionID)
	}()

	metadataKV, contentKV, err := fs.openPartitionStores(partitionID)
	if err != nil {
		return err
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	keyBytes := []byte(key)

	if err := metadataKV.Put(keyBytes, metadata); err != nil {
		return fmt.Errorf("failed to store metadata: %v", err)
	}

	if err := contentKV.Put(keyBytes, content); err != nil {
		// Try to rollback metadata
		metadataKV.Delete(keyBytes)
		return fmt.Errorf("failed to store content: %v", err)
	}

	return nil
}

// PutMetadata stores only metadata
func (fs *FileStore) PutMetadata(key string, metadata []byte) error {
	partitionID := extractPartitionID(key)
	if partitionID == "" {
		return fmt.Errorf("invalid key format")
	}

	fs.debugf("PutMetadata: acquiring write lock for partition %s, key %s", partitionID, key)
	start := time.Now()
	lock := fs.getPartitionLock(partitionID)
	lock.Lock()
	fs.debugf("PutMetadata: acquired write lock for partition %s after %v", partitionID, time.Since(start))
	defer func() {
		lock.Unlock()
		fs.debugf("PutMetadata: released write lock for partition %s", partitionID)
	}()

	metadataKV, contentKV, err := fs.openPartitionStores(partitionID)
	if err != nil {
		return err
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	return metadataKV.Put([]byte(key), metadata)
}

// Delete removes both metadata and content atomically
func (fs *FileStore) Delete(key string) error {
	checkForRecursiveScan()

	partitionID := extractPartitionID(key)
	if partitionID == "" {
		return fmt.Errorf("invalid key format")
	}

	fs.debugf("Delete: acquiring write lock for partition %s, key %s", partitionID, key)
	start := time.Now()
	lock := fs.getPartitionLock(partitionID)
	lock.Lock()
	fs.debugf("Delete: acquired write lock for partition %s after %v", partitionID, time.Since(start))
	defer func() {
		lock.Unlock()
		fs.debugf("Delete: released write lock for partition %s", partitionID)
	}()

	metadataKV, contentKV, err := fs.openPartitionStores(partitionID)
	if err != nil {
		return err
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	keyBytes := []byte(key)

	// Delete from both stores - ignore individual errors
	metadataKV.Delete(keyBytes)
	contentKV.Delete(keyBytes)

	return nil
}

// Scan calls fn for each file with the given prefix
func (fs *FileStore) Scan(prefix string, fn func(key string, metadata, content []byte) error) error {
	checkForRecursiveScan()

	// Determine which partitions to scan based on prefix
	partitions, err := fs.getPartitionsForPrefix(prefix)
	if err != nil {
		return err
	}

	fs.debugf("Scan: scanning %d partitions for prefix %s", len(partitions), prefix)

	for _, partitionID := range partitions {
		fs.debugf("Scan: acquiring read lock for partition %s", partitionID)
		start := time.Now()
		lock := fs.getPartitionLock(partitionID)
		lock.RLock()
		fs.debugf("Scan: acquired read lock for partition %s after %v", partitionID, time.Since(start))

		metadataKV, contentKV, err := fs.openPartitionStores(partitionID)
		if err != nil {
			lock.RUnlock()
			continue // Skip this partition if it can't be opened
		}

		// Collect keys and metadata from this partition
		var keys []string
		var metadataValues [][]byte

		_, mapErr := metadataKV.MapFunc(func(k, v []byte) error {
			keyStr := string(k)
			if prefix == "" || strings.HasPrefix(keyStr, prefix) {
				keys = append(keys, keyStr)
				metaCopy := make([]byte, len(v))
				copy(metaCopy, v)
				metadataValues = append(metadataValues, metaCopy)
			}
			return nil
		})

		if mapErr != nil {
			fs.closePartitionStores(metadataKV, contentKV)
			lock.RUnlock()
			return mapErr
		}

		// Get content for each key and call fn
		for i, key := range keys {
			content, _ := contentKV.Get([]byte(key))

			if err := fn(key, metadataValues[i], content); err != nil {
				fs.closePartitionStores(metadataKV, contentKV)
				lock.RUnlock()
				return err
			}
		}

		fs.closePartitionStores(metadataKV, contentKV)
		lock.RUnlock()
		fs.debugf("Scan: released read lock for partition %s", partitionID)
	}

	return nil
}

// ScanMetadata calls fn for each metadata entry with the given prefix
func (fs *FileStore) ScanMetadata(prefix string, fn func(key string, metadata []byte) error) error {
	checkForRecursiveScan()

	// Determine which partitions to scan based on prefix
	partitions, err := fs.getPartitionsForPrefix(prefix)
	if err != nil {
		return err
	}

	fs.debugf("ScanMetadata: scanning %d partitions for prefix %s", len(partitions), prefix)

	for _, partitionID := range partitions {
		fs.debugf("ScanMetadata: acquiring read lock for partition %s", partitionID)
		start := time.Now()
		lock := fs.getPartitionLock(partitionID)
		lock.RLock()
		fs.debugf("ScanMetadata: acquired read lock for partition %s after %v", partitionID, time.Since(start))

		metadataKV, contentKV, err := fs.openPartitionStores(partitionID)
		if err != nil {
			lock.RUnlock()
			continue // Skip this partition if it can't be opened
		}

		_, mapErr := metadataKV.MapFunc(func(k, v []byte) error {
			keyStr := string(k)
			if prefix == "" || strings.HasPrefix(keyStr, prefix) {
				return fn(keyStr, v)
			}
			return nil
		})

		fs.closePartitionStores(metadataKV, contentKV)
		lock.RUnlock()

		if mapErr != nil {
			return mapErr
		}
	}

	return nil
}

// getPartitionsForPrefix determines which partition directories to scan
func (fs *FileStore) getPartitionsForPrefix(prefix string) ([]string, error) {
	// If prefix specifies a specific partition, only scan that one
	partitionID := extractPartitionID(prefix)
	if partitionID != "" {
		return []string{partitionID}, nil
	}

	// Otherwise scan all partition directories
	entries, err := os.ReadDir(fs.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var partitions []string
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "p") {
			partitions = append(partitions, entry.Name())
		}
	}

	return partitions, nil
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
