package partitionmanager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/types"
	ensemblekv "github.com/donomii/ensemblekv"
)

// FileStore provides atomic access to file metadata and content with per-partition locking
type FileStore struct {
	baseDir        string                                 //Holds the partitions directories
	partitionLocks syncmap.SyncMap[string, *sync.RWMutex] // map[string]*sync.RWMutex - per-partition locks
	debugLog       bool
	encryptionKey  []byte // XOR encryption key (nil = no encryption)
	storageMajor   string // storage format major (ensemble or bolt)
	storageMinor   string // storage format minor (ensemble or bolt)
	// Handle caches
	metadataHandles sync.Map   // map[string]ensemblekv.KvLike - cached metadata handles
	contentHandles  sync.Map   // map[string]ensemblekv.KvLike - cached content handles
	handleMutex     sync.Mutex // protects handle opening/closing
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

// NewFileStore creates a new FileStore with per-partition storage
func NewFileStore(baseDir string, debug bool, storageMajor, storageMinor string) *FileStore {
	if storageMajor == "" {
		storageMajor = "mmapsingle"
	}
	return &FileStore{
		baseDir:      baseDir,
		debugLog:     debug,
		storageMajor: storageMajor,
		storageMinor: storageMinor,
	}
}

// Close closes all cached handles
func (fs *FileStore) Close() {
	fs.debugf("Closing FileStore and all cached handles")
	fs.handleMutex.Lock()
	defer fs.handleMutex.Unlock()

	fs.debugf("Flushing and closing all cached metadata handles")
	fs.metadataHandles.Range(func(key, value interface{}) bool {
		if kv, ok := value.(ensemblekv.KvLike); ok {
			kv.Flush()
			kv.Close()
		}
		fs.metadataHandles.Delete(key)
		return true
	})

	fs.debugf("Flushing and closing all cached content handles")
	fs.contentHandles.Range(func(key, value interface{}) bool {
		if kv, ok := value.(ensemblekv.KvLike); ok {
			kv.Flush()
			kv.Close()
		}
		fs.contentHandles.Delete(key)
		return true
	})
}

// SetEncryptionKey sets the encryption key for this FileStore
func (fs *FileStore) SetEncryptionKey(key []byte) {
	fs.encryptionKey = key
}

// xorEncrypt performs XOR encryption/decryption on data
func (fs *FileStore) xorEncrypt(data []byte) []byte {
	if len(fs.encryptionKey) == 0 || len(data) == 0 {
		return data
	}
	result := make([]byte, len(data))
	for i := range data {
		result[i] = data[i] ^ fs.encryptionKey[i%len(fs.encryptionKey)]
	}
	return result
}

// encrypt encrypts both metadata and content
func (fs *FileStore) encrypt(metadata, content []byte) ([]byte, []byte) {
	if len(fs.encryptionKey) == 0 {
		return metadata, content
	}
	return fs.xorEncrypt(metadata), fs.xorEncrypt(content)
}

// decrypt decrypts both metadata and content
func (fs *FileStore) decrypt(metadata, content []byte) ([]byte, []byte) {
	if len(fs.encryptionKey) == 0 {
		return metadata, content
	}
	return fs.xorEncrypt(metadata), fs.xorEncrypt(content)
}

func (fs *FileStore) debugf(format string, args ...interface{}) {
	if !fs.debugLog {
		return
	}
	_, file, line, _ := runtime.Caller(1)
	fmt.Printf("[FILESTORE %s:%d] %s\n", filepath.Base(file), line, fmt.Sprintf(format, args...))
}

// getPartitionLock gets or creates a lock for a specific partition
func (fs *FileStore) getPartitionLock(partitionID types.PartitionStore) *sync.RWMutex {
	lock, _ := fs.partitionLocks.LoadOrStore(string(partitionID), &sync.RWMutex{})
	return lock
}

// openPartitionStores opens both metadata and content stores for a partition
func (fs *FileStore) openPartitionStores(partitionStoreID types.PartitionStore) (ensemblekv.KvLike, ensemblekv.KvLike, error) {
	fs.handleMutex.Lock()
	defer fs.handleMutex.Unlock()

	// Check cache first
	if metaHandle, ok := fs.metadataHandles.Load(partitionStoreID); ok {
		if contentHandle, ok := fs.contentHandles.Load(partitionStoreID); ok {
			return metaHandle.(ensemblekv.KvLike), contentHandle.(ensemblekv.KvLike), nil
		}
	}

	partitionDir := filepath.Join(fs.baseDir, string(partitionStoreID))
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

	metadataKV := ensemblekv.SimpleEnsembleCreator(fs.storageMajor, fs.storageMinor, metadataPath, 20*1024*1024, 50, 256*1024*1024)
	contentKV := ensemblekv.SimpleEnsembleCreator(fs.storageMajor, fs.storageMinor, contentPath, 20*1024*1024, 50, 64*1024*1024)

	if metadataKV == nil {
		return nil, nil, fmt.Errorf("failed to create metadata store")
	}
	if contentKV == nil {
		metadataKV.Close()
		return nil, nil, fmt.Errorf("failed to create content store")
	}

	// Cache the handles
	fs.metadataHandles.Store(partitionStoreID, metadataKV)
	fs.contentHandles.Store(partitionStoreID, contentKV)

	return metadataKV, contentKV, nil
}

// closePartitionStores does nothing now - handles are cached
func (fs *FileStore) closePartitionStores(metadataKV, contentKV ensemblekv.KvLike) {
	// Handles are now cached and not closed after each operation
}

func encodeStoreKey(partitionID types.PartitionID, path string) []byte {
	storeID := types.ExtractPartitionStoreID(partitionID)
	key := fmt.Sprintf("partition:%s:file:%s", storeID, path)
	return []byte(key)
}

func decodeStoreKey(key []byte) (types.PartitionID, string, error) {
	keyStr := string(key)

	// Check for new format: partition:pxxxxx:file:path
	if strings.HasPrefix(keyStr, "partition:") {
		parts := strings.SplitN(keyStr, ":", 4)
		if len(parts) == 4 && parts[0] == "partition" && parts[2] == "file" {
			path := parts[3]
			return types.PartitionIDForPath(path), path, nil
		}
		return "", "", fmt.Errorf("invalid partition key format: %s", keyStr)
	}

	// Fallback to old format for compatibility
	return types.PartitionIDForPath(keyStr), keyStr, nil
}

// Get retrieves both metadata and content atomically
func (fs *FileStore) Get(path string) (*types.FileData, error) {
	partition := types.PartitionIDForPath(path)
	storeID := types.ExtractPartitionStoreID(partition)

	start := time.Now()
	fs.rLockPartition(storeID)
	//fs.debugf("Get: acquired read lock for partition store %s after %v", storeID, time.Since(start))
	defer func() {
		fs.runLockPartition(storeID)
		fs.debugf("Get: released read lock for partition store %s after %v", storeID, time.Since(start))
	}()

	metadataKV, contentKV, err := fs.openPartitionStores(storeID)
	if err != nil {
		return &types.FileData{Partition: partition, Path: path, Exists: false}, err
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	keyBytes := encodeStoreKey(partition, path)

	metadata, metaErr := metadataKV.Get(keyBytes)
	content, contentErr := contentKV.Get(keyBytes)

	// If neither exists, file doesn't exist
	if metaErr != nil && contentErr != nil {
		return &types.FileData{
			Partition: partition,
			Path:      path,
			Exists:    false,
		}, fmt.Errorf("file not found in store: %v, %v", metaErr, contentErr)
	}

	// Decrypt data
	metadata, content = fs.decrypt(metadata, content)

	return &types.FileData{
		Partition: partition,
		Path:      path,
		Metadata:  metadata,
		Content:   content,
		Exists:    true,
	}, nil
}

func (fs *FileStore) rLockPartition(partitionStoreID types.PartitionStore) {
	// := fs.getPartitionLock(partitionStoreID)
	//lock.RLock()
}

func (fs *FileStore) runLockPartition(partitionStoreID types.PartitionStore) {
	//lock := fs.getPartitionLock(partitionStoreID)
	//lock.RUnlock()
}

func (fs *FileStore) lockPartition(partitionStoreID types.PartitionStore) {
	//lock := fs.getPartitionLock(partitionStoreID)
	//lock.Lock()
}

func (fs *FileStore) unLockPartition(partitionStoreID types.PartitionStore) {
	//lock := fs.getPartitionLock(partitionStoreID)
	//lock.Unlock()
}

// GetMetadata retrieves only metadata
func (fs *FileStore) GetMetadata(path string) ([]byte, error) {
	partition := types.PartitionIDForPath(path)
	storeID := types.ExtractPartitionStoreID(partition)

	start := time.Now()
	fs.rLockPartition(storeID)
	//fs.debugf("GetMetadata: acquired read lock for partition store %s after %v", storeID, time.Since(start))
	defer func() {
		fs.runLockPartition(storeID)
		fs.debugf("GetMetadata: released read lock for partition store %s after %v", storeID, time.Since(start))
	}()

	metadataKV, contentKV, err := fs.openPartitionStores(storeID)
	if err != nil {
		return nil, err
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	// Exists can be significantly faster depending on the store, so check that first
	keyBytes := encodeStoreKey(partition, path)
	exists := metadataKV.Exists(keyBytes)
	if !exists {
		return nil, fmt.Errorf("not found: %s", path)
	}

	metadata, err := metadataKV.Get(keyBytes)
	if err != nil {
		return nil, err
	}

	// Decrypt metadata
	if len(fs.encryptionKey) > 0 {
		metadata, _ = fs.decrypt(metadata, nil)
	}

	return metadata, nil
}

// GetContent retrieves only content
func (fs *FileStore) GetContent(path string) ([]byte, error) {
	partition := types.PartitionIDForPath(path)
	storeID := types.ExtractPartitionStoreID(partition)

	fs.rLockPartition(storeID)
	defer fs.runLockPartition(storeID)

	metadataKV, contentKV, err := fs.openPartitionStores(storeID)
	if err != nil {
		return nil, err
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	content, err := contentKV.Get(encodeStoreKey(partition, path))
	if err != nil {
		return nil, err
	}

	// Decrypt content
	if len(fs.encryptionKey) > 0 {
		_, content = fs.decrypt(nil, content)
	}

	return content, nil
}

// Put stores both metadata and content atomically
func (fs *FileStore) Put(path string, metadata, content []byte) error {
	partition := types.PartitionIDForPath(path)
	storeID := types.ExtractPartitionStoreID(partition)

	fs.debugf("Put: acquiring write lock for partition store %s, path %s", storeID, path)
	//start := time.Now()
	fs.lockPartition(storeID)

	//fs.debugf("Put: acquired write lock for partition store %s after %v", storeID, time.Since(start))
	defer func() {
		fs.unLockPartition(storeID)
		fs.debugf("Put: released write lock for partition store %s", storeID)
	}()

	metadataKV, contentKV, err := fs.openPartitionStores(storeID)
	if err != nil {
		return err
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	keyBytes := encodeStoreKey(partition, path)

	// Encrypt data before storing
	encMetadata, encContent := fs.encrypt(metadata, content)

	if err := metadataKV.Put(keyBytes, encMetadata); err != nil {
		return fmt.Errorf("failed to store metadata: %v", err)
	}
	fs.debugf("Wrote file metadata %s to partition %s", path, partition)

	if err := contentKV.Put(keyBytes, encContent); err != nil {
		// Try to rollback metadata
		metadataKV.Delete(keyBytes)
		return fmt.Errorf("failed to store content: %v", err)
	}
	fs.debugf("Wrote file %s to partition %s", path, partition)

	return nil
}

// PutMetadata stores only metadata
func (fs *FileStore) PutMetadata(path string, metadata []byte) error {
	partition := types.PartitionIDForPath(path)
	storeID := types.ExtractPartitionStoreID(partition)

	fs.debugf("PutMetadata: acquiring write lock for partition store %s, path %s", storeID, path)
	start := time.Now()
	fs.lockPartition(storeID)

	fs.debugf("PutMetadata: acquired write lock for partition store %s after %v", storeID, time.Since(start))
	defer func() {
		fs.unLockPartition(storeID)
		fs.debugf("PutMetadata: released write lock for partition store %s", storeID)
	}()

	metadataKV, contentKV, err := fs.openPartitionStores(storeID)
	if err != nil {
		return err
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	keyBytes := encodeStoreKey(partition, path)

	// Encrypt metadata
	if len(fs.encryptionKey) > 0 {
		metadata, _ = fs.encrypt(metadata, nil)
	}

	if err := metadataKV.Put(keyBytes, metadata); err != nil {
		return err
	}

	return nil
}

// Delete removes both metadata and content atomically
func (fs *FileStore) Delete(path string) error {
	checkForRecursiveScan()

	partition := types.PartitionIDForPath(path)
	storeID := types.ExtractPartitionStoreID(partition)

	fs.debugf("Delete: acquiring write lock for partition store %s, path %s", storeID, path)
	//start := time.Now()
	fs.lockPartition(storeID)

	//fs.debugf("Delete: acquired write lock for partition store %s after %v", storeID, time.Since(start))
	defer func() {
		fs.unLockPartition(storeID)
		fs.debugf("Delete: released write lock for partition store %s", storeID)
	}()

	metadataKV, contentKV, err := fs.openPartitionStores(storeID)
	if err != nil {
		return err
	}
	defer fs.closePartitionStores(metadataKV, contentKV)

	keyBytes := encodeStoreKey(partition, path)

	// Delete from both stores - ignore individual errors
	metadataKV.Delete(keyBytes)
	contentKV.Delete(keyBytes)

	return nil
}

// Scan calls fn for each file that matches the provided filters.
func (fs *FileStore) Scan(pathPrefix string, fn func(path string, metadata, content []byte) error) error {
	checkForRecursiveScan()

	// Determine which partition stores to scan
	partitions, err := fs.GetAllPartitionStores()
	if err != nil {
		return err
	}

	fs.debugf("Scan: scanning %d partitions (prefix=%s)", len(partitions), pathPrefix)

	for _, storeID := range partitions {
		fs.debugf("Scan: acquiring read lock for partition store %s", storeID)
		start := time.Now()
		fs.rLockPartition(storeID)
		//fs.debugf("Scan: acquired read lock for partition store %s after %v", storeID, time.Since(start))

		metadataKV, contentKV, err := fs.openPartitionStores(storeID)
		if err != nil {
			fs.runLockPartition(storeID)
			continue // Skip this partition if it can't be opened
		}

		// Collect entries from this partition
		type entry struct {
			path     string
			keyBytes []byte
			metadata []byte
		}
		var entries []entry

		_, mapErr := metadataKV.MapPrefixFunc([]byte(pathPrefix), func(k, v []byte) error {
			_, filePath, err := decodeStoreKey(k)
			if err != nil {
				return err
			}
			if pathPrefix != "" && !strings.HasPrefix(filePath, pathPrefix) {
				return nil
			}

			metaCopy := make([]byte, len(v))
			copy(metaCopy, v)
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)

			entries = append(entries, entry{
				path:     filePath,
				keyBytes: keyCopy,
				metadata: metaCopy,
			})
			return nil
		})

		if mapErr != nil {
			fs.closePartitionStores(metadataKV, contentKV)
			fs.runLockPartition(storeID)
			return mapErr
		}

		// Get content for each key and call fn
		for _, entry := range entries {
			content, _ := contentKV.Get(entry.keyBytes)
			contentCopy := make([]byte, len(content))
			copy(contentCopy, content)

			decMetadata, decContent := fs.decrypt(entry.metadata, contentCopy)

			if err := fn(entry.path, decMetadata, decContent); err != nil {
				fs.closePartitionStores(metadataKV, contentKV)
				fs.runLockPartition(storeID)
				return err
			}
		}

		fs.closePartitionStores(metadataKV, contentKV)
		fs.runLockPartition(storeID)
		fs.debugf("Scan: released read lock for partition store %s after %v", storeID, time.Since(start))
	}

	return nil
}

// ScanMetadata calls fn for each metadata entry matching the provided filters.
func (fs *FileStore) ScanMetadata(pathPrefix string, fn func(path string, metadata []byte) error) error {
	start := time.Now()
	checkForRecursiveScan()

	// Determine which partition stores to scan
	partitions, err := fs.GetAllPartitionStores()
	if err != nil {
		return err
	}

	fs.debugf("ScanMetadata: scanning %d partitions (prefix=%s)", len(partitions), pathPrefix)

	for _, storeID := range partitions {
		fs.debugf("ScanMetadata: acquiring read lock for partition store %s", storeID)
		fs.rLockPartition(storeID)

		metadataKV, contentKV, err := fs.openPartitionStores(storeID)
		if err != nil {
			fs.runLockPartition(storeID)
			continue // Skip this partition if it can't be opened
		}

		type entry struct {
			path     string
			metadata []byte
		}
		var entries []entry

		_, mapErr := metadataKV.MapPrefixFunc([]byte(pathPrefix), func(k, v []byte) error {
			_, filePath, err := decodeStoreKey(k)
			if err != nil {
				return err
			}
			if pathPrefix != "" && !strings.HasPrefix(filePath, pathPrefix) {
				return nil
			}

			metaCopy := make([]byte, len(v))
			copy(metaCopy, v)

			entries = append(entries, entry{
				path:     filePath,
				metadata: metaCopy,
			})
			return nil
		})

		if mapErr != nil {
			fs.closePartitionStores(metadataKV, contentKV)
			fs.runLockPartition(storeID)
			return mapErr
		}

		// Call fn for each entry
		for _, entry := range entries {
			decMetadata := entry.metadata
			if len(fs.encryptionKey) > 0 {
				decMetadata = fs.xorEncrypt(decMetadata)
			}

			if err := fn(entry.path, decMetadata); err != nil {
				fs.closePartitionStores(metadataKV, contentKV)
				fs.runLockPartition(storeID)
				return err
			}
		}

		fs.closePartitionStores(metadataKV, contentKV)
		fs.runLockPartition(storeID)
		fs.debugf("ScanMetadata: released read lock for partition store %s after %v", storeID, time.Since(start))
	}

	fs.debugf("Finished ScanMetadata for prefix=%v in %v seconds", pathPrefix, time.Since(start).Seconds())
	return nil
}

// ScanMetadataFullKeys calls fn for each metadata entry, providing partition and path.
func (fs *FileStore) ScanMetadataFullKeys(pathPrefix string, fn func(path string, metadata []byte) error) error {
	return fs.ScanMetadata(pathPrefix, fn)
}

func (fs *FileStore) ScanPartitionMetaData(partitionStore types.PartitionStore, fn func(path string, metadata []byte) error) error {
	//start := time.Now()
	//fs.debugf("[FILESTORE] Opening partitionStore %v", partitionStore)
	metadataKV, contentKV, err := fs.openPartitionStores(partitionStore)
	defer fs.closePartitionStores(metadataKV, contentKV)
	if err != nil {
		fs.debugf("Warn: skipping partition %v in search\n", partitionStore)
		return err // Skip this partition if it can't be opened
	}
	//fs.debugf("Opened partition %v after %v", partitionStore, time.Since(start))

	// Use prefix search for the specific partition
	prefix := fmt.Sprintf("partition:%s:file:", partitionStore)
	_, err = metadataKV.MapPrefixFunc([]byte(prefix), func(k, v []byte) error {
		_, path, decodeErr := decodeStoreKey(k)
		if decodeErr != nil {
			return decodeErr
		}

		metaCopy := make([]byte, len(v))
		copy(metaCopy, v)

		if len(fs.encryptionKey) > 0 {
			metaCopy, _ = fs.decrypt(metaCopy, nil)
		}

		return fn(path, metaCopy)
	})
	return err

}

// ScanMetadataPartition scans only files belonging to a specific partition
func (fs *FileStore) ScanMetadataPartition(partitionID types.PartitionID, fn func(path string, metadata []byte) error) error {
	start := time.Now()
	checkForRecursiveScan()

	// Convert partition ID to store ID
	storeID := types.ExtractPartitionStoreID(partitionID)

	fs.debugf("ScanMetadataPartition: acquiring read lock for partition store %s", storeID)
	fs.rLockPartition(storeID)

	metadataKV, contentKV, err := fs.openPartitionStores(storeID)
	if err != nil {
		fs.runLockPartition(storeID)
		return err // Skip this partition if it can't be opened
	}

	type entry struct {
		path     string
		metadata []byte
	}
	var entries []entry

	// Use prefix search for the specific partition
	prefix := fmt.Sprintf("partition:%s:file:", partitionID)
	_, mapErr := metadataKV.MapPrefixFunc([]byte(prefix), func(k, v []byte) error {
		_, filePath, err := decodeStoreKey(k)
		if err != nil {
			return err
		}

		metaCopy := make([]byte, len(v))
		copy(metaCopy, v)

		entries = append(entries, entry{
			path:     filePath,
			metadata: metaCopy,
		})
		return nil
	})

	if mapErr != nil {
		fs.closePartitionStores(metadataKV, contentKV)
		fs.runLockPartition(storeID)
		return mapErr
	}

	// Call fn for each entry
	for _, entry := range entries {
		decMetadata := entry.metadata
		if len(fs.encryptionKey) > 0 {
			decMetadata = fs.xorEncrypt(decMetadata)
		}

		if err := fn(entry.path, decMetadata); err != nil {
			fs.closePartitionStores(metadataKV, contentKV)
			fs.runLockPartition(storeID)
			return err
		}
	}

	fs.closePartitionStores(metadataKV, contentKV)
	fs.runLockPartition(storeID)
	fs.debugf("ScanMetadataPartition: released read lock for partition store %s after %v", storeID, time.Since(start))

	return nil
}

// GetAllPartitionStores determines which partition directories to scan
func (fs *FileStore) GetAllPartitionStores() ([]types.PartitionStore, error) {
	// Scan all partition directories
	entries, err := os.ReadDir(fs.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []types.PartitionStore{}, nil
		}
		return nil, err
	}

	var partitions []types.PartitionStore
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "p") {
			partitions = append(partitions, types.PartitionStore(entry.Name()))
		}
	}

	return partitions, nil
}

// CalculatePartitionChecksum computes a consistent checksum for all files matching the prefix
func (fs *FileStore) CalculatePartitionChecksum(ctx context.Context, pathPrefix string) (string, error) {
	// Collect all non-deleted entries atomically
	type entry struct {
		partition types.PartitionID
		path      string
		metadata  []byte
		content   []byte
	}
	var entries []entry

	err := fs.Scan(pathPrefix, func(path string, metadata, content []byte) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var parsedMetadata types.FileMetadata
		if err := json.Unmarshal(metadata, &parsedMetadata); err == nil {
			if !parsedMetadata.Deleted {
				metaCopy := append([]byte(nil), metadata...)
				contentCopy := append([]byte(nil), content...)
				entries = append(entries, entry{
					partition: types.PartitionIDForPath(path),
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

	// Sort entries deterministically by partition then path
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].partition == entries[j].partition {
			return entries[i].path < entries[j].path
		}
		return entries[i].partition < entries[j].partition
	})

	// Hash in sorted order
	hash := sha256.New()
	for _, e := range entries {
		hash.Write([]byte(e.partition))
		hash.Write([]byte(e.path))
		hash.Write(e.metadata)
		hash.Write(e.content)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
