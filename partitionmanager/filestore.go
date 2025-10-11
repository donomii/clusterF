package partitionmanager

import (
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
	"github.com/tchap/go-patricia/patricia"
)

type PartitionStore string

// FileStore provides atomic access to file metadata and content with per-partition locking
type FileStore struct {
	baseDir        string
	partitionLocks syncmap.SyncMap[string, *sync.RWMutex] // map[string]*sync.RWMutex - per-partition locks
	debugLog       bool
	encryptionKey  []byte // XOR encryption key (nil = no encryption)
	storageMajor   string // storage format major (ensemble or bolt)
	storageMinor   string // storage format minor (ensemble or bolt)
	// Handle caches
	metadataHandles sync.Map      // map[string]ensemblekv.KvLike - cached metadata handles
	contentHandles  sync.Map      // map[string]ensemblekv.KvLike - cached content handles
	handleMutex     sync.Mutex    // protects handle opening/closing
	trie            patricia.Trie // Indexed by file path *ONLY*, not composite key.  Use makeKey/extractPath to work with composite keys
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
func NewFileStore(baseDir string, debug bool, storageMajor, storageMinor string) *FileStore {
	if storageMajor == "" {
		storageMajor = "extent"
	}
	return &FileStore{
		baseDir:      baseDir,
		debugLog:     debug,
		storageMajor: storageMajor,
		storageMinor: storageMinor,
		trie:         *patricia.NewTrie(),
	}
}

// Close closes all cached handles
func (fs *FileStore) Close() {
	fs.debugf("Closing FileStore and all cached handles")
	fs.handleMutex.Lock()
	defer fs.handleMutex.Unlock()

	fs.debugf("Closing all cached metadata handles")
	fs.metadataHandles.Range(func(key, value interface{}) bool {
		if kv, ok := value.(ensemblekv.KvLike); ok {
			kv.Close()
		}
		fs.metadataHandles.Delete(key)
		return true
	})

	fs.debugf("Closing all cached content handles")
	fs.contentHandles.Range(func(key, value interface{}) bool {
		if kv, ok := value.(ensemblekv.KvLike); ok {
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
func (fs *FileStore) getPartitionLock(partitionID PartitionStore) *sync.RWMutex {
	lock, _ := fs.partitionLocks.LoadOrStore(string(partitionID), &sync.RWMutex{})
	return lock
}

// openPartitionStores opens both metadata and content stores for a partition
func (fs *FileStore) openPartitionStores(partitionStoreID PartitionStore) (ensemblekv.KvLike, ensemblekv.KvLike, error) {
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

	//Build the trie
	metadataKV.MapFunc(func(k, v []byte) error {
		path := []byte(extractFilePath(string(k)))
		partitionID := extractPartitionStoreID(string(k))
		fmt.Printf("[FILESTORE] Storing %v, %v in trie\n", string(path), partitionID)
		fs.trie.Insert(path, partitionID)

		//FIXME errors?

		return nil
	})

	return metadataKV, contentKV, nil
}

func (fs *FileStore) Exists(key string) bool {
	return fs.trie.Match(patricia.Prefix(key))
}

// closePartitionStores does nothing now - handles are cached
func (fs *FileStore) closePartitionStores(metadataKV, contentKV ensemblekv.KvLike) {
	// Handles are now cached and not closed after each operation
}

// extractPartitionStoreID extracts the partition ID from a key
func extractPartitionStoreID(key string) PartitionStore {
	// Key format: partition:p12345:file:/path/to/file
	parts := strings.Split(key, ":")
	if len(parts) >= 2 && parts[0] == "partition" {
		if len(parts[1]) >= 3 {
			partId := parts[1][0:3] // Extract partition ID (first 3 characters)
			return PartitionStore(partId)
		}
	}
	return ""
}

func extractFilePath(key string) string {
	// Key format: partition:p12345:file:/path/to/file
	parts := strings.Split(key, ":file:")
	if len(parts) > 1 {
		return parts[1]
	}

	panic("no")
}

// Get retrieves both metadata and content atomically
func (fs *FileStore) Get(key string) (*FileData, error) {
	if !fs.Exists(extractFilePath(key)) {
		return &FileData{Key: key, Exists: false}, nil
	}
	partitionID := extractPartitionStoreID(key)
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

	// Decrypt data
	metadata, content = fs.decrypt(metadata, content)

	return &FileData{
		Key:      key,
		Metadata: metadata,
		Content:  content,
		Exists:   true,
	}, nil
}

// GetMetadata retrieves only metadata
func (fs *FileStore) GetMetadata(key string) ([]byte, error) {
	fs.debugf("Starting FileStore.GetMetadata for key %v", key)
	defer fs.debugf("Leaving FileStore.GetMetadata for key %v", key)

	if !fs.Exists(extractFilePath(key)) {
		return nil, fmt.Errorf("Not found")
	}
	partitionID := extractPartitionStoreID(key)
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

	metadata, err := metadataKV.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	// Decrypt metadata
	if len(fs.encryptionKey) > 0 {
		metadata = fs.xorEncrypt(metadata)
	}

	return metadata, nil
}

// GetContent retrieves only content
func (fs *FileStore) GetContent(key string) ([]byte, error) {
	if !fs.Exists(extractFilePath(key)) {
		return nil, fmt.Errorf("Not found")
	}
	partitionID := extractPartitionStoreID(key)
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

	content, err := contentKV.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	// Decrypt content
	if len(fs.encryptionKey) > 0 {
		content = fs.xorEncrypt(content)
	}

	return content, nil
}

// Put stores both metadata and content atomically
func (fs *FileStore) Put(key string, metadata, content []byte) error {

	partitionID := extractPartitionStoreID(key)
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

	// Encrypt data before storing
	encMetadata, encContent := fs.encrypt(metadata, content)

	if err := metadataKV.Put(keyBytes, encMetadata); err != nil {
		return fmt.Errorf("failed to store metadata: %v", err)
	}

	if err := contentKV.Put(keyBytes, encContent); err != nil {
		// Try to rollback metadata
		metadataKV.Delete(keyBytes)
		return fmt.Errorf("failed to store content: %v", err)
	}
	fs.trie.Insert([]byte(extractFilePath(key)), partitionID)
	return nil
}

// PutMetadata stores only metadata
func (fs *FileStore) PutMetadata(key string, metadata []byte) error {
	partitionID := extractPartitionStoreID(key)
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

	// Encrypt metadata
	if len(fs.encryptionKey) > 0 {
		metadata = fs.xorEncrypt(metadata)
	}

	err = metadataKV.Put([]byte(key), metadata)
	if err != nil {
		return err
	}
	fs.trie.Insert([]byte(extractFilePath(key)), partitionID)
	return err
}

// Delete removes both metadata and content atomically
func (fs *FileStore) Delete(key string) error {
	checkForRecursiveScan()

	partitionID := extractPartitionStoreID(key)
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
	fs.trie.Delete([]byte(extractFilePath(key)))
	return nil
}

// Scan calls fn for each file with the given prefix
func (fs *FileStore) Scan(prefix string, fn func(key string, metadata, content []byte) error) error {
	checkForRecursiveScan()

	// Determine which partitions to scan based on prefix
	partitions, err := fs.getAllPartitionStores(prefix)
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

			// Decrypt data before passing to callback
			decMetadata, decContent := fs.decrypt(metadataValues[i], content)

			if err := fn(key, decMetadata, decContent); err != nil {
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
	start := time.Now()
	checkForRecursiveScan()

	wg := &sync.WaitGroup{}
	countKeys := 0
	res := fs.trie.VisitSubtree([]byte(prefix), func(path_b patricia.Prefix, partitionID_b patricia.Item) error {
		wg.Add(1)
		go func(path_b patricia.Prefix, partitionID_b patricia.Item) {
			defer wg.Done()
			partitionStore := partitionID_b.(PartitionStore)
			fmt.Printf("[FILESTORE] Opening partitionStore %v", partitionStore)
			metadataKV, contentKV, err := fs.openPartitionStores(partitionStore)
			defer fs.closePartitionStores(metadataKV, contentKV)
			if err != nil {
				fmt.Printf("Warn: skipping partition %v in search\n", partitionStore)
				return // Skip this partition if it can't be opened
			}
			fs.debugf("Opened partition %v after %v", partitionStore, time.Since(start))

			fmt.Printf("[FILESTORE] Examining key %v in partition %v after %v\n", string(path_b), partitionStore, time.Since(start))
			countKeys = countKeys + 1
			kvkey := makeKey(string(path_b))
			fmt.Printf("[FILESTORE] KV key %v\n", kvkey)

			v, err := metadataKV.Get([]byte(kvkey))
			if err != nil {
				fmt.Printf("[FILESTORE] Failed to retrieve metadata for key %v\n", makeKey(string(path_b)))
				return
			}
			// Decrypt metadata before passing to callback
			decMetadata := v
			if len(fs.encryptionKey) > 0 {
				decMetadata = fs.xorEncrypt(v)
			}
			fn(string(path_b), decMetadata)
		}(path_b, partitionID_b)
		return nil
	})
	wg.Wait()
	fmt.Printf("Finished ScanMetadata for for prefix %v for %v keys in %v seconds\n", prefix, countKeys, time.Since(start))
	return res
}

func makeKey(path string) string {
	partitionID := HashToPartition(path)
	key := fmt.Sprintf("partition:%v:file:%v", partitionID, path)
	return key
}

func (fs *FileStore) ScanPartitionMetaData(partitionId PartitionStore, fn func(key []byte, metadata []byte) error) error {
	start := time.Now()
	partitionStore := PartitionStore(partitionId[0:3])
	fmt.Printf("[FILESTORE] Opening partitionStore %v", partitionStore)
	metadataKV, contentKV, err := fs.openPartitionStores(partitionStore)
	defer fs.closePartitionStores(metadataKV, contentKV)
	if err != nil {
		fmt.Printf("Warn: skipping partition %v in search\n", partitionStore)
		return err // Skip this partition if it can't be opened
	}
	fs.debugf("Opened partition %v after %v", partitionStore, time.Since(start))

	_, err = metadataKV.MapFunc(fn)
	return err

}

// getAllPartitionStores determines which partition directories to scan
func (fs *FileStore) getAllPartitionStores(prefix string) ([]PartitionStore, error) {
	// If prefix specifies a specific partition, only scan that one
	partitionID := extractPartitionStoreID(prefix)
	if partitionID != "" {
		return []PartitionStore{partitionID}, nil
	}

	// Otherwise scan all partition directories
	entries, err := os.ReadDir(fs.baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []PartitionStore{}, nil
		}
		return nil, err
	}

	var partitions []PartitionStore
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "p") {
			partitions = append(partitions, PartitionStore(entry.Name()))
		}
	}

	return partitions, nil
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

		var parsedMetadata types.FileMetadata
		if err := json.Unmarshal(metadata, &parsedMetadata); err == nil {
			if !parsedMetadata.Deleted {
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
