package partitionmanager

import (
	"fmt"
	"sync"

	ensemblekv "github.com/donomii/ensemblekv"
)

// FileStore provides atomic access to file metadata and content across two KV stores
type FileStore struct {
	metadataKV ensemblekv.KvLike
	contentKV  ensemblekv.KvLike
	mutex      sync.RWMutex
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
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	return fs.metadataKV.Get([]byte(key))
}

// GetContent retrieves only content
func (fs *FileStore) GetContent(key string) ([]byte, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

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
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	return fs.metadataKV.Put([]byte(key), metadata)
}

// Delete removes both metadata and content atomically
func (fs *FileStore) Delete(key string) error {
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
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

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
