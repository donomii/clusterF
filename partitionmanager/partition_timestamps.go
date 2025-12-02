package partitionmanager

import (
	"encoding/binary"
	"errors"
	"fmt"
	stdFs "io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/types"
	"golang.org/x/sys/unix"
)

const (
	lastReindexTimestampFile = "last_reindex_start.txt"
	lastSyncTimestampFile    = "last_sync_start.txt"
	lastUpdateTimestampFile  = "last_update.txt"
)

const (
	timestampEntryCount = 1 << 16
	timestampEntrySize  = 8 // int64 nanoseconds
	timestampFileSize   = timestampEntryCount * timestampEntrySize
)

var timestampMmapNames = map[string]string{
	lastReindexTimestampFile: "last_reindex_start.mmap",
	lastSyncTimestampFile:    "last_sync_start.mmap",
	lastUpdateTimestampFile:  "last_update.mmap",
}

type timestampMmap struct {
	file *os.File
	data []byte
}

type PartitionTimestampStore struct {
	controlDir string
	mmaps      *syncmap.SyncMap[string, *timestampMmap] // keyed by timestamp filename (txt)
	mu         sync.Mutex
}

func NewPartitionTimestampStore(controlDir string) (*PartitionTimestampStore, error) {
	store := &PartitionTimestampStore{
		controlDir: controlDir,
		mmaps:      syncmap.NewSyncMap[string, *timestampMmap](),
	}

	for txtName, mmapName := range timestampMmapNames {
		path := filepath.Join(controlDir, mmapName)
		mmapFile, created, err := openTimestampMmap(path)
		if err != nil {
			return nil, err
		}
		store.mmaps.Store(txtName, mmapFile)
		if created {
			if err := store.backfillTimestampMmap(mmapFile, txtName); err != nil {
				return nil, err
			}
		}
	}

	return store, nil
}

func (s *PartitionTimestampStore) Close() {
	s.mmaps.Range(func(_ string, mmapFile *timestampMmap) bool {
		_ = mmapFile.close()
		return true
	})
}

func (s *PartitionTimestampStore) Write(partitionID types.PartitionID, filename string, ts time.Time) error {
	return s.writeTimestampToMmap(partitionID, filename, ts)
}

func (s *PartitionTimestampStore) Read(partitionID types.PartitionID, filename string) (time.Time, error) {
	return s.readTimestampFromMmap(partitionID, filename)
}

func (s *PartitionTimestampStore) writeTimestampToMmap(partitionID types.PartitionID, filename string, ts time.Time) error {
	mmapFile, ok := s.mmaps.Load(filename)
	if !ok || mmapFile == nil || len(mmapFile.data) == 0 {
		panic(fmt.Sprintf("timestamp mmap not initialized for %s", filename))
	}
	index, err := partitionIndex(partitionID)
	if err != nil {
		return err
	}
	offset := index * timestampEntrySize

	s.mu.Lock()
	defer s.mu.Unlock()
	binary.LittleEndian.PutUint64(mmapFile.data[offset:offset+timestampEntrySize], uint64(ts.UnixNano()))
	return nil
}

func (s *PartitionTimestampStore) readTimestampFromMmap(partitionID types.PartitionID, filename string) (time.Time, error) {
	mmapFile, ok := s.mmaps.Load(filename)
	if !ok || mmapFile == nil || len(mmapFile.data) == 0 {
		panic(fmt.Sprintf("timestamp mmap not initialized for %s", filename))
	}
	index, err := partitionIndex(partitionID)
	if err != nil {
		return time.Time{}, err
	}
	offset := index * timestampEntrySize

	s.mu.Lock()
	defer s.mu.Unlock()
	nanos := int64(binary.LittleEndian.Uint64(mmapFile.data[offset : offset+timestampEntrySize]))

	if nanos == 0 {
		return time.Time{}, nil
	}
	return time.Unix(0, nanos), nil
}

func openTimestampMmap(path string) (*timestampMmap, bool, error) {
	created := false
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, false, err
	}

	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		created = true
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, false, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, false, err
	}

	if info.Size() != timestampFileSize {
		if err := file.Truncate(timestampFileSize); err != nil {
			_ = file.Close()
			return nil, false, err
		}
		created = true
	}

	data, err := unix.Mmap(int(file.Fd()), 0, timestampFileSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		return nil, false, err
	}

	return &timestampMmap{file: file, data: data}, created, nil
}

func (m *timestampMmap) close() error {
	if m == nil {
		return nil
	}
	var firstErr error
	if m.data != nil {
		if err := unix.Msync(m.data, unix.MS_SYNC); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := unix.Munmap(m.data); err != nil && firstErr == nil {
			firstErr = err
		}
		m.data = nil
	}
	if m.file != nil {
		if err := m.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		m.file = nil
	}
	return firstErr
}

func partitionIndex(partitionID types.PartitionID) (int, error) {
	if len(partitionID) < 2 || partitionID[0] != 'p' {
		return 0, fmt.Errorf("invalid partition ID: %s", partitionID)
	}
	num, err := strconv.Atoi(string(partitionID[1:]))
	if err != nil {
		return 0, fmt.Errorf("invalid partition ID %s: %w", partitionID, err)
	}
	if num < 0 || num >= timestampEntryCount {
		return 0, fmt.Errorf("partition ID out of range: %s", partitionID)
	}
	return num, nil
}

func (s *PartitionTimestampStore) backfillTimestampMmap(m *timestampMmap, filename string) error {
	if m == nil {
		return fmt.Errorf("nil timestamp mmap for %s", filename)
	}
	return filepath.WalkDir(s.controlDir, func(path string, d stdFs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Base(path) != filename {
			return nil
		}
		ts, err := readTimestampFromFile(path)
		if err != nil {
			return nil
		}
		partitionID := types.PartitionID(filepath.Base(filepath.Dir(path)))
		if partitionID == "" {
			return nil
		}
		if err := s.writeTimestampToMmap(partitionID, filename, ts); err != nil {
			return nil
		}
		return nil
	})
}

func readTimestampFromFile(path string) (time.Time, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}, err
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(b)))
	if err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}
