package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/donomii/clusterF/types"
	bolt "go.etcd.io/bbolt"
)

const legacyFixturePassword = "fixture-passphrase"
const legacyFixturePhrase = "05050d070117176b5d041d10021102151a0a08440c110606"

func legacyFixtureBytes(plain []byte) []byte {
	encoded := append([]byte(nil), plain...)
	for index := range encoded {
		encoded[index] ^= legacyFixturePassword[index%len(legacyFixturePassword)]
	}
	return encoded
}

func encryptionFixtureMetadata(t *testing.T, path string, content []byte) []byte {
	t.Helper()
	modified := time.Date(2024, 3, 2, 1, 0, 0, 123456789, time.UTC)
	metadata, err := json.Marshal(types.FileMetadata{
		Name: filepath.Base(path), Path: path, Size: int64(len(content)), ContentType: "application/octet-stream",
		CreatedAt: modified, ModifiedAt: modified, Checksum: fmt.Sprintf("%x", sha256.Sum256(content)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return metadata
}

func legacyFixtureLocation(directory, backend, kind, path string) (string, []byte) {
	partition := fmt.Sprintf("p%05d", crc32.ChecksumIEEE([]byte(path))%65536)
	if backend == "rawfile" {
		if kind == "content" {
			kind = "contents"
		}
		return filepath.Join(directory, "rawfiles", kind, partition[1:2], partition[2:3], partition[3:4], partition, strings.TrimPrefix(path, "/")), nil
	}
	return filepath.Join(directory, "partitions", partition[:3], kind, "bolt.db"), []byte("partition:" + partition[:3] + ":file:" + path)
}

func seedLegacyFixture(t *testing.T, backend string) (ClusterOpts, string, []byte, []byte) {
	t.Helper()
	opts := ClusterOpts{
		ID: "legacy-node", DataDir: t.TempDir(), StorageMajor: backend, EncryptionKey: legacyFixturePassword,
		Logger: log.New(io.Discard, "", 0),
	}
	directory := filepath.Join(opts.DataDir, opts.ID)
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	settings := fmt.Sprintf(`{"program":"clusterF","url":"https://github.com/donomii/clusterF","storage_major":%q,"storage_minor":"","version":"legacy-fixture","encrypted_test_phrase":%q}`, backend, legacyFixturePhrase)
	if err := os.WriteFile(filepath.Join(directory, "settings.json"), []byte(settings), 0600); err != nil {
		t.Fatal(err)
	}
	path := "/archive/original.bin"
	content := bytes.Repeat([]byte("original legacy data\x00\xff\n"), 4097)
	metadata := encryptionFixtureMetadata(t, path, content)
	for kind, plain := range map[string][]byte{"metadata": metadata, "content": content} {
		location, key := legacyFixtureLocation(directory, backend, kind, path)
		if err := os.MkdirAll(filepath.Dir(location), 0700); err != nil {
			t.Fatal(err)
		}
		encoded := legacyFixtureBytes(plain)
		if backend == "rawfile" {
			if err := os.WriteFile(location, encoded, 0600); err != nil {
				t.Fatal(err)
			}
			continue
		}
		database, err := bolt.Open(location, 0600, nil)
		if err != nil {
			t.Fatal(err)
		}
		err = database.Update(func(transaction *bolt.Tx) error {
			bucket, err := transaction.CreateBucketIfNotExists([]byte("Blocks"))
			if err != nil {
				return err
			}
			return bucket.Put(key, encoded)
		})
		closeErr := database.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("seed %s: write=%v close=%v", location, err, closeErr)
		}
	}
	return opts, path, metadata, content
}

func readLegacyFixtureRecord(t *testing.T, directory, backend, kind, path string) []byte {
	t.Helper()
	location, key := legacyFixtureLocation(directory, backend, kind, path)
	if backend == "rawfile" {
		encoded, err := os.ReadFile(location)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	database, err := bolt.Open(location, 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var encoded []byte
	err = database.View(func(transaction *bolt.Tx) error {
		bucket := transaction.Bucket([]byte("Blocks"))
		if bucket == nil {
			return fmt.Errorf("missing Blocks bucket in %s", location)
		}
		encoded = append([]byte(nil), bucket.Get(key)...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func openEncryptionTestCluster(t *testing.T, opts ClusterOpts) (*Cluster, func()) {
	t.Helper()
	cluster := NewCluster(opts)
	var once sync.Once
	closeCluster := func() {
		once.Do(func() {
			cluster.cancel()
			cluster.partitionManager.FileStore().Close()
		})
	}
	t.Cleanup(closeCluster)
	return cluster, closeCluster
}

func requireEncryptionFile(t *testing.T, cluster *Cluster, path string, content []byte) {
	t.Helper()
	buffered, metadata, err := cluster.FileSystem.GetFile(path)
	if err != nil || !bytes.Equal(buffered, content) || metadata.Path != path {
		t.Fatalf("buffered read %s: path=%s size=%d error=%v", path, metadata.Path, len(buffered), err)
	}
	reader, metadata, err := cluster.FileSystem.GetFileReader(path)
	if err != nil {
		t.Fatalf("stream read %s: %v", path, err)
	}
	streamed, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(streamed, content) || metadata.Path != path {
		t.Fatalf("stream read %s: path=%s size=%d read=%v close=%v", path, metadata.Path, len(streamed), readErr, closeErr)
	}
}

func requireLegacyStoredBytes(t *testing.T, opts ClusterOpts, path string, metadata, content []byte) {
	t.Helper()
	directory := filepath.Join(opts.DataDir, opts.ID)
	for kind, plain := range map[string][]byte{"metadata": metadata, "content": content} {
		encoded := readLegacyFixtureRecord(t, directory, opts.StorageMajor, kind, path)
		if !bytes.Equal(encoded, legacyFixtureBytes(plain)) {
			t.Fatalf("%s %s ciphertext differs from original legacy encoding", kind, path)
		}
	}
}

func TestLegacyRepositoriesRemainReadableAndWritable(t *testing.T) {
	for _, backend := range []string{"rawfile", "bolt"} {
		for _, mode := range []string{"", "auto", "legacy"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				opts, path, metadata, content := seedLegacyFixture(t, backend)
				opts.EncryptionMode = mode
				settingsPath := filepath.Join(opts.DataDir, opts.ID, "settings.json")
				settingsBefore, err := os.ReadFile(settingsPath)
				if err != nil {
					t.Fatal(err)
				}
				cluster, closeCluster := openEncryptionTestCluster(t, opts)
				requireEncryptionFile(t, cluster, path, content)
				closeCluster()
				requireLegacyStoredBytes(t, opts, path, metadata, content)
				settingsAfter, err := os.ReadFile(settingsPath)
				if err != nil || !bytes.Equal(settingsBefore, settingsAfter) {
					t.Fatalf("opening legacy repository changed settings: %v", err)
				}

				cluster, closeCluster = openEncryptionTestCluster(t, opts)
				updated := []byte("updated existing legacy file")
				updatedMetadata := encryptionFixtureMetadata(t, path, updated)
				if err := cluster.partitionManager.StoreFileInPartition(context.Background(), path, updatedMetadata, updated); err != nil {
					t.Fatal(err)
				}
				newPath := "/archive/new-stream.bin"
				newContent := bytes.Repeat([]byte("streamed legacy content\x00\xff"), 9001)
				newMetadata := encryptionFixtureMetadata(t, newPath, newContent)
				if err := cluster.partitionManager.StoreFileInPartitionStream(context.Background(), newPath, newMetadata, bytes.NewReader(newContent), int64(len(newContent))); err != nil {
					t.Fatal(err)
				}
				closeCluster()
				requireLegacyStoredBytes(t, opts, path, updatedMetadata, updated)
				requireLegacyStoredBytes(t, opts, newPath, newMetadata, newContent)
				opts.EncryptionMode = "auto"
				cluster, _ = openEncryptionTestCluster(t, opts)
				requireEncryptionFile(t, cluster, path, updated)
				requireEncryptionFile(t, cluster, newPath, newContent)
			})
		}
	}
}

func encryptionRepositorySnapshot(t *testing.T, directory string) map[string][]byte {
	t.Helper()
	entries := make(map[string][]byte)
	err := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			entries[path+string(filepath.Separator)] = nil
			return nil
		}
		data, err := os.ReadFile(path)
		entries[path] = data
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func requireRejectedEncryptionOpen(t *testing.T, opts ClusterOpts) {
	t.Helper()
	directory := filepath.Join(opts.DataDir, opts.ID)
	before := encryptionRepositorySnapshot(t, directory)
	var rejection string
	func() {
		defer func() {
			if failure := recover(); failure != nil {
				rejection = fmt.Sprint(failure)
			}
		}()
		_, closeCluster := openEncryptionTestCluster(t, opts)
		closeCluster()
	}()
	if !strings.Contains(rejection, "Cannot open repository "+directory+":") {
		t.Fatalf("expected repository encryption rejection, received %q", rejection)
	}
	if after := encryptionRepositorySnapshot(t, directory); !reflect.DeepEqual(before, after) {
		t.Fatal("rejected encryption options changed repository files or directories")
	}
}

func TestLegacyRejectedOptionsPreserveRepository(t *testing.T) {
	for _, backend := range []string{"rawfile", "bolt"} {
		t.Run(backend, func(t *testing.T) {
			opts, _, _, _ := seedLegacyFixture(t, backend)
			for _, password := range []string{"", "incorrect password"} {
				invalid := opts
				invalid.EncryptionKey = password
				requireRejectedEncryptionOpen(t, invalid)
			}
			opts.EncryptionMode = "authenticated"
			requireRejectedEncryptionOpen(t, opts)
		})
	}
}

func TestNewEncryptedRepositoriesReopenWithAuto(t *testing.T) {
	for _, backend := range []string{"rawfile", "bolt"} {
		for _, mode := range []string{"legacy", "", "authenticated"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				opts := ClusterOpts{
					ID: "new-node", DataDir: t.TempDir(), StorageMajor: backend, EncryptionKey: legacyFixturePassword,
					EncryptionMode: mode, Logger: log.New(io.Discard, "", 0),
				}
				cluster, closeCluster := openEncryptionTestCluster(t, opts)
				path := "/new-encrypted.bin"
				content := bytes.Repeat([]byte("new encrypted data\x00\xff"), 7001)
				metadata := encryptionFixtureMetadata(t, path, content)
				if err := cluster.partitionManager.StoreFileInPartitionStream(context.Background(), path, metadata, bytes.NewReader(content), int64(len(content))); err != nil {
					t.Fatal(err)
				}
				closeCluster()
				directory := filepath.Join(opts.DataDir, opts.ID)
				settings, err := loadStorageSettings(directory)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "legacy" {
					if settings.EncryptedTestPhrase != legacyFixturePhrase || settings.Encryption != nil {
						t.Fatalf("new legacy repository has incompatible settings: %+v", settings)
					}
					requireLegacyStoredBytes(t, opts, path, metadata, content)
				} else if settings.Encryption == nil || settings.EncryptedTestPhrase != "" {
					t.Fatalf("new authenticated repository has incompatible settings: %+v", settings)
				}
				opts.EncryptionMode = "auto"
				cluster, closeCluster = openEncryptionTestCluster(t, opts)
				requireEncryptionFile(t, cluster, path, content)
				closeCluster()
				for _, password := range []string{"", "incorrect password"} {
					invalid := opts
					invalid.EncryptionKey = password
					requireRejectedEncryptionOpen(t, invalid)
				}
				if mode == "legacy" {
					opts.EncryptionMode = "authenticated"
				} else {
					opts.EncryptionMode = "legacy"
				}
				requireRejectedEncryptionOpen(t, opts)
			})
		}
	}
}
