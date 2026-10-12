package partitionmanager

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/donomii/clusterF/metrics"
	"github.com/donomii/clusterF/types"
)

func TestLegacyStoreFixturesRemainCompatible(t *testing.T) {
	metrics.NewMetricsCollector(&types.App{NodeID: "legacy-storage-test"})
	path := "/legacy.bin"
	metadata := []byte(`{"path":"/legacy.bin","size":19,"modified_at":"2026-01-02T03:04:05Z","checksum":"5bdf4e8b89a46e6c990b286e483464cc78efa4a68942f658730d2dcee2c155bb"}`)
	content := []byte("existing file bytes")
	encodedMetadata := decodeLegacyFixture(t, "1747170017110f4a435c1f12081307154b05080d5b0152121a09124d48555549450c0c1d4416081617280e064656475551514f0040505e43453b42575655535b534c77524d51101f0a110f1f100a43595b18120515471257105c55045357064f4e4958431145574401585d5455554d4e13564b16110e46055a5d5e55511f1b45594440470b40000f000253004818450311510a")
	encodedContent := decodeLegacyFixture(t, "091d0e121710431741151a1b0a520615110212")
	for _, backend := range []string{"rawfile", "bolt", "extent"} {
		t.Run(backend, func(t *testing.T) {
			var store types.EncryptedFileStoreLike
			if backend == "rawfile" {
				store = NewDiskFileStore(t.TempDir())
			} else {
				store = NewFileStore(t.TempDir(), false, backend, "")
			}
			defer store.Close()
			seedLegacyStore(t, store, path, encodedMetadata, encodedContent)
			if err := store.SetLegacyEncryptionKey([]byte("legacy-password")); err != nil {
				t.Fatal(err)
			}
			gotMetadata, gotContent, exists, err := store.Get(path)
			if err != nil || !exists || !bytes.Equal(gotMetadata, metadata) || !bytes.Equal(gotContent, content) {
				t.Fatalf("legacy fixture unreadable: exists=%t metadata=%q content=%q error=%v", exists, gotMetadata, gotContent, err)
			}
			gotMetadata, err = store.GetMetadata(path)
			if err != nil || !bytes.Equal(gotMetadata, metadata) {
				t.Fatalf("legacy metadata unreadable: %q, %v", gotMetadata, err)
			}
			gotContent, err = store.GetContent(path)
			if err != nil || !bytes.Equal(gotContent, content) {
				t.Fatalf("legacy content unreadable: %q, %v", gotContent, err)
			}
			visited := 0
			err = store.ScanMetadataPartition(context.Background(), types.PartitionIDForPath(path), func(filePath string, data []byte) error {
				visited++
				if filePath != path || !bytes.Equal(data, metadata) {
					t.Errorf("legacy partition scan returned %s: %q", filePath, data)
				}
				return nil
			})
			if err != nil || visited != 1 {
				t.Fatalf("legacy partition scan visited %d entries: %v", visited, err)
			}
			verifyLegacyStoreBytes(t, store, path, encodedMetadata, encodedContent)
			if err := store.Put(path, metadata, content); err != nil {
				t.Fatal(err)
			}
			if err := store.PutMetadata(path, metadata); err != nil {
				t.Fatal(err)
			}
			verifyLegacyStoreBytes(t, store, path, encodedMetadata, encodedContent)
			store.SetEncryptionKey(bytes.Repeat([]byte{7}, 32))
			if _, _, _, err := store.Get(path); err == nil {
				t.Fatal("authenticated store accepted legacy ciphertext")
			}
			verifyLegacyStoreBytes(t, store, path, encodedMetadata, encodedContent)
		})
	}
}

func TestLegacyDiskStreamKeepsKeyOffsets(t *testing.T) {
	metrics.NewMetricsCollector(&types.App{NodeID: "legacy-stream-test"})
	store := NewDiskFileStore(t.TempDir())
	defer store.Close()
	key := []byte("0123456789abcdef0123456789abcdef")
	if err := store.SetLegacyEncryptionKey(key); err != nil {
		t.Fatal(err)
	}
	path := "/legacy-stream.bin"
	content := bytes.Repeat([]byte{0x80}, 200017)
	metadata := storageTestMetadata(path, content)
	source := io.MultiReader(bytes.NewReader(content[:7]), bytes.NewReader(content[7:65539]), bytes.NewReader(content[65539:]))
	if err := store.PutStream(path, metadata, source, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	contentPath, err := store.contentPath(path)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(contentPath)
	if err != nil {
		t.Fatal(err)
	}
	block := decodeLegacyFixture(t, "b0b1b2b3b4b5b6b7b8b9e1e2e3e4e5e6b0b1b2b3b4b5b6b7b8b9e1e2e3e4e5e6")
	expected := bytes.Repeat(block, len(content)/len(block)+1)[:len(content)]
	if !bytes.Equal(encoded, expected) {
		t.Fatal("legacy disk stream changed ciphertext format or reset the key offset")
	}
	gotMetadata, reader, err := store.GetStream(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	first := make([]byte, 13)
	if _, err := io.ReadFull(reader, first); err != nil {
		t.Fatal(err)
	}
	rest, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(gotMetadata, metadata) || !bytes.Equal(append(first, rest...), content) {
		t.Fatalf("legacy disk stream read changed metadata or content: %v", err)
	}
}

func decodeLegacyFixture(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func seedLegacyStore(t *testing.T, store types.EncryptedFileStoreLike, path string, metadata, content []byte) {
	t.Helper()
	switch store := store.(type) {
	case *DiskFileStore:
		metadataPath, err := store.metadataPath(path)
		if err != nil {
			t.Fatal(err)
		}
		contentPath, err := store.contentPath(path)
		if err != nil {
			t.Fatal(err)
		}
		for filePath, data := range map[string][]byte{metadataPath: metadata, contentPath: content} {
			if err := ensureParentDir(filePath); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filePath, data, 0600); err != nil {
				t.Fatal(err)
			}
		}
	case *FileStore:
		partition := types.PartitionIDForPath(path)
		metadataKV, contentKV, err := store.openPartitionStores(types.ExtractPartitionStoreID(partition))
		if err != nil {
			t.Fatal(err)
		}
		key := encodeStoreKey(partition, path)
		if err := metadataKV.Put(key, metadata); err != nil {
			t.Fatal(err)
		}
		if err := contentKV.Put(key, content); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported fixture backend %T", store)
	}
}

func verifyLegacyStoreBytes(t *testing.T, store types.EncryptedFileStoreLike, path string, metadata, content []byte) {
	t.Helper()
	store.SetEncryptionKey(nil)
	gotMetadata, gotContent, exists, err := store.Get(path)
	if err != nil || !exists || !bytes.Equal(gotMetadata, metadata) || !bytes.Equal(gotContent, content) {
		t.Fatalf("persisted legacy bytes changed: exists=%t metadata match=%t content match=%t error=%v", exists, bytes.Equal(gotMetadata, metadata), bytes.Equal(gotContent, content), err)
	}
	if err := store.SetLegacyEncryptionKey([]byte("legacy-password")); err != nil {
		t.Fatal(err)
	}
}
