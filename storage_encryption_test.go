package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/donomii/clusterF/storagecrypto"
)

func TestEncryptedStartupRejectsInvalidLegacyBeforeWrites(t *testing.T) {
	base := t.TempDir()
	directory := filepath.Join(base, "legacy-node")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	settings := StorageSettings{StorageMajor: "rawfile", EncryptedTestPhrase: "legacy-ciphertext"}
	encoded, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	settingsPath := filepath.Join(directory, "settings.json")
	if err := os.WriteFile(settingsPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	var rejection string
	func() {
		defer func() { rejection = fmt.Sprint(recover()) }()
		NewCluster(ClusterOpts{ID: "legacy-node", DataDir: base, StorageMajor: "rawfile", EncryptionKey: "password", Logger: log.New(io.Discard, "", 0)})
	}()
	if !strings.Contains(rejection, "invalid legacy encryption verification") {
		t.Fatalf("invalid legacy verification value was not rejected: %s", rejection)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 1 || entries[0].Name() != "settings.json" {
		t.Fatalf("rejected startup changed repository files: %v, %v", entries, err)
	}
	after, err := os.ReadFile(settingsPath)
	if err != nil || !bytes.Equal(after, encoded) {
		t.Fatalf("rejected startup changed settings: %q, %v", after, err)
	}
}

func TestStorageEncryptionRequiresOriginalModeAndPassword(t *testing.T) {
	settings, expectedKey, err := storagecrypto.Create("correct password")
	if err != nil {
		t.Fatal(err)
	}
	existing := &StorageSettings{Encryption: settings}
	encryption, err := prepareStorageEncryption(ClusterOpts{EncryptionKey: "correct password"}, existing)
	if err != nil || !bytes.Equal(encryption.key, expectedKey) {
		t.Fatalf("correct password rejected: %v", err)
	}
	for _, password := range []string{"", "incorrect password"} {
		if _, err := prepareStorageEncryption(ClusterOpts{EncryptionKey: password}, existing); err == nil {
			t.Fatal("missing or incorrect password accepted")
		}
	}
	if _, err := prepareStorageEncryption(ClusterOpts{EncryptionKey: "new password"}, &StorageSettings{}); err == nil {
		t.Fatal("encryption enabled on an existing plaintext repository")
	}
}

func TestStorageEncryptionRejectsInvalidModesAndMixedMarkers(t *testing.T) {
	for _, mode := range []string{"unknown", encryptionModeLegacy, encryptionModeAuthenticated} {
		_, err := prepareStorageEncryption(ClusterOpts{DataDir: t.TempDir(), EncryptionMode: mode}, nil)
		if err == nil {
			t.Fatalf("accepted encryption mode %q without a password", mode)
		}
	}
	settings := &StorageSettings{EncryptedTestPhrase: "00", Encryption: &storagecrypto.Settings{}}
	_, err := prepareStorageEncryption(ClusterOpts{EncryptionKey: "password"}, settings)
	if err == nil || !strings.Contains(err.Error(), "both legacy and authenticated") {
		t.Fatalf("ambiguous encryption markers were not rejected: %v", err)
	}
}
