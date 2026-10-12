package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/donomii/clusterF/storagecrypto"
)

const (
	encryptionModeAuto          = "auto"
	encryptionModeLegacy        = "legacy"
	encryptionModeAuthenticated = "authenticated"
	legacyVerificationPhrase    = "clusterF-encryption-test"
)

type storageEncryption struct {
	settings         *storagecrypto.Settings
	key              []byte
	legacyTestPhrase string
}

func prepareStorageEncryption(opts ClusterOpts, existing *StorageSettings) (storageEncryption, error) {
	mode := opts.EncryptionMode
	if mode == "" {
		mode = encryptionModeAuto
	}
	switch mode {
	case encryptionModeAuto, encryptionModeLegacy, encryptionModeAuthenticated:
	default:
		return storageEncryption{}, fmt.Errorf("invalid encryption mode %q: expected auto, legacy, or authenticated", mode)
	}
	if existing != nil {
		return openStorageEncryption(opts.EncryptionKey, mode, existing)
	}
	for _, directory := range []string{"rawfiles", "partitions"} {
		path := filepath.Join(opts.DataDir, directory)
		entries, err := os.ReadDir(path)
		if err != nil && !os.IsNotExist(err) {
			return storageEncryption{}, fmt.Errorf("checking existing storage %s: %w", path, err)
		}
		if len(entries) != 0 {
			return storageEncryption{}, fmt.Errorf("storage %s contains data but repository settings are missing", path)
		}
	}
	if opts.EncryptionKey == "" {
		if mode != encryptionModeAuto {
			return storageEncryption{}, fmt.Errorf("encryption mode %q requires --encryption-key", mode)
		}
		return storageEncryption{}, nil
	}
	if mode == encryptionModeLegacy {
		key := []byte(opts.EncryptionKey)
		verification, err := storagecrypto.LegacyEncode(key, []byte(legacyVerificationPhrase))
		return storageEncryption{key: key, legacyTestPhrase: hex.EncodeToString(verification)}, err
	}
	settings, key, err := storagecrypto.Create(opts.EncryptionKey)
	return storageEncryption{settings: settings, key: key}, err
}

func openStorageEncryption(password string, mode string, existing *StorageSettings) (storageEncryption, error) {
	if existing.EncryptedTestPhrase != "" && existing.Encryption != nil {
		return storageEncryption{}, fmt.Errorf("repository settings contain both legacy and authenticated encryption markers")
	}
	storedMode := "unencrypted"
	if existing.EncryptedTestPhrase != "" {
		storedMode = encryptionModeLegacy
	} else if existing.Encryption != nil {
		storedMode = encryptionModeAuthenticated
	}
	if mode != encryptionModeAuto && mode != storedMode {
		return storageEncryption{}, fmt.Errorf("encryption mode %q does not match repository format %q; repository data has not been changed", mode, storedMode)
	}
	switch storedMode {
	case encryptionModeLegacy:
		return unlockLegacyStorageEncryption(password, existing.EncryptedTestPhrase)
	case encryptionModeAuthenticated:
		key, err := storagecrypto.Unlock(password, existing.Encryption)
		return storageEncryption{settings: existing.Encryption, key: key}, err
	default:
		if password != "" {
			return storageEncryption{}, fmt.Errorf("cannot enable encryption on an existing unencrypted repository without explicit migration")
		}
		return storageEncryption{}, nil
	}
}

func unlockLegacyStorageEncryption(password string, verification string) (storageEncryption, error) {
	if password == "" {
		return storageEncryption{}, fmt.Errorf("legacy encrypted repository requires --encryption-key")
	}
	encoded, err := hex.DecodeString(verification)
	if err != nil {
		return storageEncryption{}, fmt.Errorf("invalid legacy encryption verification value: %w", err)
	}
	key := []byte(password)
	plain, err := storagecrypto.LegacyDecode(key, encoded)
	if err != nil || !bytes.Equal(plain, []byte(legacyVerificationPhrase)) {
		return storageEncryption{}, fmt.Errorf("legacy encryption password is incorrect or repository encryption settings are damaged")
	}
	return storageEncryption{key: key, legacyTestPhrase: verification}, nil
}
