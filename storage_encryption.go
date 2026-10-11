package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/donomii/clusterF/storagecrypto"
)

func prepareStorageEncryption(opts ClusterOpts, existing *StorageSettings) (*storagecrypto.Settings, []byte, error) {
	if existing != nil {
		if existing.EncryptedTestPhrase != "" {
			return nil, nil, fmt.Errorf("legacy repeating-key encryption is unsupported; repository data has not been changed and requires explicit migration")
		}
		if existing.Encryption != nil {
			key, err := storagecrypto.Unlock(opts.EncryptionKey, existing.Encryption)
			return existing.Encryption, key, err
		}
		if opts.EncryptionKey != "" {
			return nil, nil, fmt.Errorf("cannot enable encryption on an existing unencrypted repository without explicit migration")
		}
		return nil, nil, nil
	}
	for _, directory := range []string{"rawfiles", "partitions"} {
		path := filepath.Join(opts.DataDir, directory)
		entries, err := os.ReadDir(path)
		if err != nil && !os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("checking existing storage %s: %w", path, err)
		}
		if len(entries) != 0 {
			return nil, nil, fmt.Errorf("storage %s contains data but repository settings are missing", path)
		}
	}
	if opts.EncryptionKey == "" {
		return nil, nil, nil
	}
	return storagecrypto.Create(opts.EncryptionKey)
}
