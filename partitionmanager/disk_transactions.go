package partitionmanager

import (
	"errors"
	"fmt"
	"os"
)

func (fs *DiskFileStore) publishPair(contentTemp, metadataTemp, contentPath, metadataPath string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	backupFile, err := os.CreateTemp(fs.transferDir, "previous-content-*")
	if err != nil {
		return err
	}
	backup := backupFile.Name()
	if err := backupFile.Close(); err != nil {
		return err
	}
	if err := os.Remove(backup); err != nil {
		return err
	}
	backupErr := os.Link(contentPath, backup)
	if backupErr != nil && !errors.Is(backupErr, os.ErrNotExist) {
		return fmt.Errorf("preserving previous content %s: %w", contentPath, backupErr)
	}
	preserveBackup := false
	defer func() {
		if !preserveBackup {
			os.Remove(backup)
		}
	}()
	if err := os.Rename(contentTemp, contentPath); err != nil {
		return err
	}
	if err := os.Rename(metadataTemp, metadataPath); err != nil {
		var restoreErr error
		if backupErr == nil {
			restoreErr = os.Rename(backup, contentPath)
		} else {
			restoreErr = os.Remove(contentPath)
		}
		if restoreErr != nil {
			preserveBackup = backupErr == nil
			return fmt.Errorf("publishing metadata %s failed: %w; restoring content %s failed: %v; previous content backup=%s", metadataPath, err, contentPath, restoreErr, backup)
		}
		return fmt.Errorf("publishing metadata %s failed; previous content restored: %w", metadataPath, err)
	}
	return nil
}

func writeFileAtomically(path string, content []byte, transferDir string) error {
	file, err := os.CreateTemp(transferDir, ".clusterf-write-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err := file.Write(content); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
