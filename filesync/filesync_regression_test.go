package filesync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/donomii/clusterF/types"
)

func TestMetadataMatchesRequiresContentAndExactTime(t *testing.T) {
	modified := time.Now()
	checksum := sha256.Sum256([]byte("old"))
	metadata := types.FileMetadata{
		Size: 3, ModifiedAt: modified, Checksum: hex.EncodeToString(checksum[:]),
	}
	if !metadataMatches(metadata, []byte("old"), modified) {
		t.Fatal("identical content and timestamp should match")
	}
	if metadataMatches(metadata, []byte("new"), modified) {
		t.Fatal("same-size changed content was skipped")
	}
	if metadataMatches(metadata, []byte("old"), modified.Add(time.Millisecond)) {
		t.Fatal("subsecond timestamp change was skipped")
	}
}

func TestImportFromDirReportsWalkFailure(t *testing.T) {
	syncer := &Syncer{importDir: filepath.Join(t.TempDir(), "missing")}
	if err := syncer.importFromDir(context.Background()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing import directory error = %v", err)
	}
}

func TestImportFromDirReportsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	syncer := &Syncer{importDir: t.TempDir()}
	if err := syncer.importFromDir(ctx); err == nil {
		t.Fatal("canceled import reported success")
	}
}
