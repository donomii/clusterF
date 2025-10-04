package frontend

import (
	_ "embed"
	"errors"
	"os"
	"path/filepath"
)

// Embed the default static assets for the UI.

//go:embed file-browser.html
var embeddedFileBrowser []byte

//go:embed cluster-visualizer.html
var embeddedClusterVisualizer []byte

//go:embed crdt-inspector.html
var embeddedCRDTInspector []byte

var embeddedAssets = map[string][]byte{
	"file-browser.html":       embeddedFileBrowser,
	"cluster-visualizer.html": embeddedClusterVisualizer,
	"crdt-inspector.html":     embeddedCRDTInspector,
}

func loadAsset(name string) ([]byte, error) {
	if data, ok := embeddedAssets[name]; ok && len(data) > 0 {
		return data, nil
	}
	if data, err := os.ReadFile(name); err == nil {
		return data, nil
	}
	if data, err := os.ReadFile(filepath.Join("frontend", name)); err == nil {
		return data, nil
	}
	if data, ok := embeddedAssets[name]; ok && len(data) == 0 {
		return nil, errors.New("frontend asset embedded but empty: " + name)
	}
	return nil, errors.New("frontend asset not found: " + name)
}
