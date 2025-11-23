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

//go:embed profiling.html
var embeddedProfiling []byte

//go:embed profiling.js
var embeddedProfilingJS []byte

//go:embed monitor.html
var embeddedMonitor []byte

//go:embed monitor.js
var embeddedMonitorJS []byte

//go:embed metrics.html
var embeddedMetrics []byte

//go:embed metrics.js
var embeddedMetricsJS []byte

var embeddedAssets = map[string][]byte{
	"file-browser.html":       embeddedFileBrowser,
	"cluster-visualizer.html": embeddedClusterVisualizer,
	"crdt-inspector.html":     embeddedCRDTInspector,
	"profiling.html":          embeddedProfiling,
	"profiling.js":            embeddedProfilingJS,
	"monitor.html":            embeddedMonitor,
	"monitor.js":              embeddedMonitorJS,
	"metrics.html":            embeddedMetrics,
	"metrics.js":              embeddedMetricsJS,
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
