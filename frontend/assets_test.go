package frontend

import (
    "strings"
    "testing"
)

func TestLoadAssetFileBrowser(t *testing.T) {
    data, err := loadAsset("file-browser.html")
    if err != nil {
        t.Fatalf("loadAsset failed: %v", err)
    }
    if !strings.Contains(string(data), "Cluster File Browser") {
        t.Fatalf("unexpected asset contents: %q", data[:min(len(data), 30)])
    }
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
