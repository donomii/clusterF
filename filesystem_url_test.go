package main

import (
	"net/url"
	"strings"
	"testing"

	"github.com/donomii/clusterF/discovery"
)

func TestBuildPeerFileURL(t *testing.T) {
	peer := &discovery.PeerInfo{Address: "node.example", HTTPPort: 8080}

	trickyName := "🌎🌎🌎.        #learnjapanese #japaneseculture - Real Real Japan (1080p, h264, youtube, a_AbypkmBe4).mp4"

	testCases := []struct {
		name string
		path string
	}{
		{name: "RootFile", path: "/simple.txt"},
		{name: "DeepPath", path: "/folder/sub folder/file name.txt"},
		{name: "EmojiAndHash", path: "/videos/" + trickyName},
		{name: "LeadingSlashAdded", path: "relative/path.txt"},
		{name: "PercentAndQuestion", path: "/weird/100% legit?.txt"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildPeerFileURL(peer, tc.path)
			if err != nil {
				t.Fatalf("buildPeerFileURL returned error: %v", err)
			}

			parsed, err := url.Parse(got)
			if err != nil {
				t.Fatalf("output is not a valid URL: %v", err)
			}

			normalizedPath := tc.path
			if !strings.HasPrefix(normalizedPath, "/") {
				normalizedPath = "/" + normalizedPath
			}
			expectedPath := "/api/files" + normalizedPath

			if parsed.Path != expectedPath {
				t.Fatalf("unexpected path: got %q want %q", parsed.Path, expectedPath)
			}

			expectedEscaped := (&url.URL{Path: expectedPath}).EscapedPath()
			if parsed.EscapedPath() != expectedEscaped {
				t.Fatalf("unexpected escaped path: got %q want %q", parsed.EscapedPath(), expectedEscaped)
			}

			if parsed.RawQuery != "" {
				t.Fatalf("expected empty query, got %q", parsed.RawQuery)
			}

			if parsed.Fragment != "" {
				t.Fatalf("expected empty fragment, got %q", parsed.Fragment)
			}
		})
	}
}

func TestBuildPeerFileURLRequiresPeer(t *testing.T) {
	if _, err := buildPeerFileURL(nil, "/foo"); err == nil {
		t.Fatalf("expected error when peer is nil")
	}
}
