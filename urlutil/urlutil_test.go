package urlutil

import "testing"

func TestEncodePathPreservesTrailingSlash(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "root", input: "/", expected: "/"},
		{name: "single", input: "/folder", expected: "/folder"},
		{name: "trailing", input: "/folder/", expected: "/folder/"},
		{name: "spaces", input: "/folder name/file #1.txt", expected: "/folder%20name/file%20%231.txt"},
		{name: "alreadyEncoded", input: "/folder%20name/file.txt", expected: "/folder%20name/file.txt"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := EncodePath(tc.input); got != tc.expected {
				t.Fatalf("EncodePath(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestBuildHTTPURL(t *testing.T) {
	url, err := BuildHTTPURL("example.com", 8080, "/frogpond/full store")
	if err != nil {
		t.Fatalf("BuildHTTPURL returned error: %v", err)
	}

	expected := "http://example.com:8080/frogpond/full%20store"
	if url != expected {
		t.Fatalf("BuildHTTPURL returned %q, want %q", url, expected)
	}
}

func TestBuildFilesURL(t *testing.T) {
	url, err := BuildFilesURL("node.example", 9000, "/🌎/mix of spaces #hash?.txt")
	if err != nil {
		t.Fatalf("BuildFilesURL returned error: %v", err)
	}

	expected := "http://node.example:9000/api/files/%F0%9F%8C%8E/mix%20of%20spaces%20%23hash%3F.txt"
	if url != expected {
		t.Fatalf("BuildFilesURL returned %q, want %q", url, expected)
	}
}

func TestBuildFilesURLRequiresAddress(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for empty address, path /foo, port 1234")
		}
	}()
	_, _ = BuildFilesURL("", 1234, "/foo")
}

func TestBuildHTTPURLRequiresPort(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for invalid port, path /foo, port 0")
		}
	}()
	_, _ = BuildHTTPURL("example.com", 0, "/foo")
}
