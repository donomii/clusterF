package urlutil

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/donomii/clusterF/types"
)

func normalizeAbsolutePath(path string) string {
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

func encodePath(path string) (string, string) {
	normalized := normalizeAbsolutePath(path)
	parts := strings.Split(normalized, "/")
	encodedParts := make([]string, len(parts))
	decodedParts := make([]string, len(parts))
	for i := range parts {
		if i == 0 {
			encodedParts[i] = ""
			decodedParts[i] = ""
			continue
		}
		if parts[i] == "" {
			encodedParts[i] = ""
			decodedParts[i] = ""
			continue
		}
		segment := parts[i]
		if unescaped, err := url.PathUnescape(segment); err == nil {
			segment = unescaped
		}
		decodedParts[i] = segment
		encodedParts[i] = url.PathEscape(segment)
	}
	encoded := strings.Join(encodedParts, "/")
	decoded := strings.Join(decodedParts, "/")
	if !strings.HasPrefix(decoded, "/") {
		decoded = "/" + decoded
	}
	if !strings.HasPrefix(encoded, "/") {
		encoded = "/" + encoded
	}
	return decoded, encoded
}

func BuildHTTPURL(address string, port int, rawPath string) (string, error) {
	types.Assertf(address != "", "address is required, path: %v, port %v", rawPath, port)
	types.Assertf(port > 0, "port must be positive, path: %v, port %v", rawPath, port)

	decodedPath, _ := encodePath(rawPath)
	u := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", address, port),
		Path:   decodedPath,
	}
	return u.String(), nil
}

func BuildFilesURL(address string, port int, filePath string) (string, error) {
	normalized := normalizeAbsolutePath(filePath)
	return BuildHTTPURL(address, port, "/api/files"+normalized)
}

func BuildInternalFilesURL(address string, port int, filePath string) (string, error) {
	normalized := normalizeAbsolutePath(filePath)
	return BuildHTTPURL(address, port, "/internal/files"+normalized)
}

func BuildInternalMetadataURL(address string, port int, filePath string) (string, error) {
	normalized := normalizeAbsolutePath(filePath)
	return BuildHTTPURL(address, port, "/internal/metadata"+normalized)
}

func BuildInternalSearchURL(address string, port int) (string, error) {
	return BuildHTTPURL(address, port, "/internal/search")
}

func EncodePath(path string) string {
	_, encoded := encodePath(path)
	return encoded
}

func NormalizeAbsolutePath(path string) string {
	return normalizeAbsolutePath(path)
}
