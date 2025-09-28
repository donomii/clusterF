package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/donomii/clusterF/types"
)

// handleFilesAPI handles file system API operations.
func (c *Cluster) handleFilesAPI(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/files")
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	switch r.Method {
	case http.MethodGet:
		c.handleFileGet(w, r, path)
	case http.MethodHead:
		c.handleFileHead(w, r, path)
	case http.MethodPut:
		c.handleFilePut(w, r, path)
	case http.MethodDelete:
		c.handleFileDelete(w, r, path)
	case http.MethodPost:
		c.handleFilePost(w, r, path)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (c *Cluster) handleFileGet(w http.ResponseWriter, r *http.Request, path string) {
	c.debugf("[FILES] GET request for path: %s", path)

	serveDirectory := func() {
		c.debugf("[FILES] Directory request for: %s", path)
		entries, err := c.FileSystem.ListDirectory(path)
		if err != nil {
			c.debugf("[FILES] Failed to list directory %s: %v", path, err)
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		response := map[string]interface{}{
			"path":    path,
			"entries": entries,
			"count":   len(entries),
		}
		json.NewEncoder(w).Encode(response)
	}

	if strings.HasSuffix(path, "/") {
		serveDirectory()
		return
	}

	content, metadata, err := c.FileSystem.GetFile(path)
	if err != nil {
		switch {
		case errors.Is(err, types.ErrIsDirectory):
			serveDirectory()
			return
		case errors.Is(err, types.ErrFileNotFound):
			c.debugf("[FILES] File %s not found", path)
			http.Error(w, "Not found", http.StatusNotFound)
			return
		default:
			c.Logger().Printf("[FILES] Failed to retrieve %s: %v", path, err)
			http.Error(w, "Failed to retrieve file", http.StatusInternalServerError)
			return
		}
	}

	c.debugf("[FILES] Retrieved file %s: %d bytes, content type: %s", path, len(content), metadata.ContentType)

	download := r.URL.Query().Get("download")
	filename := filepath.Base(path)
	if download == "1" || download == "true" {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	} else {
		ct := metadata.ContentType
		if ct == "" {
			ct = "application/octet-stream"
		}
		w.Header().Set("Content-Type", ct)
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", metadata.Size))
	w.Header().Set("Last-Modified", metadata.ModifiedAt.Format(http.TimeFormat))
	w.Header().Set("X-ClusterF-Created-At", metadata.CreatedAt.Format(time.RFC3339))
	if metadata.Checksum != "" {
		w.Header().Set("X-ClusterF-Checksum", metadata.Checksum)
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", metadata.Checksum))
	}
	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

func (c *Cluster) handleFileHead(w http.ResponseWriter, r *http.Request, path string) {
	c.debugf("[FILES] HEAD request for path: %s", path)

	metadata, err := c.FileSystem.GetMetadata(path)
	if err != nil {
		switch {
		case errors.Is(err, types.ErrIsDirectory):
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-ClusterF-Is-Directory", "true")
			w.WriteHeader(http.StatusOK)
			return
		case errors.Is(err, types.ErrFileNotFound):
			c.debugf("[FILES] HEAD metadata not found for %s", path)
			http.Error(w, "Not found", http.StatusNotFound)
			return
		default:
			c.Logger().Printf("[FILES] Failed HEAD metadata %s: %v", path, err)
			http.Error(w, "Failed to retrieve metadata", http.StatusInternalServerError)
			return
		}
	}

	if metadata.IsDirectory {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-ClusterF-Is-Directory", "true")
		w.WriteHeader(http.StatusOK)
		return
	}

	ct := metadata.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", metadata.Size))
	w.Header().Set("Last-Modified", metadata.ModifiedAt.Format(http.TimeFormat))
	w.Header().Set("X-ClusterF-Created-At", metadata.CreatedAt.Format(time.RFC3339))
	w.Header().Set("X-ClusterF-Is-Directory", "false")
	if metadata.Checksum != "" {
		w.Header().Set("X-ClusterF-Checksum", metadata.Checksum)
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", metadata.Checksum))
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Cluster) handleFilePut(w http.ResponseWriter, r *http.Request, path string) {
	content, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	if len(content) == 0 && r.Header.Get("Content-Type") == "" {
		c.debugf("[FILES] Ignoring empty upload for %s (likely directory)", path)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"path":    path,
			"message": "Directory ignored",
		})
		return
	}

	forwardedFrom := r.Header.Get("X-Forwarded-From")
	isForwarded := forwardedFrom != ""

	var metadata map[string]interface{}
	var modTime time.Time
	if isForwarded {
		metaHeader := r.Header.Get("X-ClusterF-Metadata")
		if metaHeader == "" {
			http.Error(w, "Missing forwarded metadata", http.StatusBadRequest)
			return
		}
		decoded, err := base64.StdEncoding.DecodeString(metaHeader)
		if err != nil {
			http.Error(w, "Invalid forwarded metadata encoding", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(decoded, &metadata); err != nil {
			http.Error(w, "Invalid forwarded metadata payload", http.StatusBadRequest)
			return
		}
		var parseErr error
		modTime, parseErr = parseForwardedModTime(metadata)
		if parseErr != nil {
			http.Error(w, fmt.Sprintf("%v", parseErr), http.StatusBadRequest)
			return
		}
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if isForwarded {
		if metadata != nil {
			if ct, ok := metadata["content_type"].(string); ok && ct != "" {
				contentType = ct
			}
		}
		if err := c.FileSystem.StoreFileWithModTime(path, content, contentType, modTime); err != nil {
			http.Error(w, fmt.Sprintf("Failed to store file: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		modHeader := r.Header.Get("X-ClusterF-Modified-At")
		if modHeader == "" {
			http.Error(w, "Missing X-ClusterF-Modified-At header", http.StatusBadRequest)
			return
		}
		localModTime, err := parseHeaderTimestamp(modHeader)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid X-ClusterF-Modified-At header: %v", err), http.StatusBadRequest)
			return
		}
		if err := c.FileSystem.StoreFileWithModTime(path, content, contentType, localModTime); err != nil {
			http.Error(w, fmt.Sprintf("Failed to store file: %v", err), http.StatusInternalServerError)
			return
		}
	}

	c.Logger().Printf("[FILES] Stored %s (%d bytes)", path, len(content))
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"path":    path,
		"size":    len(content),
	})
}

func parseForwardedModTime(meta map[string]interface{}) (time.Time, error) {
	if meta == nil {
		return time.Time{}, fmt.Errorf("forwarded metadata missing")
	}
	if raw, ok := meta["version"]; ok {
		switch v := raw.(type) {
		case float64:
			if v != 0 {
				nanos := int64(v)
				return time.Unix(0, nanos), nil
			}
		case string:
			if v != "" {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					return time.Unix(0, n), nil
				}
			}
		}
	}
	if raw, ok := meta["modified_at"]; ok {
		switch v := raw.(type) {
		case float64:
			return time.Unix(int64(v), 0), nil
		case string:
			if v == "" {
				break
			}
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				return t, nil
			}
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				return t, nil
			}
			if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
				return time.Unix(secs, 0), nil
			}
		}
	}
	return time.Time{}, fmt.Errorf("forwarded metadata missing modified time")
}

func parseHeaderTimestamp(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("empty timestamp header")
	}
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}
	if strings.Contains(value, ".") {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			secs := int64(f)
			nanos := int64((f - float64(secs)) * 1e9)
			return time.Unix(secs, nanos), nil
		}
	}
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		abs := i
		if abs < 0 {
			abs = -abs
		}
		switch {
		case abs >= 1_000_000_000_000_000_000:
			return time.Unix(0, i), nil
		case abs >= 1_000_000_000_000_000:
			return time.Unix(0, i*1_000), nil
		case abs >= 1_000_000_000_000:
			return time.Unix(0, i*1_000_000), nil
		default:
			return time.Unix(i, 0), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format")
}

func (c *Cluster) handleFileDelete(w http.ResponseWriter, r *http.Request, path string) {
	if err := c.FileSystem.DeleteFile(path); err != nil {
		if errors.Is(err, types.ErrFileNotFound) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to delete file: %v", err), http.StatusInternalServerError)
		return
	}

	c.Logger().Printf("[FILES] Deleted %s", path)
	w.WriteHeader(http.StatusNoContent)
}

func (c *Cluster) handleFilePost(w http.ResponseWriter, r *http.Request, path string) {
	createDir := strings.EqualFold(r.Header.Get("X-Create-Directory"), "true")
	if !createDir {
		http.Error(w, "unsupported operation", http.StatusBadRequest)
		return
	}

	if err := c.FileSystem.CreateDirectory(path); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create directory: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"path":    path,
	})
}
