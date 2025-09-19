package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"
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
		case errors.Is(err, ErrIsDirectory):
			serveDirectory()
			return
		case errors.Is(err, ErrFileNotFound):
			c.debugf("[FILES] File %s not found", path)
			http.Error(w, "Not found", http.StatusNotFound)
			return
		default:
			c.Logger.Printf("[FILES] Failed to retrieve %s: %v", path, err)
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
	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

func (c *Cluster) handleFileHead(w http.ResponseWriter, r *http.Request, path string) {
	c.debugf("[FILES] HEAD request for path: %s", path)

	metadata, err := c.FileSystem.getMetadata(path)
	if err != nil {
		switch {
		case errors.Is(err, ErrIsDirectory):
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-ClusterF-Is-Directory", "true")
			w.WriteHeader(http.StatusOK)
			return
		case errors.Is(err, ErrFileNotFound):
			c.debugf("[FILES] HEAD metadata not found for %s", path)
			http.Error(w, "Not found", http.StatusNotFound)
			return
		default:
			c.Logger.Printf("[FILES] Failed HEAD metadata %s: %v", path, err)
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

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if err := c.FileSystem.StoreFile(path, content, contentType); err != nil {
		http.Error(w, fmt.Sprintf("Failed to store file: %v", err), http.StatusInternalServerError)
		return
	}

	c.Logger.Printf("[FILES] Stored %s (%d bytes)", path, len(content))
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"path":    path,
		"size":    len(content),
	})
}

func (c *Cluster) handleFileDelete(w http.ResponseWriter, r *http.Request, path string) {
	if err := c.FileSystem.DeleteFile(path); err != nil {
		if errors.Is(err, ErrFileNotFound) {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to delete file: %v", err), http.StatusInternalServerError)
		return
	}

	c.Logger.Printf("[FILES] Deleted %s", path)
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
