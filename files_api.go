package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/donomii/clusterF/httpclient"
	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
)

// handleInternalFilesAPI handles internal peer-to-peer file operations
// Separate endpoint for internal cluster communication at /internal/files/
func (c *Cluster) handleInternalFilesAPI(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/internal/files")
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	switch r.Method {
	case http.MethodGet:
		c.handleFileGetInternal(w, r, path)
	case http.MethodHead:
		c.handleFileHeadInternal(w, r, path)
	case http.MethodPut:
		c.handleFilePutInternal(w, r, path)
	case http.MethodDelete:
		c.handleFileDeleteInternal(w, r, path)
	case http.MethodPost:
		c.handleFilePostInternal(w, r, path)
	default:
		http.Error(w, fmt.Sprintf("Method %s not allowed for /internal/files (supported: GET, HEAD, PUT, DELETE, POST)", r.Method), http.StatusMethodNotAllowed)
	}
}

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
		http.Error(w, fmt.Sprintf("Method %s not allowed for /api/files (supported: GET, HEAD, PUT, DELETE, POST)", r.Method), http.StatusMethodNotAllowed)
	}
}

// handleFileGetInternal handles internal peer-to-peer file GET requests
// Only queries local storage, never forwards to other peers
func (c *Cluster) handleFileGetInternal(w http.ResponseWriter, r *http.Request, path string) {
	c.debugf("[FILES] Internal GET request for path: %s", path)

	serveDirectory := func() {
		c.debugf("[FILES] Internal directory request for: %s", path)
		entries, err := c.FileSystem.ListDirectory(path)
		if err != nil {
			c.debugf("[FILES] Failed to list directory %s: %v", path, err)
			http.Error(w, fmt.Sprintf("Directory not found or listing failed: %s", path), http.StatusNotFound)
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

	// Only check local storage, never forward to peers
	content, metadata, err := c.FileSystem.GetFile(path)
	if err != nil {
		switch {
		case errors.Is(err, types.ErrIsDirectory):
			serveDirectory()
			return
		case errors.Is(err, types.ErrFileNotFound):
			detail := err.Error()
			if strings.HasPrefix(detail, types.ErrFileNotFound.Error()) {
				detail = strings.TrimPrefix(detail, types.ErrFileNotFound.Error())
				detail = strings.TrimPrefix(detail, ": ")
			}
			if detail == "" {
				detail = path
			}
			message := fmt.Sprintf("File not found on holder %s: %s", string(c.ID()), detail)
			c.debugf("[FILES] %s", message)
			http.Error(w, message, http.StatusNotFound)
			return
		case strings.Contains(fmt.Sprintf("%v", err), "not found for file"): // Legacy error strings from older storage layers
			message := fmt.Sprintf("File not found on holder %s: %v", string(c.ID()), err)
			c.debugf("[FILES] %s", message)
			http.Error(w, message, http.StatusNotFound)
			return
		default:
			c.debugf("[FILES] Failed to retrieve %s: %v", path, err)
			http.Error(w, fmt.Sprintf("Failed to retrieve file: %v", err), http.StatusInternalServerError)
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
	w.Header().Set("X-ClusterF-Created-At", metadata.CreatedAt.Format(time.RFC3339))
	w.Header().Set("X-ClusterF-Modified-At", metadata.ModifiedAt.Format(time.RFC3339))
	if metadata.Checksum != "" {
		w.Header().Set("X-ClusterF-Checksum", metadata.Checksum)
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", metadata.Checksum))
	} else {
		panic("no")
	}
	w.WriteHeader(http.StatusOK)
	w.Write(content)
}

// handleFileGet handles external client file requests
// Queries only the nodes that hold the partition for this file
func (c *Cluster) handleFileGet(w http.ResponseWriter, r *http.Request, path string) {
	c.debugf("[FILES] External GET request for path: %s", path)

	if strings.HasSuffix(path, "/") {
		c.serveDirectoryListing(w, path)
		return
	}

	// Get partition info to find which nodes hold this file
	partitionID := c.PartitionManager().CalculatePartitionName(path)
	partitionInfo := c.PartitionManager().GetPartitionInfo(types.PartitionID(partitionID))

	if partitionInfo == nil {
		c.debugf("[FILES] No partition info found for %s (partition %s)", path, partitionID)
		http.Error(w, fmt.Sprintf("File not found in cluster: %s (no partition info found for partition %s)", path, partitionID), http.StatusNotFound)
		return
	}

	if len(partitionInfo.Holders) == 0 {
		c.debugf("[FILES] No holders registered for partition %s (file %s)", partitionID, path)
		http.Error(w, fmt.Sprintf("File not found in cluster: %s (partition %s has no holders registered)", path, partitionID), http.StatusNotFound)
		return
	}

	// Get peer info for all holders
	peers := c.DiscoveryManager().GetPeers()
	peerLookup := make(map[types.NodeID]*types.PeerInfo)
	for _, peer := range peers {
		peerLookup[types.NodeID(peer.NodeID)] = peer
	}

	c.debugf("[FILES] Partition %s for file %s has holders: %v", partitionID, path, partitionInfo.Holders)

	// Try each holder until we find the file
	var holderErrors []string
	for _, holderID := range partitionInfo.Holders {
		c.debugf("[FILES] Trying holder %s for file %s", holderID, path)

		// Get peer info for this holder
		var peer *types.PeerInfo
		if holderID == c.ID() {
			c.debugf("[FILES] Fetching file from localhost via HTTP")
			// This is us, but still go through HTTP path for consistency
			peer = &types.PeerInfo{
				NodeID:   c.ID(),
				Address:  c.DiscoveryManager().GetLocalAddress(),
				HTTPPort: c.HTTPPort(),
			}
		} else if p, ok := peerLookup[holderID]; ok {
			c.debugf("Fetching file from peer %+v", p)
			peer = p
		} else {
			c.debugf("[FILES] No peer info available for holder %s", holderID)
			holderErrors = append(holderErrors, fmt.Sprintf("%s: no peer info", holderID))
			continue
		}

		// Build URL for this peer
		fileURL, err := urlutil.BuildInternalFilesURL(peer.Address, peer.HTTPPort, path)
		if err != nil {
			c.debugf("[FILES] Failed to build URL for peer %s: %v", peer.NodeID, err)
			holderErrors = append(holderErrors, fmt.Sprintf("%s: URL build failed: %v", peer.NodeID, err))
			continue
		}

		options := []httpclient.RequestOption{
			httpclient.WithHeader("X-ClusterF-Internal", "1"),
		}
		if download := r.URL.Query().Get("download"); download != "" {
			options = append(options, httpclient.WithQueryParam("download", download))
		}

		body, headers, status, err := httpclient.SimpleGet(r.Context(), c.HttpDataClient, fileURL, options...)
		if err != nil {
			c.debugf("[FILES] Failed to get file from peer %s: %v", peer.NodeID, err)
			holderErrors = append(holderErrors, fmt.Sprintf("%s: HTTP request failed: %v", peer.NodeID, err))
			continue
		}

		if status == http.StatusOK {
			c.debugf("[FILES] Found file %s on holder %s", path, peer.NodeID)

			// Copy all headers from peer response
			for key, values := range headers {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}

			w.WriteHeader(http.StatusOK)
			if _, err := w.Write(body); err != nil {
				c.debugf("[FILES] Failed streaming response body from %s: %v", peer.NodeID, err)
			}
			return
		}

		// Read error response body for more detailed error information
		msg := strings.TrimSpace(string(body))
		holderErrors = append(holderErrors, strings.TrimSpace(fmt.Sprintf("%s: %d %s", peer.NodeID, status, msg)))
		c.debugf("[FILES] Holder %s returned %d for file %s: %s", peer.NodeID, status, path, msg)
	}

	// File not found on any registered holder
	c.debugf("[FILES] File %s not found on any registered holder for partition %s", path, partitionID)
	errorSummary := fmt.Sprintf("File not found in cluster: %s", path)
	if len(holderErrors) > 0 {
		errorSummary += fmt.Sprintf(" (tried holders: %s)", strings.Join(holderErrors, ", "))
	}
	http.Error(w, errorSummary, http.StatusNotFound)
}

func (c *Cluster) serveDirectoryListing(w http.ResponseWriter, path string) {
	c.debugf("[FILES] Directory request for: %s", path)
	entries, err := c.FileSystem.ListDirectory(path)
	if err != nil {
		c.debugf("[FILES] Failed to list directory %s: %v", path, err)
		http.Error(w, fmt.Sprintf("Directory not found or listing failed: %s", path), http.StatusNotFound)
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

func (c *Cluster) handleFileHead(w http.ResponseWriter, r *http.Request, path string) {
	c.debugf("[FILES] HEAD request for path: %s", path)

	// FIXME we should do a basic search here to see if there are any files in this directory tree
	if strings.HasSuffix(path, "/") {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-ClusterF-Is-Directory", "true")
		w.WriteHeader(http.StatusOK)
		return
	}

	partitionID := c.PartitionManager().CalculatePartitionName(path)
	partitionInfo := c.PartitionManager().GetPartitionInfo(types.PartitionID(partitionID))

	if partitionInfo == nil {
		c.debugf("[FILES] No partition info found for %s (partition %s)", path, partitionID)
		http.Error(w, fmt.Sprintf("File metadata not found: %s (no partition info found for partition %s)", path, partitionID), http.StatusNotFound)
		return
	}

	if len(partitionInfo.Holders) == 0 {
		c.debugf("[FILES] No holders registered for partition %s (file %s)", partitionID, path)
		http.Error(w, fmt.Sprintf("File metadata not found: %s (partition %s has no holders registered)", path, partitionID), http.StatusNotFound)
		return
	}

	peers := c.DiscoveryManager().GetPeers()
	peerLookup := make(map[types.NodeID]*types.PeerInfo)
	for _, peer := range peers {
		peerLookup[types.NodeID(peer.NodeID)] = peer
	}

	c.debugf("[FILES] Partition %s for file %s has holders: %v", partitionID, path, partitionInfo.Holders)

	var holderErrors []string
	for _, holderID := range partitionInfo.Holders {
		c.debugf("[FILES] Trying holder %s for HEAD on %s", holderID, path)

		var peer *types.PeerInfo
		if holderID == c.ID() {
			c.debugf("[FILES] Fetching HEAD metadata from localhost via HTTP")
			peer = &types.PeerInfo{
				NodeID:   c.ID(),
				Address:  c.DiscoveryManager().GetLocalAddress(),
				HTTPPort: c.HTTPPort(),
			}
		} else if p, ok := peerLookup[holderID]; ok {
			c.debugf("Fetching HEAD metadata from peer %+v", p)
			peer = p
		} else {
			c.debugf("[FILES] No peer info available for holder %s", holderID)
			holderErrors = append(holderErrors, fmt.Sprintf("%s: no peer info", holderID))
			continue
		}

		fileURL, err := urlutil.BuildInternalFilesURL(peer.Address, peer.HTTPPort, path)
		if err != nil {
			c.debugf("[FILES] Failed to build URL for peer %s: %v", peer.NodeID, err)
			holderErrors = append(holderErrors, fmt.Sprintf("%s: URL build failed: %v", peer.NodeID, err))
			continue
		}

		headers, status, err := httpclient.SimpleHead(r.Context(), c.HttpDataClient, fileURL,
			httpclient.WithHeader("X-ClusterF-Internal", "1"),
		)
		if err != nil {
			c.debugf("[FILES] Failed HEAD metadata from peer %s: %v", peer.NodeID, err)
			holderErrors = append(holderErrors, fmt.Sprintf("%s: HTTP request failed: %v", peer.NodeID, err))
			continue
		}

		if status == http.StatusOK {
			for key, values := range headers {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		holderErrors = append(holderErrors, fmt.Sprintf("%s: %d", peer.NodeID, status))
		c.debugf("[FILES] Holder %s returned %d for HEAD %s", peer.NodeID, status, path)

		if w.Header().Get("Content-Type") != "" || w.Header().Get("X-ClusterF-Is-Directory") != "" {
			return
		}
	}

	c.debugf("[FILES] Metadata for %s not found on any registered holder for partition %s", path, partitionID)
	errorSummary := fmt.Sprintf("File metadata not found in cluster: %s", path)
	if len(holderErrors) > 0 {
		errorSummary += fmt.Sprintf(" (tried holders: %s)", strings.Join(holderErrors, ", "))
	}
	http.Error(w, errorSummary, http.StatusNotFound)
}

// handleFilePutInternal handles internal peer-to-peer file PUT requests
func (c *Cluster) handleFilePutInternal(w http.ResponseWriter, r *http.Request, path string) {
	// Update current file for monitoring
	c.currentFile.Store(path)
	defer c.currentFile.Store("")

	c.debugf("[FILES] Internal PUT request for path: %s", path)

	content, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read request body for %s: %v", path, err), http.StatusBadRequest)
		return
	}

	if len(content) == 0 && r.Header.Get("Content-Type") == "" {
		c.debugf("[FILES] Ignoring empty internal upload for %s (likely directory)", path)
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

	var metadata types.FileMetadata

	if isForwarded {
		metaHeader := r.Header.Get("X-ClusterF-Metadata")
		if metaHeader == "" {
			http.Error(w, fmt.Sprintf("Missing forwarded metadata for internal file upload: %s", path), http.StatusBadRequest)
			return
		}
		decoded, err := base64.StdEncoding.DecodeString(metaHeader)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid forwarded metadata encoding for %s: %v", path, err), http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(decoded, &metadata); err != nil {
			http.Error(w, fmt.Sprintf("Invalid forwarded metadata payload for %s: %v", path, err), http.StatusBadRequest)
			return
		}

		if metadata.ModifiedAt.IsZero() {
			panic("no")
			http.Error(w, fmt.Sprintf("ModifiedAt timestamp is zero in forwarded metadata for %s", path), http.StatusBadRequest)
			return
		}
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if isForwarded {
		if metadata.ContentType != "" {
			contentType = metadata.ContentType
		}

		if _, err := c.FileSystem.StoreFileWithModTime(c.AppContext(), path, content, contentType, metadata.ModifiedAt); err != nil {
			http.Error(w, fmt.Sprintf("Failed to store internal file: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		modHeader := r.Header.Get("X-ClusterF-Modified-At")
		if modHeader == "" {
			http.Error(w, fmt.Sprintf("Missing X-ClusterF-Modified-At header for internal file upload: %s", path), http.StatusBadRequest)
			return
		}
		localModTime, err := parseHeaderTimestamp(modHeader)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid X-ClusterF-Modified-At header: %v", err), http.StatusBadRequest)
			return
		}
		if _, err := c.FileSystem.StoreFileWithModTime(c.AppContext(), path, content, contentType, localModTime); err != nil {
			http.Error(w, fmt.Sprintf("Failed to store internal file: %v", err), http.StatusInternalServerError)
			return
		}
	}

	c.debugf("[FILES] Stored internal %s (%d bytes)", path, len(content))
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"path":    path,
		"size":    len(content),
	})
}

// handleFileDeleteInternal handles internal peer-to-peer file DELETE requests
func (c *Cluster) handleFileDeleteInternal(w http.ResponseWriter, r *http.Request, path string) {
	c.debugf("[FILES] Internal DELETE request for path: %s", path)

	if err := c.FileSystem.DeleteFile(c.AppContext(), path); err != nil {
		if errors.Is(err, types.ErrFileNotFound) {
			http.Error(w, fmt.Sprintf("File not found for internal deletion: %s", path), http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to delete internal file: %v", err), http.StatusInternalServerError)
		return
	}

	c.debugf("[FILES] Deleted internal %s", path)
	w.WriteHeader(http.StatusNoContent)
}

// handleFilePostInternal handles internal peer-to-peer file POST requests
func (c *Cluster) handleFilePostInternal(w http.ResponseWriter, r *http.Request, path string) {
	c.debugf("[FILES] Internal POST request for path: %s", path)

	createDir := strings.EqualFold(r.Header.Get("X-Create-Directory"), "true")
	if !createDir {
		http.Error(w, fmt.Sprintf("Unsupported internal POST operation for %s (only directory creation supported via X-Create-Directory header)", path), http.StatusBadRequest)
		return
	}

	if err := c.FileSystem.CreateDirectory(path); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create internal directory: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"path":    path,
	})
}

// handleFileHeadInternal handles internal peer-to-peer file HEAD requests
func (c *Cluster) handleFileHeadInternal(w http.ResponseWriter, r *http.Request, path string) {
	c.debugf("[FILES] Internal HEAD request for path: %s", path)

	// Only check local storage, never forward to peers
	metadata, err := c.FileSystem.GetMetadata(path)
	if err != nil {
		switch {
		case errors.Is(err, types.ErrIsDirectory):
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-ClusterF-Is-Directory", "true")
			w.WriteHeader(http.StatusOK)
			return
		case errors.Is(err, types.ErrFileNotFound):
			c.debugf("[FILES] Internal HEAD metadata not found locally for %s", path)
			http.Error(w, fmt.Sprintf("File metadata not found: %s", path), http.StatusNotFound)
			return
		default:
			c.debugf("[FILES] Failed internal HEAD metadata %s: %v", path, err)
			http.Error(w, fmt.Sprintf("Failed to retrieve metadata for %s: %v", path, err), http.StatusInternalServerError)
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
	if metadata.ModifiedAt.IsZero() {
		panic("no")
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", metadata.Size))
	w.Header().Set("X-ClusterF-Created-At", metadata.CreatedAt.Format(time.RFC3339))
	w.Header().Set("X-ClusterF-Modified-At", metadata.ModifiedAt.Format(time.RFC3339))
	w.Header().Set("X-ClusterF-Is-Directory", "false")
	if metadata.Checksum != "" {
		w.Header().Set("X-ClusterF-Checksum", metadata.Checksum)
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", metadata.Checksum))
	} else {
		panic("no")
	}
	w.WriteHeader(http.StatusOK)
}

func (c *Cluster) handleFilePut(w http.ResponseWriter, r *http.Request, path string) {
	// Update current file for monitoring
	c.currentFile.Store(path)
	defer c.currentFile.Store("")

	// Debug: log all file uploads with no-store status
	if c.noStore {
		c.Logger().Printf("[FILES] NO-STORE node received PUT for %s (forwarded=%v)", path, r.Header.Get("X-Forwarded-From") != "")
	}

	content, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read request body for %s: %v", path, err), http.StatusBadRequest)
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

	var metadata types.FileMetadata

	if isForwarded {
		metaHeader := r.Header.Get("X-ClusterF-Metadata")
		if metaHeader == "" {
			http.Error(w, fmt.Sprintf("Missing forwarded metadata for file upload: %s", path), http.StatusBadRequest)
			return
		}
		decoded, err := base64.StdEncoding.DecodeString(metaHeader)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid forwarded metadata encoding for %s: %v", path, err), http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(decoded, &metadata); err != nil {
			http.Error(w, fmt.Sprintf("Invalid forwarded metadata payload for %s: %v", path, err), http.StatusBadRequest)
			return
		}

		if metadata.ModifiedAt.IsZero() {
			panic("no")
			http.Error(w, fmt.Sprintf("ModifiedAt timestamp is zero in forwarded metadata for %s", path), http.StatusBadRequest)
			return
		}
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if isForwarded {

		if metadata.ContentType != "" {
			contentType = metadata.ContentType
		}

		if _, err := c.FileSystem.StoreFileWithModTime(c.AppContext(), path, content, contentType, metadata.ModifiedAt); err != nil {
			http.Error(w, fmt.Sprintf("Failed to store file: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		modHeader := r.Header.Get("X-ClusterF-Modified-At")
		if modHeader == "" {
			http.Error(w, fmt.Sprintf("Missing X-ClusterF-Modified-At header for file upload: %s", path), http.StatusBadRequest)
			return
		}
		localModTime, err := parseHeaderTimestamp(modHeader)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid X-ClusterF-Modified-At header: %v", err), http.StatusBadRequest)
			return
		}
		if _, err := c.FileSystem.StoreFileWithModTime(c.AppContext(), path, content, contentType, localModTime); err != nil {
			http.Error(w, fmt.Sprintf("Failed to store file: %v", err), http.StatusInternalServerError)
			return
		}
	}

	c.debugf("[FILES] Stored %s (%d bytes)", path, len(content))
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"path":    path,
		"size":    len(content),
	})
}

func parseHeaderTimestamp(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("empty timestamp header")
	}
	t, err := time.Parse(time.RFC3339, value)
	if err == nil {
		return t, nil
	} else {
		return time.Time{}, err
	}
}

func (c *Cluster) handleFileDelete(w http.ResponseWriter, r *http.Request, path string) {
	if err := c.FileSystem.DeleteFile(c.AppContext(), path); err != nil {
		if errors.Is(err, types.ErrFileNotFound) {
			http.Error(w, fmt.Sprintf("File not found for deletion: %s", path), http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to delete file: %v", err), http.StatusInternalServerError)
		return
	}

	c.debugf("[FILES] Deleted %s", path)
	w.WriteHeader(http.StatusNoContent)
}

func (c *Cluster) handleFilePost(w http.ResponseWriter, r *http.Request, path string) {
	createDir := strings.EqualFold(r.Header.Get("X-Create-Directory"), "true")
	if !createDir {
		http.Error(w, fmt.Sprintf("Unsupported POST operation for %s (only directory creation supported via X-Create-Directory header)", path), http.StatusBadRequest)
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
