// search.go - Search API for directory browsing and file finding
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
)

// SearchMode defines the type of search
type SearchMode string

const (
	SearchModeDirectory SearchMode = "directory" // Prefix search with directory collapsing
	SearchModeFile      SearchMode = "file"      // Suffix search for files
)

// SearchRequest represents a search query
type SearchRequest struct {
	Query string `json:"query"`
	Limit int    `json:"limit,omitempty"`
}

// SearchResult represents a search result entry
type SearchResult struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	Size        int64  `json:"size,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	ModifiedAt  int64  `json:"modified_at,omitempty"`
}

// SearchResponse represents the search API response
type SearchResponse struct {
	Results []SearchResult `json:"results"`
	Count   int            `json:"count"`
}

// performLocalSearch performs a search on local files
func (c *Cluster) performLocalSearch(req SearchRequest) []SearchResult {
	var results []SearchResult
	seen := make(map[string]bool) // Prevent duplicates

	c.metadataKV.MapFunc(func(k, v []byte) error {
		key := string(k)

		// Only process file entries, but skip root directory metadata
		if !strings.Contains(key, ":file:") {
			return nil
		}

		// Extract file path from partition key
		parts := strings.Split(key, ":file:")
		if len(parts) != 2 {
			c.logger.Panicf("invalid metadata key format for %v", key)
		}
		filePath := parts[1]

		// Parse the metadata
		var metadata map[string]interface{}
		if err := json.Unmarshal(v, &metadata); err != nil {
			c.logger.Panicf("failed to unmarshal metadata for %v: %v", key, err)
		}

		// Skip deleted files
		if deleted, ok := metadata["deleted"].(bool); ok && deleted {
			return nil
		}

		c.logger.Printf("[SEARCH_DEBUG] Found local file: %s, %v", filePath, filepath.Base(filePath))
		if !seen[filePath] {
			seen[filePath] = true
			results = append(results, SearchResult{
				Name:        filepath.Base(filePath),
				Path:        filePath,
				Size:        c.GetMetadataSize(metadata),
				ContentType: c.GetMetadataContentType(metadata),
				ModifiedAt:  c.GetMetadataModifiedAt(metadata),
			})
		}
		// Apply limit if specified
		if req.Limit > 0 && len(results) >= req.Limit {
			return fmt.Errorf("limit reached") // Break the loop
		}

		return nil
	})

	c.logger.Printf("[SEARCH] Found %d local results for query: %s", len(results), req.Query)

	return results
}

// Helper functions to extract metadata fields
func (c *Cluster) GetMetadataSize(metadata map[string]interface{}) int64 {
	if size, ok := metadata["size"].(float64); ok {
		return int64(size)
	}
	return 0
}

func (c *Cluster) GetMetadataContentType(metadata map[string]interface{}) string {
	if contentType, ok := metadata["content_type"].(string); ok {
		return contentType
	}
	return ""
}

func (c *Cluster) GetMetadataModifiedAt(metadata map[string]interface{}) int64 {
	if modifiedAt, ok := metadata["modified_at"].(float64); ok {
		return int64(modifiedAt)
	}
	return 0
}

// searchAllPeers performs a search across all peers and combines results
func (c *Cluster) searchAllPeers(req SearchRequest) []SearchResult {
	var allResults []SearchResult
	seen := make(map[string]bool)

	// Search locally first
	localResults := c.performLocalSearch(req)
	for _, result := range localResults {
		if !seen[result.Path] {
			seen[result.Path] = true
			allResults = append(allResults, result)
		}
	}

	// Search all peers
	peers := c.DiscoveryManager().GetPeers()
	for _, peer := range peers {
		peerResults := c.searchPeer(peer, req)
		for _, result := range peerResults {
			if !seen[result.Path] {
				seen[result.Path] = true
				allResults = append(allResults, result)
			}
		}
	}

	// Sort results alphabetically
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Name < allResults[j].Name
	})

	// Apply limit if specified
	if req.Limit > 0 && len(allResults) > req.Limit {
		allResults = allResults[:req.Limit]
	}

	return allResults
}

// searchPeer performs a search on a specific peer
func (c *Cluster) searchPeer(peer *types.PeerInfo, req SearchRequest) []SearchResult {
	endpointURL, err := urlutil.BuildHTTPURL(peer.Address, peer.HTTPPort, "/api/search")
	if err != nil {
		c.debugf("Failed to build search URL for peer %s: %v", peer.NodeID, err)
		return nil
	}

	reqJSON, _ := json.Marshal(req)
	resp, err := c.httpClient.Post(endpointURL, "application/json", strings.NewReader(string(reqJSON)))
	if err != nil {
		c.debugf("Failed to search peer %s: %v", peer.NodeID, err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.debugf("Peer %s search returned %d", peer.NodeID, resp.StatusCode)
		return nil
	}

	var response SearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		c.debugf("Failed to decode search response from %s: %v", peer.NodeID, err)
		return nil
	}

	return response.Results
}

// handleSearchAPI handles the search API endpoint
func (c *Cluster) handleSearchAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	if req.Query == "" {
		http.Error(w, "query cannot be empty", http.StatusBadRequest)
		return
	}

	// Perform local search only (peers will call this endpoint)
	results := c.performLocalSearch(req)

	response := SearchResponse{
		Results: results,
		Count:   len(results),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// ListDirectoryUsingSearch implements directory listing using the search API
func (c *Cluster) ListDirectoryUsingSearch(path string) ([]*types.FileMetadata, error) {
	// Normalize path to ensure it ends with / for prefix search
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	c.debugf("[SEARCH] ListDirectory for path: %s", path)

	// Create search request for directory mode
	req := SearchRequest{
		Query: path,
		Limit: 1000, // Reasonable limit for directory listings
	}

	// Search all peers
	raw_results := c.searchAllPeers(req)
	c.debugf("[SEARCH] Found %d results for %s", len(raw_results), path)

	// Create directories by collapsing paths
	c.debugf("[SEARCH] Found %d file results for %s", len(raw_results), path)
	results := make([]SearchResult, 0, len(raw_results))
	seen := make(map[string]bool)
	for _, res := range raw_results {
		// Clip off the prefix path
		relPath := strings.TrimPrefix(res.Path, path)
		//Take the string up to the first /
		parts := strings.SplitN(relPath, "/", 2)
		c.debugf("[SEARCH] Path split into parts: %v", parts)
		if len(parts) > 1 {
			if parts[0] != "" {
				dir := parts[0] + "/"
				c.debugf("[SEARCH] Processing result: %s (rel: %s) is a directory", dir, relPath)
				if !seen[dir] {
					seen[dir] = true
					results = append(results, SearchResult{
						Name: dir,
						Path: dir,
					})
				}
			}
		} else {
			// It's a file in the current directory
			c.debugf("[SEARCH] Processing result: %s (rel: %s) is a file", relPath, relPath)
			if !seen[relPath] {
				seen[relPath] = true //Could get multiple files with same name from different peers
				results = append(results, SearchResult{
					Name:        relPath,
					Path:        relPath,
					Size:        res.Size,
					ContentType: res.ContentType,
					ModifiedAt:  res.ModifiedAt,
				})
			}
		}
	}

	// Convert to FileMetadata format
	var fileMetadata []*types.FileMetadata
	for _, result := range results {
		metadata := &types.FileMetadata{
			Name:        strings.TrimSuffix(result.Name, "/"),
			Path:        result.Path,
			Size:        result.Size,
			ContentType: result.ContentType,
			IsDirectory: strings.HasSuffix(result.Name, "/"),
		}

		if result.ModifiedAt > 0 {
			metadata.ModifiedAt = time.Unix(result.ModifiedAt, 0)
		}

		fileMetadata = append(fileMetadata, metadata)
	}

	return fileMetadata, nil
}

// SearchFiles implements file search using the search API
func (c *Cluster) SearchFiles(filename string) ([]*types.FileMetadata, error) {
	req := SearchRequest{
		Query: filename,
		Limit: 100, // Reasonable limit for file search
	}

	// Search all peers
	raw_results := c.searchAllPeers(req)

	results := make([]SearchResult, 0, len(raw_results))
	seen := make(map[string]bool)
	for _, res := range raw_results {
		dir := filepath.Dir(res.Path)
		if !seen[dir] {
			seen[dir] = true
			results = append(results, SearchResult{
				Name: filepath.Base(dir) + "/",
				Path: dir,
			})
		}
	}

	// Convert to FileMetadata format
	var fileMetadata []*types.FileMetadata
	for _, result := range results {

		metadata := &types.FileMetadata{
			Name:        result.Name,
			Path:        result.Path,
			Size:        result.Size,
			ContentType: result.ContentType,
			IsDirectory: false,
		}

		if result.ModifiedAt > 0 {
			metadata.ModifiedAt = time.Unix(result.ModifiedAt, 0)
		}

		fileMetadata = append(fileMetadata, metadata)
	}

	return fileMetadata, nil
}
