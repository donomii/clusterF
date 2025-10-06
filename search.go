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

// SearchResponse represents the search API response
type SearchResponse struct {
	Results []types.SearchResult `json:"results"`
	Count   int                  `json:"count"`
}

// performLocalSearch performs a search on local files
func (c *Cluster) performLocalSearch(req SearchRequest) []types.SearchResult {
	var results []types.SearchResult
	result := make(map[string]types.SearchResult)

	// Scan all local partition stores for files matching the query
	c.partitionManager.ScanAllFiles(func(filePath string, metadata map[string]interface{}) error {
		// Skip deleted files
		if deleted, ok := metadata["deleted"].(bool); ok && deleted {
			return nil
		}

		if !strings.Contains(filePath, req.Query) {
			return nil
		}

		if !strings.HasPrefix(filePath, req.Query) {
			return nil
		}

		// Add to results if not already seen
		// Collapse to directory if needed

		res := types.SearchResult{
			Name:        filepath.Base(filePath),
			Path:        filePath,
			Size:        c.GetMetadataSize(metadata),
			ContentType: c.GetMetadataContentType(metadata),
			ModifiedAt:  c.GetMetadataModifiedAt(metadata),
			Checksum:    c.GetMetadataChecksum(metadata),
		}

		types.AddResultToMap(res, result, req.Query)

		return nil
	})

	c.debugf("[SEARCH] Found %d local results for query: %s", len(results), req.Query)

	// Convert map to slice
	for _, res := range result {
		results = append(results, res)
	}
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

func (c *Cluster) GetMetadataChecksum(metadata map[string]interface{}) string {
	checksum, ok := metadata["checksum"].(string)
	if !ok {
		c.logger.Panicf("missing checksum in metadata: %v", metadata)
	}
	return checksum
}

// searchAllNodes performs a search across all peers and combines results
func (c *Cluster) searchAllNodes(req SearchRequest) []types.SearchResult {
	var allResults []types.SearchResult
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
		fmt.Printf("[SEARCH] Searching peer %s (%s)\n", peer.NodeID, peer.Address)
		peerResults := c.searchPeer(peer, req)
		for _, result := range peerResults {
			if !seen[result.Path] {
				seen[result.Path] = true
				allResults = append(allResults, result)
			}
		}
	}

	searchMap := make(map[string]types.SearchResult)

	for _, res := range allResults {
		types.AddResultToMap(res, searchMap, req.Query)
	}

	allResults = make([]types.SearchResult, 0, len(searchMap))
	for _, res := range searchMap {
		allResults = append(allResults, res)
	}

	// Sort results alphabetically
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].Name < allResults[j].Name
	})

	// Apply limit if specified
	if req.Limit > 0 && len(allResults) > req.Limit {
		allResults = allResults[:req.Limit]
	}
	for _, r := range allResults {
		c.logger.Printf("Result: %v", r.Name)
		c.logger.Printf("ResultPath: %v", r.Path)
	}

	return allResults
}

// searchPeer performs a search on a specific peer
func (c *Cluster) searchPeer(peer *types.PeerInfo, req SearchRequest) []types.SearchResult {
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
	fmt.Printf("[SEARCH] Listing directory using search for path: %s\n", path)
	// Normalize path to ensure it ends with / for prefix search
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("path must start with /")
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	c.debugf("[SEARCH] ListDirectory for path: %s", path)

	// Create search request for directory mode
	req := SearchRequest{
		Query: path,
		Limit: 1000000, // Reasonable limit for directory listings
	}

	// Search all peers
	raw_results := c.searchAllNodes(req)
	c.debugf("[SEARCH] Found %d results for %s", len(raw_results), path)

	// Create directories by collapsing paths
	c.debugf("[SEARCH] Found %d file results for %s", len(raw_results), path)
	results := types.CollapseSearchResults(raw_results, path)

	// Convert to FileMetadata format
	var fileMetadata []*types.FileMetadata
	for _, result := range results {
		metadata := &types.FileMetadata{
			Name:        strings.TrimSuffix(result.Name, "/"),
			Path:        result.Path,
			Size:        result.Size,
			ContentType: result.ContentType,
			IsDirectory: strings.HasSuffix(result.Name, "/"),
			Checksum:    result.Checksum,
		}

		if result.ModifiedAt > 0 {
			metadata.ModifiedAt = time.Unix(result.ModifiedAt, 0)
		}

		fileMetadata = append(fileMetadata, metadata)
	}

	return fileMetadata, nil
}
