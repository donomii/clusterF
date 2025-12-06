// search.go - Search API for directory browsing and file finding
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
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

// performLocalSearch performs a search on local files using the indexer
func (c *Cluster) performLocalSearch(req SearchRequest) []types.SearchResult {
	start := time.Now()

	// Use indexer for fast prefix search
	results := c.indexer.PrefixSearch(req.Query)

	c.debugf("[SEARCH] Found %d local results for query: %s in %v seconds", len(results), req.Query, time.Now().Sub(start).Seconds())

	return results
}

// searchAllNodes performs a search across all peers and combines results
func (c *Cluster) searchAllNodes(req SearchRequest) []types.SearchResult {
	allResults := make([]types.SearchResult, 0)

	// Search locally first
	localResults := c.performLocalSearch(req)
	allResults = append(allResults, localResults...)

	wg := &sync.WaitGroup{}
	peers := c.DiscoveryManager().GetPeers()
	resultsCh := make(chan []types.SearchResult, len(peers))

	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.debugf("[SEARCH] Searching peer %s (%s)\n", peer.NodeID, peer.Address)
			peerResults := c.searchPeer(peer, req)
			c.debugf("Received results from peer: %+v", peerResults)
			if len(peerResults) > 0 {
				for i := range peerResults {
					peerResults[i].Holders = append(peerResults[i].Holders, peer.NodeID)
				}
				resultsCh <- peerResults
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	for peerResults := range resultsCh {
		allResults = append(allResults, peerResults...)
	}

	searchMap := make(map[string]types.SearchResult)

	for _, res := range allResults {
		types.AddResultToMap(res, searchMap, res.Name, req.Query)
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

	c.debugf("Compiled allResults: %+v", allResults)

	return allResults
}

// searchPeer performs a search on a specific peer
func (c *Cluster) searchPeer(peer *types.PeerInfo, req SearchRequest) []types.SearchResult {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		c.debugf("[PEER_QUERY_TIMING] Query to peer %s took %v", peer.NodeID, duration)
	}()

	endpointURL, err := urlutil.BuildInternalSearchURL(peer.Address, peer.HTTPPort)
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
		body, _ := types.ReadAll(resp.Body)
		c.debugf("Peer %s search returned %d: %s", peer.NodeID, resp.StatusCode, string(body))
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
	defer func() {
		if rec := recover(); rec != nil {
			c.debugf("Search API panic: %v", rec)
			http.Error(w, fmt.Sprintf("Internal server error: %v", rec), http.StatusInternalServerError)
		}
	}()

	if r.Method != http.MethodPost {
		http.Error(w, fmt.Sprintf("Method %s not allowed for search API (only POST supported)", r.Method), http.StatusMethodNotAllowed)
		return
	}

	var req SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	if req.Query == "" {
		http.Error(w, "Search query cannot be empty - please provide a search term", http.StatusBadRequest)
		return
	}

	// Perform local search only (peers will call this endpoint)
	results := c.performLocalSearch(req)

	response := SearchResponse{
		Results: results,
		Count:   len(results),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		c.debugf("Failed to encode search response: %v", err)
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

// handleInternalSearchAPI handles internal peer-to-peer search requests
// Only performs local search, never forwards to other peers
func (c *Cluster) handleInternalSearchAPI(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if rec := recover(); rec != nil {
			c.debugf("Internal search API panic: %v", rec)
			http.Error(w, fmt.Sprintf("Internal server error: %v", rec), http.StatusInternalServerError)
		}
	}()

	if r.Method != http.MethodPost {
		http.Error(w, fmt.Sprintf("Method %s not allowed for internal search API (only POST supported)", r.Method), http.StatusMethodNotAllowed)
		return
	}

	c.debugf("[INTERNAL_SEARCH_API] POST request from peer")

	var req SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	if req.Query == "" {
		http.Error(w, "Search query cannot be empty - please provide a search term", http.StatusBadRequest)
		return
	}

	c.debugf("[INTERNAL_SEARCH_API] Performing local search for query: %s", req.Query)

	// Only perform local search, never forward to peers
	results := c.performLocalSearch(req)

	response := SearchResponse{
		Results: results,
		Count:   len(results),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		c.debugf("Failed to encode internal search response: %v", err)
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

// ListDirectoryUsingSearch implements directory listing using the search API
func (c *Cluster) ListDirectoryUsingSearch(path string) ([]*types.FileMetadata, error) {
	c.debugf("[SEARCH] Listing directory using search for path: %s\n", path)
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
		if result.ModifiedAt.IsZero() {
			panic("no")
		}
		metadata := &types.FileMetadata{
			Name:        strings.TrimSuffix(result.Name, "/"),
			Path:        result.Path,
			Size:        result.Size,
			ContentType: result.ContentType,
			IsDirectory: strings.HasSuffix(result.Name, "/"),
			Checksum:    result.Checksum,
			ModifiedAt:  result.ModifiedAt,
			CreatedAt:   result.CreatedAt,
			Holders:     result.Holders,
		}
		if !metadata.IsDirectory {
			hasLocal := false
			for _, holder := range metadata.Holders {
				if holder == c.ID() {
					hasLocal = true
					break
				}
			}
			if !hasLocal {
				metadata.Holders = append(metadata.Holders, c.ID())
			}
		}

		fileMetadata = append(fileMetadata, metadata)
	}

	return fileMetadata, nil
}
