package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/donomii/clusterF/types"
)

const (
	crdtDefaultPageLimit = 100
	crdtMaxPageLimit     = 2000
)

type crdtListItem struct {
	Name  string `json:"name"`
	Key   string `json:"key"`
	Type  string `json:"type"`
	Count int    `json:"count,omitempty"`
}

type crdtSearchItem struct {
	Name string `json:"name"`
	Key  string `json:"key"`
	Type string `json:"type"`
}

type crdtListResponse struct {
	Prefix    string         `json:"prefix"`
	Items     []crdtListItem `json:"items"`
	HasMore   bool           `json:"has_more"`
	NextAfter string         `json:"next_after,omitempty"`
}

type crdtSearchResponse struct {
	Prefix    string           `json:"prefix"`
	Query     string           `json:"q"`
	Items     []crdtSearchItem `json:"items"`
	HasMore   bool             `json:"has_more"`
	NextAfter string           `json:"next_after,omitempty"`
}

type crdtChildAggregate struct {
	HasChildren bool
	HasLeaf     bool
	Count       int
}

// handleCRDTListAPI lists immediate children (dirs and keys) under a prefix, with pagination.
func (c *Cluster) handleCRDTListAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, fmt.Sprintf("Method %s not allowed for CRDT list API (only GET supported)", r.Method), http.StatusMethodNotAllowed)
		return
	}

	prefix := normalizeCRDTPrefix(r.URL.Query().Get("prefix"))
	after := strings.TrimSpace(r.URL.Query().Get("after"))
	limit := parseCRDTLimit(r.URL.Query().Get("limit"))

	types.Assertf(c.frogpond != nil, "frogpond must not be nil when listing CRDT prefix %s", prefix)

	dataPoints := c.frogpond.GetAllMatchingPrefix(prefix)

	childAggregates := make(map[string]crdtChildAggregate)

	for _, dp := range dataPoints {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}

		keyStr := string(dp.Key)
		remaining := strings.TrimPrefix(keyStr, prefix)
		remaining = strings.TrimPrefix(remaining, "/")
		if remaining == "" {
			continue
		}

		segment := remaining
		remainder := ""
		if idx := strings.Index(segment, "/"); idx >= 0 {
			remainder = segment[idx+1:]
			segment = segment[:idx]
		}
		if segment == "" {
			continue
		}

		agg := childAggregates[segment]
		agg.Count++
		if remainder == "" {
			agg.HasLeaf = true
		} else {
			agg.HasChildren = true
		}
		childAggregates[segment] = agg
	}

	childNames := make([]string, 0, len(childAggregates))
	for name := range childAggregates {
		childNames = append(childNames, name)
	}
	sort.Strings(childNames)

	startIndex := 0
	if after != "" {
		startIndex = sort.Search(len(childNames), func(i int) bool {
			return childNames[i] > after
		})
	}

	endIndex := startIndex + limit
	if endIndex > len(childNames) {
		endIndex = len(childNames)
	}

	items := make([]crdtListItem, 0, endIndex-startIndex)
	for _, name := range childNames[startIndex:endIndex] {
		aggregate := childAggregates[name]
		itemType := "key"
		if aggregate.HasChildren {
			itemType = "dir"
		}
		itemKey := joinCRDTPath(prefix, name)
		items = append(items, crdtListItem{
			Name:  name,
			Key:   itemKey,
			Type:  itemType,
			Count: aggregate.Count,
		})
	}

	response := crdtListResponse{
		Prefix:  prefix,
		Items:   items,
		HasMore: endIndex < len(childNames),
	}
	if response.HasMore {
		response.NextAfter = childNames[endIndex-1]
	}

	log.Printf("CRDT ListAPI: prefix=%q items=%d has_more=%v", prefix, len(items), response.HasMore)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func longestCommonPrefix(a, b string) string {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	i := 0
	for i < minLen && a[i] == b[i] {
		i++
	}
	return a[:i]
}

// handleCRDTGetAPI returns the value for a specific CRDT key in multiple representations.
func (c *Cluster) handleCRDTGetAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, fmt.Sprintf("Method %s not allowed for CRDT get API (only GET supported)", r.Method), http.StatusMethodNotAllowed)
		return
	}
	key := normalizeCRDTPrefix(r.URL.Query().Get("key"))
	if key == "" {
		http.Error(w, "Missing required 'key' parameter in CRDT get request", http.StatusBadRequest)
		return
	}

	types.Assertf(c.frogpond != nil, "frogpond must not be nil when fetching CRDT key %s", key)

	dp := c.frogpond.GetDataPoint(key)
	val := dp.Value

	resp := map[string]interface{}{
		"key":     key,
		"deleted": dp.Deleted,
		"size":    len(val),
	}

	if len(val) > 0 && !dp.Deleted {
		var parsed interface{}
		_ = json.Unmarshal(val, &parsed)
		var textStr string
		if utf8.Valid(val) {
			textStr = string(val)
		}
		resp["value_base64"] = base64.StdEncoding.EncodeToString(val)
		if parsed != nil {
			resp["value_json"] = parsed
		}
		if textStr != "" {
			resp["value_text"] = textStr
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// countCRDTKeys counts non-deleted keys under a given prefix in the CRDT store.
func (c *Cluster) countCRDTKeys(prefix string) int {
	dps := c.frogpond.GetAllMatchingPrefix(prefix)
	n := 0
	for _, dp := range dps {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}
		n++
	}
	return n
}

// handleCRDTSearchAPI searches for keys containing a substring within a prefix.
func (c *Cluster) handleCRDTSearchAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, fmt.Sprintf("Method %s not allowed for CRDT search API (only GET supported)", r.Method), http.StatusMethodNotAllowed)
		return
	}
	prefix := normalizeCRDTPrefix(r.URL.Query().Get("prefix"))
	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "Missing required 'q' (query) parameter in CRDT search request", http.StatusBadRequest)
		return
	}
	limit := parseCRDTLimit(r.URL.Query().Get("limit"))
	after := strings.TrimSpace(r.URL.Query().Get("after"))

	types.Assertf(c.frogpond != nil, "frogpond must not be nil when searching CRDT prefix %s for query %s", prefix, q)

	dps := c.frogpond.GetAllMatchingPrefix(prefix)
	keys := make([]string, 0, len(dps))
	for _, dp := range dps {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}
		k := string(dp.Key)
		if strings.Contains(k, q) && strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	start := 0
	if after != "" {
		start = sort.Search(len(keys), func(i int) bool {
			return keys[i] > after
		})
	}
	end := start + limit
	if end > len(keys) {
		end = len(keys)
	}

	items := make([]crdtSearchItem, 0, end-start)
	for _, k := range keys[start:end] {
		name := strings.TrimPrefix(k, prefix)
		name = strings.TrimPrefix(name, "/")
		items = append(items, crdtSearchItem{
			Type: "key",
			Name: name,
			Key:  k,
		})
	}

	response := crdtSearchResponse{
		Prefix:  prefix,
		Query:   q,
		Items:   items,
		HasMore: end < len(keys),
	}
	if response.HasMore {
		response.NextAfter = keys[end-1]
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func normalizeCRDTPrefix(raw string) string {
	trimmed := strings.TrimSpace(raw)
	for strings.HasPrefix(trimmed, "/") {
		trimmed = strings.TrimPrefix(trimmed, "/")
	}
	return trimmed
}

func parseCRDTLimit(raw string) int {
	if raw == "" {
		return crdtDefaultPageLimit
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return crdtDefaultPageLimit
	}
	if value < 1 {
		return crdtDefaultPageLimit
	}
	if value > crdtMaxPageLimit {
		return crdtMaxPageLimit
	}
	return value
}

func joinCRDTPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	if name == "" {
		return prefix
	}
	return prefix + "/" + name
}
