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

	"github.com/donomii/frogpond"
)

// handleCRDTListAPI lists immediate children (dirs and keys) under a prefix, with pagination.
func (c *Cluster) handleCRDTListAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, fmt.Sprintf("Method %s not allowed for CRDT list API (only GET supported)", r.Method), http.StatusMethodNotAllowed)
		return
	}
	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))

	dps := c.frogpond.GetAllMatchingPrefix(prefix)

	// Filter out deleted keys
	var activeDPs []frogpond.DataPoint
	for _, dp := range dps {
		if !dp.Deleted && len(dp.Value) > 0 {
			activeDPs = append(activeDPs, dp)
		}
	}

	log.Printf("CRDT ListAPI: found %d entries under prefix %q", len(activeDPs), prefix)
	prefix_len := len(prefix)

	ke := make(map[string]bool)
	if len(activeDPs) > 100 {

		for _, dp := range activeDPs {
			k := string(dp.Key)
			var newk string
			if len(k) > prefix_len {
				newk = k[:prefix_len+1]
			} else {
				newk = k
			}
			ke[newk] = true
		}
	} else {
		for _, dp := range activeDPs {
			k := string(dp.Key)
			ke[k] = true

		}
	}

	log.Printf("CRDT keylist: %+v", ke)

	// Find common prefix in ke
	var commonPrefix string
	for k := range ke {
		if commonPrefix == "" {
			commonPrefix = k
		} else {
			commonPrefix = longestCommonPrefix(commonPrefix, k)
		}
	}
	log.Printf("CRDT common prefix: %q", commonPrefix)
	prefix = commonPrefix

	out := make([]map[string]string, 0, 100)
	for k := range ke {
		name := strings.TrimPrefix(k, prefix)
		dps := c.frogpond.GetAllMatchingPrefix(k)
		// Count only non-deleted keys
		count := 0
		for _, dp := range dps {
			if !dp.Deleted && len(dp.Value) > 0 {
				count++
			}
		}
		out = append(out, map[string]string{"name": name, "key": k, "count": strconv.Itoa(count)})
	}

	resp := map[string]interface{}{
		"prefix": prefix,
		"items":  out,
	}
	log.Printf("CRDT ListAPI response: %+v", resp)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
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
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing required 'key' parameter in CRDT get request", http.StatusBadRequest)
		return
	}
	
	dp := c.frogpond.GetDataPoint(key)
	
	val := dp.Value
	var parsed interface{}
	_ = json.Unmarshal(val, &parsed)
	var textStr string
	if utf8.Valid(val) {
		textStr = string(val)
	}
	resp := map[string]interface{}{
		"key":          key,
		"deleted":      dp.Deleted,
		"size":         len(val),
		"value_base64": base64.StdEncoding.EncodeToString(val),
	}
	if parsed != nil {
		resp["value_json"] = parsed
	}
	if textStr != "" {
		resp["value_text"] = textStr
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
	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "Missing required 'q' (query) parameter in CRDT search request", http.StatusBadRequest)
		return
	}
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 2000 {
			limit = v
		}
	}
	after := r.URL.Query().Get("after")
	dps := c.frogpond.GetAllMatchingPrefix("")
	keys := make([]string, 0, len(dps))
	for _, dp := range dps {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}
		k := string(dp.Key)
		if strings.Contains(k, q) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	start := 0
	if after != "" {
		for i := range keys {
			if keys[i] > after {
				start = i
				break
			}
		}
	}
	end := start + limit
	if end > len(keys) {
		end = len(keys)
	}
	items := make([]map[string]string, 0, end-start)
	for _, k := range keys[start:end] {
		name := k
		items = append(items, map[string]string{
			"type": "key",
			"name": name,
			"key":  k,
		})
	}
	resp := map[string]interface{}{
		"prefix":   prefix,
		"q":        q,
		"items":    items,
		"has_more": end < len(keys),
	}
	if end < len(keys) {
		resp["next_after"] = keys[end-1]
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
