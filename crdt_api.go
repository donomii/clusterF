package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// handleCRDTListAPI lists immediate children (dirs and keys) under a prefix, with pagination.
func (c *Cluster) handleCRDTListAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	if prefix != "" && strings.HasSuffix(prefix, "/") {
		prefix = strings.TrimSuffix(prefix, "/")
	}
	after := r.URL.Query().Get("after")
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 2000 {
			limit = v
		}
	}

	if prefix == "" {
		items := []map[string]string{
			{"type": "dir", "name": "cluster"},
			{"type": "dir", "name": "nodes"},
			{"type": "dir", "name": "partitions"},
		}
		start := 0
		if after != "" {
			for i, it := range items {
				if it["name"] > after {
					start = i
					break
				}
			}
		}
		end := start + limit
		if end > len(items) {
			end = len(items)
		}
		slice := items[start:end]
		resp := map[string]interface{}{
			"prefix":   prefix,
			"items":    slice,
			"has_more": end < len(items),
		}
		if end < len(items) {
			resp["next_after"] = items[end-1]["name"]
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	}

	if prefix == "partitions" || strings.HasPrefix(prefix, "partitions/") {
		rem := strings.TrimPrefix(prefix, "partitions")
		rem = strings.TrimPrefix(rem, "/")

		paginate := func(names []string) (page []string, hasMore bool, next string) {
			sort.Strings(names)
			start := 0
			if after != "" {
				for i := range names {
					if names[i] > after {
						start = i
						break
					}
				}
			}
			end := start + limit
			if end > len(names) {
				end = len(names)
			}
			page = names[start:end]
			if end < len(names) {
				hasMore = true
				next = names[end-1]
			}
			return
		}

		if rem == "" || len(rem) < 3 {
			names := make([]string, 0, 66)
			for i := 0; i <= 65; i++ {
				names = append(names, fmt.Sprintf("p%02d", i))
			}
			page, hasMore, next := paginate(names)
			items := make([]map[string]interface{}, 0, len(page))
			for _, g := range page {
				cnt := c.countCRDTKeys("partitions/" + g)
				items = append(items, map[string]interface{}{"name": g, "type": "dir", "count": cnt})
			}
			resp := map[string]interface{}{"prefix": prefix, "items": items, "has_more": hasMore}
			if hasMore {
				resp["next_after"] = next
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}
		if len(rem) == 3 {
			base := rem
			if len(base) != 3 {
				http.Error(w, "bad prefix", http.StatusBadRequest)
				return
			}
			names := make([]string, 0, 10)
			for i := 0; i <= 9; i++ {
				names = append(names, fmt.Sprintf("%s%d", base, i))
			}
			page, hasMore, next := paginate(names)
			items := make([]map[string]interface{}, 0, len(page))
			for _, g := range page {
				cnt := c.countCRDTKeys("partitions/" + g)
				items = append(items, map[string]interface{}{"name": g, "type": "dir", "count": cnt})
			}
			resp := map[string]interface{}{"prefix": prefix, "items": items, "has_more": hasMore}
			if hasMore {
				resp["next_after"] = next
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}
		if len(rem) == 4 {
			base := rem
			names := make([]string, 0, 10)
			for i := 0; i <= 9; i++ {
				names = append(names, fmt.Sprintf("%s%d", base, i))
			}
			page, hasMore, next := paginate(names)
			items := make([]map[string]interface{}, 0, len(page))
			for _, g := range page {
				cnt := c.countCRDTKeys("partitions/" + g)
				items = append(items, map[string]interface{}{"name": g, "type": "dir", "count": cnt})
			}
			resp := map[string]interface{}{"prefix": prefix, "items": items, "has_more": hasMore}
			if hasMore {
				resp["next_after"] = next
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(resp)
			return
		}
		realPrefix := "partitions/" + rem
		dps := c.frogpond.GetAllMatchingPrefix(realPrefix)
		keySet := make(map[string]struct{})
		for _, dp := range dps {
			k := string(dp.Key)
			rest := strings.TrimPrefix(k, "partitions/")
			if !strings.HasPrefix(rest, rem) {
				continue
			}
			if strings.Contains(rest[len(rem):], "/") {
				continue
			}
			keySet[rest] = struct{}{}
		}
		names := make([]string, 0, len(keySet))
		for n := range keySet {
			names = append(names, n)
		}
		sort.Strings(names)
		start := 0
		if after != "" {
			for i := range names {
				if names[i] > after {
					start = i
					break
				}
			}
		}
		end := start + limit
		if end > len(names) {
			end = len(names)
		}
		out := make([]map[string]string, 0, end-start)
		for _, n := range names[start:end] {
			out = append(out, map[string]string{"name": n, "type": "key"})
		}
		resp := map[string]interface{}{"prefix": prefix, "items": out, "has_more": end < len(names)}
		if end < len(names) {
			resp["next_after"] = names[end-1]
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
		return
	}

	dps := c.frogpond.GetAllMatchingPrefix(prefix)

	dirSet := make(map[string]struct{})
	keySet := make(map[string]struct{})
	for _, dp := range dps {
		key := string(dp.Key)
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		rest := strings.TrimPrefix(key, prefix)
		if strings.HasPrefix(rest, "/") {
			rest = rest[1:]
		}
		if rest == "" {
			continue
		}
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			seg := rest[:i]
			if seg != "" {
				dirSet[seg] = struct{}{}
			}
		} else {
			keySet[rest] = struct{}{}
		}
	}
	items := make([]struct{ Name, Type string }, 0, len(dirSet)+len(keySet))
	for name := range dirSet {
		items = append(items, struct{ Name, Type string }{name, "dir"})
	}
	for name := range keySet {
		items = append(items, struct{ Name, Type string }{name, "key"})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Name == items[j].Name {
			return items[i].Type < items[j].Type
		}
		return items[i].Name < items[j].Name
	})
	start := 0
	if after != "" {
		for i := range items {
			if items[i].Name > after {
				start = i
				break
			}
		}
	}
	end := start + limit
	if end > len(items) {
		end = len(items)
	}
	page := items[start:end]
	out := make([]map[string]string, 0, len(page))
	for _, it := range page {
		out = append(out, map[string]string{"name": it.Name, "type": it.Type})
	}

	resp := map[string]interface{}{
		"prefix":   prefix,
		"items":    out,
		"has_more": end < len(items),
	}
	if end < len(items) {
		resp["next_after"] = items[end-1].Name
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleCRDTGetAPI returns the value for a specific CRDT key in multiple representations.
func (c *Cluster) handleCRDTGetAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	if prefix != "" && strings.HasSuffix(prefix, "/") {
		prefix = strings.TrimSuffix(prefix, "/")
	}
	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, "missing q", http.StatusBadRequest)
		return
	}
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 2000 {
			limit = v
		}
	}
	after := r.URL.Query().Get("after")

	trimPrefix := prefix
	if trimPrefix != "" && !strings.HasSuffix(trimPrefix, "/") {
		trimPrefix += "/"
	}

	dps := c.frogpond.GetAllMatchingPrefix(prefix)
	keys := make([]string, 0, len(dps))
	for _, dp := range dps {
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
		if trimPrefix != "" && strings.HasPrefix(k, trimPrefix) {
			name = strings.TrimPrefix(k, trimPrefix)
		}
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
