package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"

	"github.com/donomii/clusterF/types"
)

// handleProfilingAPI handles profiling control API.
func (c *Cluster) handleProfilingAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		c.profilingMutex.Lock()
		active := c.profilingActive
		c.profilingMutex.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{
			"active": active,
		})

	case http.MethodPost:
		body, err := types.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}

		var request map[string]interface{}
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		action, ok := request["action"].(string)
		if !ok {
			http.Error(w, "Missing action field", http.StatusBadRequest)
			return
		}

		c.profilingMutex.Lock()
		switch action {
		case "start":
			if !c.profilingActive {
				if err := c.startProfiling(); err != nil {
					c.profilingMutex.Unlock()
					http.Error(w, fmt.Sprintf("Failed to start profiling: %v", err), http.StatusInternalServerError)
					return
				}
				c.profilingActive = true
				c.Logger().Printf("[PROFILING] Started")
			}
		case "stop":
			if c.profilingActive {
				c.stopProfiling()
				c.profilingActive = false
				c.Logger().Printf("[PROFILING] Stopped")
			}
		default:
			c.profilingMutex.Unlock()
			http.Error(w, "Invalid action", http.StatusBadRequest)
			return
		}
		c.profilingMutex.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"action":  action,
		})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// startProfiling enables Go's built-in profiling.
func (c *Cluster) startProfiling() error {
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
	runtime.MemProfileRate = 512 * 1024 // Set memory profiling rate to every 512KB
	c.Logger().Printf("[PROFILING] Enabled block, mutex, and memory profiling")
	return nil
}

// stopProfiling disables Go's built-in profiling.
func (c *Cluster) stopProfiling() {
	runtime.SetBlockProfileRate(0)
	runtime.SetMutexProfileFraction(0)
	runtime.MemProfileRate = 0 // Disable memory profiling
	c.Logger().Printf("[PROFILING] Disabled block, mutex, and memory profiling")
}

// enableProfiling enables profiling with proper mutex handling
func (c *Cluster) enableProfiling() error {
	c.profilingMutex.Lock()
	defer c.profilingMutex.Unlock()

	if !c.profilingActive {
		if err := c.startProfiling(); err != nil {
			return err
		}
		c.profilingActive = true
	}
	return nil
}
