package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"runtime/pprof"
	"time"
)

// handleFlameGraph generates CPU flame graph SVG directly
func (c *Cluster) handleFlameGraph(w http.ResponseWriter, r *http.Request) {
	// Collect CPU profile for 30 seconds
	var profileBuf bytes.Buffer
	if err := pprof.StartCPUProfile(&profileBuf); err != nil {
		http.Error(w, "Failed to start CPU profile: "+err.Error(), http.StatusInternalServerError)
		return
	}

	time.Sleep(30 * time.Second)
	pprof.StopCPUProfile()

	// Write profile data to temp file
	tmpFile, err := ioutil.TempFile("", "cpu_profile_*.pb.gz")
	if err != nil {
		http.Error(w, "Failed to create temp file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(profileBuf.Bytes()); err != nil {
		http.Error(w, "Failed to write profile data: "+err.Error(), http.StatusInternalServerError)
		return
	}
	tmpFile.Close()

	// Generate SVG using go tool pprof
	cmd := exec.Command("go", "tool", "pprof", "-svg", tmpFile.Name())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	svgOutput, err := cmd.Output()
	if err != nil {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "Failed to generate SVG: %v\nstderr: %s", err, stderr.String())
		return
	}

	// Display SVG
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Write(svgOutput)
}
