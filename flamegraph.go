package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"runtime"
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

// handleMemoryFlameGraph generates memory flame graph SVG directly
func (c *Cluster) handleMemoryFlameGraph(w http.ResponseWriter, r *http.Request) {
	// Force GC to get accurate heap data
	runtime.GC()
	runtime.GC() // Run twice to ensure clean collection

	// Collect heap profile
	var profileBuf bytes.Buffer
	if err := pprof.WriteHeapProfile(&profileBuf); err != nil {
		http.Error(w, "Failed to write heap profile: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Write profile data to temp file
	tmpFile, err := ioutil.TempFile("", "heap_profile_*.pb.gz")
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

// handleAllocFlameGraph generates allocation flame graph SVG directly
func (c *Cluster) handleAllocFlameGraph(w http.ResponseWriter, r *http.Request) {
	// Collect alloc profile (shows allocation sites)
	var profileBuf bytes.Buffer
	profile := pprof.Lookup("allocs")
	if profile == nil {
		http.Error(w, "Alloc profile not available", http.StatusInternalServerError)
		return
	}

	if err := profile.WriteTo(&profileBuf, 0); err != nil {
		http.Error(w, "Failed to write alloc profile: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Write profile data to temp file
	tmpFile, err := ioutil.TempFile("", "alloc_profile_*.pb.gz")
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

// handleFunctionProfile collects a CPU profile and returns the top functions by time.
func (c *Cluster) handleFunctionProfile(w http.ResponseWriter, r *http.Request) {
	var profileBuf bytes.Buffer
	if err := pprof.StartCPUProfile(&profileBuf); err != nil {
		http.Error(w, "Failed to start CPU profile: "+err.Error(), http.StatusInternalServerError)
		return
	}

	time.Sleep(30 * time.Second)
	pprof.StopCPUProfile()

	tmpFile, err := ioutil.TempFile("", "function_profile_*.pb.gz")
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

	cmd := exec.Command("go", "tool", "pprof", "-top", tmpFile.Name())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	topOutput, err := cmd.Output()
	if err != nil {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "Failed to generate function profile: %v\nstderr: %s", err, stderr.String())
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write(topOutput)
}
