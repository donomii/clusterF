// transcoder.go - Server-side media transcoding for web compatibility
package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Transcoder struct {
	cacheDir    string
	maxCacheSize int64 // bytes
	logger      *log.Logger
	mu          sync.RWMutex
	cacheEntries map[string]*CacheEntry
}

type CacheEntry struct {
	Path      string
	Size      int64
	AccessTime time.Time
	InProgress bool
	mu        sync.RWMutex
}

type TranscodeRequest struct {
	InputPath   string
	ContentType string
	OutputFormat string // "web" for now
}

func NewTranscoder(cacheDir string, maxCacheSize int64, logger *log.Logger) *Transcoder {
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		logger.Printf("[TRANSCODE] Failed to create cache dir %s: %v", cacheDir, err)
	}

	t := &Transcoder{
		cacheDir:     cacheDir,
		maxCacheSize: maxCacheSize,
		logger:       logger,
		cacheEntries: make(map[string]*CacheEntry),
	}

	// Load existing cache entries
	t.scanCacheDir()
	
	return t
}

// checkFFmpegAvailable verifies ffmpeg is installed and accessible
func (t *Transcoder) checkFFmpegAvailable() bool {
	cmd := exec.Command("ffmpeg", "-version")
	if err := cmd.Run(); err != nil {
		t.logger.Printf("[TRANSCODE] ffmpeg not available: %v", err)
		return false
	}
	return true
}

// generateCacheKey creates a unique cache key for the transcode request
func (t *Transcoder) generateCacheKey(req TranscodeRequest) string {
	h := sha256.New()
	h.Write([]byte(req.InputPath))
	h.Write([]byte(req.ContentType))
	h.Write([]byte(req.OutputFormat))
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// getCachedPath returns the filesystem path for a cached transcode
func (t *Transcoder) getCachedPath(cacheKey string, outputExt string) string {
	return filepath.Join(t.cacheDir, cacheKey+outputExt)
}

// TranscodeToWeb transcodes media to web-compatible format with streaming support
func (t *Transcoder) TranscodeToWeb(ctx context.Context, inputReader io.Reader, req TranscodeRequest) (string, error) {
	if !t.checkFFmpegAvailable() {
		return "", fmt.Errorf("ffmpeg not available")
	}

	cacheKey := t.generateCacheKey(req)
	
	// Determine output format and extension
	var outputExt string
	var ffmpegArgs []string
	
	if strings.HasPrefix(req.ContentType, "video/") {
		outputExt = ".mp4"
		ffmpegArgs = []string{
			"-i", "pipe:0",
			"-c:v", "libx264",
			"-preset", "fast", // balance speed vs size
			"-crf", "23", // good quality
			"-c:a", "aac",
			"-movflags", "frag_keyframe+empty_moov", // streaming support
			"-f", "mp4",
			"-y", // overwrite
		}
	} else if strings.HasPrefix(req.ContentType, "audio/") {
		outputExt = ".mp4" // AAC in MP4 container for better seeking
		ffmpegArgs = []string{
			"-i", "pipe:0",
			"-c:a", "aac",
			"-b:a", "128k",
			"-movflags", "frag_keyframe+empty_moov",
			"-f", "mp4",
			"-y",
		}
	} else {
		return "", fmt.Errorf("unsupported content type for transcoding: %s", req.ContentType)
	}

	outputPath := t.getCachedPath(cacheKey, outputExt)
	
	// Check if already cached
	t.mu.RLock()
	if entry, exists := t.cacheEntries[cacheKey]; exists {
		if !entry.InProgress {
			// Update access time
			entry.mu.Lock()
			entry.AccessTime = time.Now()
			entry.mu.Unlock()
			t.mu.RUnlock()
			
			// Verify file still exists
			if _, err := os.Stat(outputPath); err == nil {
				t.logger.Printf("[TRANSCODE] Cache hit for %s", cacheKey)
				return outputPath, nil
			} else {
				// File missing, remove from cache map
				t.mu.RUnlock()
				t.mu.Lock()
				delete(t.cacheEntries, cacheKey)
				t.mu.Unlock()
				t.mu.RLock()
			}
		}
	}
	t.mu.RUnlock()

	// Mark as in progress
	t.mu.Lock()
	entry := &CacheEntry{
		Path:       outputPath,
		AccessTime: time.Now(),
		InProgress: true,
	}
	t.cacheEntries[cacheKey] = entry
	t.mu.Unlock()

	// Ensure we clean up on failure
	defer func() {
		entry.mu.Lock()
		entry.InProgress = false
		entry.mu.Unlock()
	}()

	t.logger.Printf("[TRANSCODE] Starting transcode to %s", outputPath)
	start := time.Now()

	// Create temporary file for input (ffmpeg needs seekable input for some operations)
	tempInput, err := os.CreateTemp("", "transcode_input_*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp input file: %v", err)
	}
	defer os.Remove(tempInput.Name())
	defer tempInput.Close()

	// Copy input to temp file
	if _, err := io.Copy(tempInput, inputReader); err != nil {
		return "", fmt.Errorf("failed to copy input data: %v", err)
	}
	tempInput.Close()

	// Replace pipe:0 with temp file path
	for i, arg := range ffmpegArgs {
		if arg == "pipe:0" {
			ffmpegArgs[i] = tempInput.Name()
			break
		}
	}

	// Add output path
	ffmpegArgs = append(ffmpegArgs, outputPath)

	// Run ffmpeg
	cmd := exec.CommandContext(ctx, "ffmpeg", ffmpegArgs...)
	
	// Capture stderr for debugging
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return "", fmt.Errorf("failed to get stderr pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("failed to start ffmpeg: %v", err)
	}

	// Read stderr in goroutine (ffmpeg is verbose)
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				// Only log errors, not all ffmpeg output
				output := string(buf[:n])
				if strings.Contains(output, "error") || strings.Contains(output, "Error") {
					t.logger.Printf("[TRANSCODE] ffmpeg: %s", strings.TrimSpace(output))
				}
			}
			if err != nil {
				break
			}
		}
	}()

	if err := cmd.Wait(); err != nil {
		os.Remove(outputPath) // Clean up partial file
		return "", fmt.Errorf("ffmpeg failed: %v", err)
	}

	duration := time.Since(start)
	
	// Get file size
	if stat, err := os.Stat(outputPath); err == nil {
		entry.mu.Lock()
		entry.Size = stat.Size()
		entry.mu.Unlock()
		
		t.logger.Printf("[TRANSCODE] Completed %s in %v (size: %d bytes)", 
			cacheKey, duration, stat.Size())
	} else {
		t.logger.Printf("[TRANSCODE] Completed %s in %v", cacheKey, duration)
	}

	// Clean up cache if needed
	go t.cleanupCache()

	return outputPath, nil
}

// scanCacheDir loads existing cache entries from disk
func (t *Transcoder) scanCacheDir() {
	entries, err := os.ReadDir(t.cacheDir)
	if err != nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		
		name := entry.Name()
		if !strings.HasSuffix(name, ".mp4") {
			continue
		}
		
		cacheKey := strings.TrimSuffix(name, ".mp4")
		if len(cacheKey) != 16 { // Our cache keys are 16 hex chars
			continue
		}
		
		fullPath := filepath.Join(t.cacheDir, name)
		if stat, err := os.Stat(fullPath); err == nil {
			t.cacheEntries[cacheKey] = &CacheEntry{
				Path:       fullPath,
				Size:       stat.Size(),
				AccessTime: stat.ModTime(),
				InProgress: false,
			}
		}
	}
	
	t.logger.Printf("[TRANSCODE] Loaded %d cache entries", len(t.cacheEntries))
}

// cleanupCache removes old entries if cache size exceeds limit
func (t *Transcoder) cleanupCache() {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Calculate total cache size
	var totalSize int64
	for _, entry := range t.cacheEntries {
		entry.mu.RLock()
		totalSize += entry.Size
		entry.mu.RUnlock()
	}

	if totalSize <= t.maxCacheSize {
		return
	}

	t.logger.Printf("[TRANSCODE] Cache cleanup: %d bytes (limit: %d bytes)", totalSize, t.maxCacheSize)

	// Sort entries by access time (oldest first)
	type entryWithKey struct {
		key   string
		entry *CacheEntry
	}

	var entries []entryWithKey
	for key, entry := range t.cacheEntries {
		entry.mu.RLock()
		if !entry.InProgress {
			entries = append(entries, entryWithKey{key, entry})
		}
		entry.mu.RUnlock()
	}

	// Sort by access time
	for i := 0; i < len(entries)-1; i++ {
		for j := i + 1; j < len(entries); j++ {
			entries[i].entry.mu.RLock()
			entries[j].entry.mu.RLock()
			if entries[i].entry.AccessTime.After(entries[j].entry.AccessTime) {
				entries[i], entries[j] = entries[j], entries[i]
			}
			entries[j].entry.mu.RUnlock()
			entries[i].entry.mu.RUnlock()
		}
	}

	// Remove oldest entries until under limit
	removed := 0
	for _, entryWithKey := range entries {
		if totalSize <= t.maxCacheSize {
			break
		}
		
		entry := entryWithKey.entry
		entry.mu.RLock()
		size := entry.Size
		path := entry.Path
		entry.mu.RUnlock()

		if err := os.Remove(path); err != nil {
			t.logger.Printf("[TRANSCODE] Failed to remove cache file %s: %v", path, err)
		} else {
			delete(t.cacheEntries, entryWithKey.key)
			totalSize -= size
			removed++
		}
	}

	if removed > 0 {
		t.logger.Printf("[TRANSCODE] Removed %d cache entries, new size: %d bytes", removed, totalSize)
	}
}

// GetCacheStats returns cache statistics
func (t *Transcoder) GetCacheStats() map[string]interface{} {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var totalSize int64
	var inProgress int
	
	for _, entry := range t.cacheEntries {
		entry.mu.RLock()
		totalSize += entry.Size
		if entry.InProgress {
			inProgress++
		}
		entry.mu.RUnlock()
	}

	return map[string]interface{}{
		"total_entries": len(t.cacheEntries),
		"total_size":    totalSize,
		"max_size":      t.maxCacheSize,
		"in_progress":   inProgress,
		"cache_dir":     t.cacheDir,
	}
}
