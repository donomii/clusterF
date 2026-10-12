// transcoder.go - Server-side media transcoding for web compatibility
package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/types"
)

type Transcoder struct {
	cacheDir     string
	maxCacheSize int64 // bytes
	logger       *log.Logger
	cacheEntries *syncmap.SyncMap[string, *CacheEntry]
	inflight     *syncmap.SyncMap[string, *transcodeFlight]
}

type transcodeFlight struct {
	done chan struct{}
	path string
	err  error
}

type CacheEntry struct {
	Path       string
	Size       int64
	AccessTime time.Time
	InProgress bool
}

type TranscodeRequest struct {
	InputPath    string
	ContentType  string
	OutputFormat string // "web" for now
	Checksum     string
}

func NewTranscoder(cacheDir string, maxCacheSize int64, logger *log.Logger) *Transcoder {
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		logger.Printf("[TRANSCODE] Failed to create cache dir %s: %v", cacheDir, err)
	}

	t := &Transcoder{
		cacheDir:     cacheDir,
		maxCacheSize: maxCacheSize,
		logger:       logger,
		cacheEntries: syncmap.NewSyncMap[string, *CacheEntry](),
		inflight:     syncmap.NewSyncMap[string, *transcodeFlight](),
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
	fmt.Fprintf(h, "%q|%q|%q|%s", req.InputPath, req.ContentType, req.OutputFormat, req.Checksum)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// getCachedPath returns the filesystem path for a cached transcode
func (t *Transcoder) getCachedPath(cacheKey string, outputExt string) string {
	return filepath.Join(t.cacheDir, cacheKey+outputExt)
}

// TranscodeToWeb transcodes media to web-compatible format with streaming support
func (t *Transcoder) TranscodeToWeb(ctx context.Context, inputReader io.Reader, req TranscodeRequest) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	// Hash the actual input so replacing a path cannot reuse a previous version's output.
	tempInput, err := os.CreateTemp(t.cacheDir, "transcode_input_*")
	if err != nil {
		return "", fmt.Errorf("create transcode input for %s: %w", req.InputPath, err)
	}
	defer os.Remove(tempInput.Name())
	defer tempInput.Close()
	checksum := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tempInput, checksum), inputReader); err != nil {
		return "", fmt.Errorf("read transcode input for %s: %w", req.InputPath, err)
	}
	if err := tempInput.Close(); err != nil {
		return "", fmt.Errorf("close transcode input for %s: %w", req.InputPath, err)
	}
	req.Checksum = fmt.Sprintf("%x", checksum.Sum(nil))
	cacheKey := t.generateCacheKey(req)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		flight := &transcodeFlight{done: make(chan struct{})}
		active, loaded := t.inflight.LoadOrStore(cacheKey, flight)
		if loaded {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-active.done:
				if errors.Is(active.err, context.Canceled) || errors.Is(active.err, context.DeadlineExceeded) {
					continue
				}
				return active.path, active.err
			}
		}
		defer func() {
			t.inflight.Delete(cacheKey)
			close(flight.done)
		}()
		flight.path, flight.err = t.transcodeFileToWeb(ctx, tempInput.Name(), req)
		if ctx.Err() != nil && flight.err != nil {
			flight.err = ctx.Err()
		}
		return flight.path, flight.err
	}
}

func (t *Transcoder) transcodeFileToWeb(ctx context.Context, inputPath string, req TranscodeRequest) (string, error) {
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
	entry, exists := t.cacheEntries.Load(cacheKey)
	if exists && !entry.InProgress {
		// Update access time
		updated := *entry
		updated.AccessTime = time.Now()
		t.cacheEntries.Store(cacheKey, &updated)
		// Verify file still exists
		if _, err := os.Stat(outputPath); err == nil {
			t.logger.Printf("[TRANSCODE] Cache hit for %s", cacheKey)
			return outputPath, nil
		} else {
			// File missing, remove from cache map
			t.cacheEntries.Delete(cacheKey)
		}
	}
	if !t.checkFFmpegAvailable() {
		return "", fmt.Errorf("ffmpeg not available for transcoding %s", req.InputPath)
	}

	entry = &CacheEntry{
		Path:       outputPath,
		AccessTime: time.Now(),
		InProgress: true,
	}
	t.cacheEntries.Store(cacheKey, entry)

	// Ensure we clean up on failure
	completed := false
	defer func() {
		if !completed {
			t.cacheEntries.Delete(cacheKey)
		}
	}()

	t.logger.Printf("[TRANSCODE] Starting transcode to %s", outputPath)
	start := time.Now()

	// Publish only completed output; concurrent readers must never see a partial conversion.
	tempOutput, err := os.CreateTemp(t.cacheDir, "transcode_output_*")
	if err != nil {
		return "", fmt.Errorf("create transcode output for %s: %w", req.InputPath, err)
	}
	defer os.Remove(tempOutput.Name())
	if err := tempOutput.Close(); err != nil {
		return "", err
	}

	// Replace pipe:0 with temp file path
	for i, arg := range ffmpegArgs {
		if arg == "pipe:0" {
			ffmpegArgs[i] = inputPath
			break
		}
	}

	// Add output path
	ffmpegArgs = append(ffmpegArgs, tempOutput.Name())

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
		return "", fmt.Errorf("ffmpeg failed: %v", err)
	}
	if err := os.Rename(tempOutput.Name(), outputPath); err != nil {
		return "", fmt.Errorf("publish transcode output %s: %w", outputPath, err)
	}

	duration := time.Since(start)

	// Get file size
	if stat, err := os.Stat(outputPath); err == nil {
		finished := *entry
		finished.Size = stat.Size()
		finished.InProgress = false
		t.cacheEntries.Store(cacheKey, &finished)
		completed = true

		t.logger.Printf("[TRANSCODE] Completed %s in %v (size: %d bytes)",
			cacheKey, duration, stat.Size())
	} else {
		return "", fmt.Errorf("stat transcode output %s: %w", outputPath, err)
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

	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".mp4") {
			continue
		}

		cacheKey := strings.TrimSuffix(name, ".mp4")
		if len(cacheKey) != 16 && len(cacheKey) != 64 { // Include earlier cache keys in eviction accounting.
			continue
		}

		fullPath := filepath.Join(t.cacheDir, name)
		if stat, err := os.Stat(fullPath); err == nil {
			t.cacheEntries.Store(cacheKey, &CacheEntry{
				Path:       fullPath,
				Size:       stat.Size(),
				AccessTime: stat.ModTime(),
				InProgress: false,
			})
			count++
		}
	}

	t.logger.Printf("[TRANSCODE] Loaded %d cache entries", count)
}

// cleanupCache removes old entries if cache size exceeds limit
func (t *Transcoder) cleanupCache() {
	// Calculate total cache size
	var totalSize int64
	var entries []struct {
		key   string
		entry *CacheEntry
	}

	t.cacheEntries.Range(func(key string, entry *CacheEntry) bool {
		totalSize += entry.Size
		if !entry.InProgress {
			entries = append(entries, struct {
				key   string
				entry *CacheEntry
			}{key, entry})
		}
		return true
	})

	if totalSize <= t.maxCacheSize {
		return
	}

	t.logger.Printf("[TRANSCODE] Cache cleanup: %d bytes (limit: %d bytes)", totalSize, t.maxCacheSize)

	// Sort by access time
	for i := 0; i < len(entries)-1; i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].entry.AccessTime.After(entries[j].entry.AccessTime) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	// Remove oldest entries until under limit
	removed := 0
	for _, entryWithKey := range entries {
		if totalSize <= t.maxCacheSize {
			break
		}

		entry := entryWithKey.entry
		size := entry.Size
		path := entry.Path

		if err := os.Remove(path); err != nil {
			t.logger.Printf("[TRANSCODE] Failed to remove cache file %s: %v", path, err)
		} else {
			t.cacheEntries.Delete(entryWithKey.key)
			totalSize -= size
			removed++
		}
	}

	if removed > 0 {
		t.logger.Printf("[TRANSCODE] Removed %d cache entries, new size: %d bytes", removed, totalSize)
	}
}

// GetCacheStats returns cache statistics
func (t *Transcoder) GetCacheStats() types.TranscodeStatistics {
	var totalSize int64
	var inProgress int
	var totalEntries int

	t.cacheEntries.Range(func(key string, entry *CacheEntry) bool {
		totalSize += entry.Size
		if entry.InProgress {
			inProgress++
		}
		totalEntries++
		return true
	})

	return types.TranscodeStatistics{
		TotalEntries: totalEntries,
		TotalSize:    int(totalSize),
		MaxSize:      int(t.maxCacheSize),
		InProgress:   inProgress,
		CacheDir:     t.cacheDir,
	}
}
