// transcode_api.go - HTTP API for media transcoding
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// handleTranscodeAPI handles transcoding requests for media files
func (c *Cluster) handleTranscodeAPI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract file path from URL
	path := strings.TrimPrefix(r.URL.Path, "/api/transcode/")
	if path == "" {
		http.Error(w, "missing file path", http.StatusBadRequest)
		return
	}

	// URL decode the path
	decodedPath, err := url.PathUnescape(path)
	if err != nil {
		http.Error(w, "invalid file path", http.StatusBadRequest)
		return
	}

	// Ensure path starts with /
	if !strings.HasPrefix(decodedPath, "/") {
		decodedPath = "/" + decodedPath
	}

	// Check if transcode parameter is present
	transcodeFormat := r.URL.Query().Get("format")
	if transcodeFormat == "" {
		transcodeFormat = "web" // default
	}

	if transcodeFormat != "web" {
		http.Error(w, "only 'web' format supported", http.StatusBadRequest)
		return
	}

	// Get the original file
	fileData, contentType, err := c.FileSystem.GetFileWithContentType(decodedPath)
	if err != nil {
		c.Logger.Printf("[TRANSCODE_API] Failed to get file %s: %v", decodedPath, err)
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	defer fileData.Close()

	// Check if file needs transcoding
	if !needsTranscoding(contentType, filepath.Base(decodedPath)) {
		c.Logger.Printf("[TRANSCODE_API] File %s (%s) doesn't need transcoding", decodedPath, contentType)
		// Serve original file
		c.serveFile(w, r, fileData, contentType, filepath.Base(decodedPath))
		return
	}

	// Create transcode request
	req := TranscodeRequest{
		InputPath:    decodedPath,
		ContentType:  contentType,
		OutputFormat: transcodeFormat,
	}

	// If content type is generic, try to infer from filename
	if contentType == "application/octet-stream" || contentType == "" {
		filename := strings.ToLower(filepath.Base(decodedPath))
		if strings.HasSuffix(filename, ".webm") || strings.HasSuffix(filename, ".avi") || 
		   strings.HasSuffix(filename, ".mkv") || strings.HasSuffix(filename, ".mov") {
			req.ContentType = "video/unknown"
		} else if strings.HasSuffix(filename, ".wav") || strings.HasSuffix(filename, ".flac") ||
				  strings.HasSuffix(filename, ".ogg") {
			req.ContentType = "audio/unknown"
		}
	}

	// Transcode with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	outputPath, err := c.Transcoder.TranscodeToWeb(ctx, fileData, req)
	if err != nil {
		c.Logger.Printf("[TRANSCODE_API] Transcoding failed for %s: %v", decodedPath, err)
		http.Error(w, fmt.Sprintf("transcoding failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Serve transcoded file
	transcodedFile, err := os.Open(outputPath)
	if err != nil {
		c.Logger.Printf("[TRANSCODE_API] Failed to open transcoded file %s: %v", outputPath, err)
		http.Error(w, "failed to open transcoded file", http.StatusInternalServerError)
		return
	}
	defer transcodedFile.Close()

	// Determine output content type
	var outputContentType string
	if strings.HasPrefix(req.ContentType, "video/") {
		outputContentType = "video/mp4"
	} else if strings.HasPrefix(req.ContentType, "audio/") {
		outputContentType = "audio/mp4"
	} else {
		// Fallback - determine by original content type or filename
		filename := strings.ToLower(filepath.Base(decodedPath))
		videoExts := []string{".webm", ".avi", ".mkv", ".mov", ".flv", ".wmv", ".m4v"}
		for _, ext := range videoExts {
			if strings.HasSuffix(filename, ext) {
				outputContentType = "video/mp4"
				break
			}
		}
		if outputContentType == "" {
			audioExts := []string{".wav", ".flac", ".ogg", ".wma", ".m4a"}
			for _, ext := range audioExts {
				if strings.HasSuffix(filename, ext) {
					outputContentType = "audio/mp4"
					break
				}
			}
		}
		if outputContentType == "" {
			outputContentType = "application/octet-stream"
		}
	}

	// Get file info for proper serving
	stat, err := transcodedFile.Stat()
	if err != nil {
		http.Error(w, "failed to stat transcoded file", http.StatusInternalServerError)
		return
	}

	// Serve with proper headers for streaming
	filename := filepath.Base(decodedPath)
	if ext := filepath.Ext(filename); ext != "" {
		filename = strings.TrimSuffix(filename, ext) + ".mp4"
	} else {
		filename = filename + ".mp4"
	}

	c.serveFileWithSeeker(w, r, transcodedFile, outputContentType, filename, stat.Size())
}

// needsTranscoding determines if a file needs transcoding for web compatibility
func needsTranscoding(contentType string, filename string) bool {
	// Check by content type first
	switch contentType {
	case "video/mp4":
		// MP4 usually works, but might have codec issues
		return false
	case "video/webm", "video/ogg", "video/avi", "video/mov", "video/mkv":
		// These often have codec issues in browsers
		return true
	case "audio/mpeg", "audio/mp3":
		// MP3 usually works
		return false
	case "audio/mp4", "audio/aac":
		// AAC usually works
		return false
	case "audio/wav", "audio/ogg", "audio/flac":
		// These often need transcoding
		return true
	case "application/octet-stream", "":
		// Generic content type - check by file extension
		filename = strings.ToLower(filename)
		// Video extensions that need transcoding
		videoExts := []string{".webm", ".avi", ".mkv", ".mov", ".flv", ".wmv", ".m4v"}
		for _, ext := range videoExts {
			if strings.HasSuffix(filename, ext) {
				return true
			}
		}
		// Audio extensions that need transcoding
		audioExts := []string{".wav", ".flac", ".ogg", ".wma", ".m4a"}
		for _, ext := range audioExts {
			if strings.HasSuffix(filename, ext) {
				return true
			}
		}
		// Video extensions that usually work
		if strings.HasSuffix(filename, ".mp4") {
			return false
		}
		// Audio extensions that usually work
		if strings.HasSuffix(filename, ".mp3") {
			return false
		}
		// Unknown file type with media-like extension
		mediaExts := []string{".mp4", ".mp3", ".webm", ".wav", ".avi", ".mkv", ".mov"}
		for _, ext := range mediaExts {
			if strings.HasSuffix(filename, ext) {
				// If we can't determine, err on the side of transcoding
				return true
			}
		}
		return false
	default:
		// Unknown video/audio types likely need transcoding
		return strings.HasPrefix(contentType, "video/") || strings.HasPrefix(contentType, "audio/")
	}
}

// serveFileWithSeeker serves a file that supports seeking (for HTTP range requests)
func (c *Cluster) serveFileWithSeeker(w http.ResponseWriter, r *http.Request, file *os.File, contentType, filename string, size int64) {
	// Set headers for streaming support
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))

	// Handle range requests for seeking
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		ranges, err := parseRange(rangeHeader, size)
		if err != nil || len(ranges) != 1 {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		start := ranges[0].start
		end := ranges[0].end
		length := end - start + 1

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.WriteHeader(http.StatusPartialContent)

		// Seek to start position
		if _, err := file.Seek(start, io.SeekStart); err != nil {
			c.Logger.Printf("[TRANSCODE_API] Seek failed: %v", err)
			return
		}

		// Copy the requested range
		io.CopyN(w, file, length)
	} else {
		// Serve entire file
		w.WriteHeader(http.StatusOK)
		io.Copy(w, file)
	}
}

// httpRange represents a single HTTP range request
type httpRange struct {
	start, end int64
}

// parseRange parses HTTP Range header
func parseRange(s string, size int64) ([]httpRange, error) {
	if s == "" {
		return nil, nil
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return nil, fmt.Errorf("invalid range")
	}
	var ranges []httpRange
	for _, ra := range strings.Split(s[len(b):], ",") {
		ra = strings.TrimSpace(ra)
		if ra == "" {
			continue
		}
		i := strings.Index(ra, "-")
		if i < 0 {
			return nil, fmt.Errorf("invalid range")
		}
		start, end := strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
		var r httpRange
		if start == "" {
			// If no start is specified, end specifies the
			// range start relative to the end of the file.
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid range")
			}
			if i > size {
				i = size
			}
			r.start = size - i
			r.end = size - 1
		} else {
			i, err := strconv.ParseInt(start, 10, 64)
			if err != nil || i < 0 {
				return nil, fmt.Errorf("invalid range")
			}
			r.start = i
			if end == "" {
				// If no end is specified, range extends to end of the file.
				r.end = size - 1
			} else {
				i, err := strconv.ParseInt(end, 10, 64)
				if err != nil || r.start > i {
					return nil, fmt.Errorf("invalid range")
				}
				if i >= size {
					i = size - 1
				}
				r.end = i
			}
		}
		if r.start > size {
			return nil, fmt.Errorf("invalid range")
		}
		ranges = append(ranges, r)
	}
	return ranges, nil
}

// serveFile serves a file with appropriate headers
func (c *Cluster) serveFile(w http.ResponseWriter, r *http.Request, reader io.ReadCloser, contentType, filename string) {
	defer reader.Close()

	// Set content type
	if contentType == "" {
		contentType = mime.TypeByExtension(filepath.Ext(filename))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
	}
	w.Header().Set("Content-Type", contentType)

	// Copy file data
	io.Copy(w, reader)
}

// handleTranscodeStats returns transcoder cache statistics
func (c *Cluster) handleTranscodeStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := c.Transcoder.GetCacheStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
