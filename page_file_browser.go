package main

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "path/filepath"
    "strings"
)

// handleFiles serves the file browser interface
func (c *Cluster) handleFiles(w http.ResponseWriter, r *http.Request) {
    // Strip /files/ prefix to get path
    path := strings.TrimPrefix(r.URL.Path, "/files")
    if path == "" {
        path = "/"
    }

    w.Header().Set("Content-Type", "text/html; charset=utf-8")

    html := c.generateFileBrowserHTML()
    w.Write([]byte(html))
}

// handleFilesAPI handles file system API operations
func (c *Cluster) handleFilesAPI(w http.ResponseWriter, r *http.Request) {
    // Strip /api/files/ prefix to get path
    path := strings.TrimPrefix(r.URL.Path, "/api/files")
    if path == "" {
        path = "/"
    }

    // Ensure path starts with /
    if !strings.HasPrefix(path, "/") {
        path = "/" + path
    }

    switch r.Method {
    case http.MethodGet:
        c.handleFileGet(w, r, path)
    case http.MethodPut:
        c.handleFilePut(w, r, path)
    case http.MethodDelete:
        c.handleFileDelete(w, r, path)
    case http.MethodPost:
        c.handleFilePost(w, r, path)
    default:
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
    }
}

// handleFileGet handles GET requests for files/directories
func (c *Cluster) handleFileGet(w http.ResponseWriter, r *http.Request, path string) {
    c.debugf("[FILES] GET request for path: %s", path)
    
    // Try to get as file first
    if content, metadata, err := c.FileSystem.GetFile(path); err == nil {
        c.debugf("[FILES] Retrieved file %s: %d bytes, content type: %s", path, len(content), metadata.ContentType)
        
        // It's a file - serve it (optionally as download)
        download := r.URL.Query().Get("download")
        filename := filepath.Base(path)
        if download == "1" || download == "true" {
            // Force download
            w.Header().Set("Content-Type", "application/octet-stream")
            w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
        } else {
            // Inline
            ct := metadata.ContentType
            if ct == "" {
                ct = "application/octet-stream"
            }
            w.Header().Set("Content-Type", ct)
        }
        w.Header().Set("Content-Length", fmt.Sprintf("%d", metadata.Size))
        w.Header().Set("Last-Modified", metadata.ModifiedAt.Format(http.TimeFormat))
        w.WriteHeader(http.StatusOK)
        w.Write(content)
        return
    } else {
        c.debugf("[FILES] Failed to get file %s: %v", path, err)
    }

    // Try to list as directory
    if entries, err := c.FileSystem.ListDirectory(path); err == nil {
        // It's a directory - return JSON listing
        w.Header().Set("Content-Type", "application/json")
        response := map[string]interface{}{
            "path":    path,
            "entries": entries,
            "count":   len(entries),
        }
        json.NewEncoder(w).Encode(response)
        return
    }

    // Neither file nor directory found
    http.NotFound(w, r)
}

// handleFilePut handles PUT requests for uploading files
func (c *Cluster) handleFilePut(w http.ResponseWriter, r *http.Request, path string) {
    // Read the request body
    content, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "Failed to read request body", http.StatusBadRequest)
        return
    }

    // Determine content type
    contentType := r.Header.Get("Content-Type")
    if contentType == "" {
        contentType = "application/octet-stream"
    }

    // Store the file
    if err := c.FileSystem.StoreFile(path, content, contentType); err != nil {
        http.Error(w, fmt.Sprintf("Failed to store file: %v", err), http.StatusInternalServerError)
        return
    }

    c.Logger.Printf("[FILES] Stored %s (%d bytes)", path, len(content))
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(map[string]interface{}{
        "success": true,
        "path":    path,
        "size":    len(content),
    })
}

// handleFilePost handles POST requests for creating directories
func (c *Cluster) handleFilePost(w http.ResponseWriter, r *http.Request, path string) {
    // Check if this is a directory creation request
    if r.Header.Get("X-Create-Directory") == "true" {
        if err := c.FileSystem.CreateDirectory(path); err != nil {
            http.Error(w, fmt.Sprintf("Failed to create directory: %v", err), http.StatusInternalServerError)
            return
        }

        c.Logger.Printf("[FILES] Created directory %s", path)
        w.WriteHeader(http.StatusCreated)
        json.NewEncoder(w).Encode(map[string]interface{}{
            "success": true,
            "path":    path,
            "type":    "directory",
        })
        return
    }

    http.Error(w, "Invalid POST request", http.StatusBadRequest)
}

// handleFileDelete handles DELETE requests for files/directories
func (c *Cluster) handleFileDelete(w http.ResponseWriter, r *http.Request, path string) {
    if err := c.FileSystem.DeleteFile(path); err != nil {
        http.Error(w, fmt.Sprintf("Failed to delete: %v", err), http.StatusInternalServerError)
        return
    }

    c.Logger.Printf("[FILES] Deleted %s", path)
    w.WriteHeader(http.StatusNoContent)
}

// generateFileBrowserHTML creates the full file browser interface
func (c *Cluster) generateFileBrowserHTML() string {
    // Try to read the file-browser.html file
    content, err := os.ReadFile("file-browser.html")
    if err != nil {
        // Fallback to a simple interface if file not found
        return `<!DOCTYPE html>
    <html><head><meta charset="utf-8"><title>File Browser</title><link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>🐸</text></svg>"></head>
    <body style="font-family: Arial; padding: 40px; background: #1a1a2e; color: white;">
    <h1>🐸 Cluster File Browser</h1>
    <p><a href="/" style="color:#06b6d4; text-decoration:none;">← Back to Home</a></p>
    <p>File browser interface temporarily unavailable.</p>
    <p>The file-browser.html file is missing. Use the API endpoints:</p>
    <ul>
    <li>GET /api/files/ - List root directory</li>
    <li>GET /api/files/path - List directory or download file</li>
    <li>PUT /api/files/path - Upload file</li>
    <li>DELETE /api/files/path - Delete file</li>
    <li><a href="/api">API Reference</a></li>
    <li><a href="/monitor">Monitor</a></li>
    <li><a href="/cluster-visualizer.html">Visualizer</a></li>
    <li><a href="/files/">File Browser</a></li>
    <li><a href="/">Home</a></li>
    </ul>
    </body></html>`
    }

    return string(content)
}

