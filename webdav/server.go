// Package webdav provides WebDAV server functionality for the cluster filesystem
package webdav

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/donomii/clusterF/types"
	webdav "github.com/donomii/go-webdav"
)

// Server represents a WebDAV server instance
type Server struct {
	handler    *webdav.Handler
	httpServer *http.Server
	logger     *log.Logger
}

// NewServer creates a new WebDAV server
func NewServer(fs types.FileSystemLike, clusterDir string, port int, logger *log.Logger) *Server {
	clusterFS := NewClusterFileSystem(fs, clusterDir)
	
	handler := &webdav.Handler{
		FileSystem: clusterFS,
	}
	
	addr := fmt.Sprintf(":%d", port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: handler,
	}
	
	return &Server{
		handler:    handler,
		httpServer: httpServer,
		logger:     logger,
	}
}

// Start starts the WebDAV server
func (s *Server) Start() error {
	s.logger.Printf("[WEBDAV] Starting WebDAV server on %s", s.httpServer.Addr)
	
	// Start server in a goroutine
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Printf("[WEBDAV] Server error: %v", err)
		}
	}()
	
	return nil
}

// Stop stops the WebDAV server
func (s *Server) Stop() error {
	s.logger.Printf("[WEBDAV] Stopping WebDAV server")
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	return s.httpServer.Shutdown(ctx)
}

// StartWebDAVServer is a convenience function to start a WebDAV server
func StartWebDAVServer(fs types.FileSystemLike, clusterDir string, port int, logger *log.Logger) error {
	if logger == nil {
		logger = log.Default()
	}
	
	server := NewServer(fs, clusterDir, port, logger)
	
	logger.Printf("[WEBDAV] WebDAV server starting on port %d", port)
	if clusterDir != "" {
		logger.Printf("[WEBDAV] Serving cluster path: %s", clusterDir)
	} else {
		logger.Printf("[WEBDAV] Serving entire cluster")
	}
	
	return server.Start()
}
