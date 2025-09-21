// cluster.go - Self-organizing P2P storage cluster
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donomii/clusterF/discovery"
	"github.com/donomii/clusterF/exporter"
	"github.com/donomii/clusterF/frontend"
	"github.com/donomii/clusterF/partitionmanager"
	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/threadmanager"
	"github.com/donomii/clusterF/urlutil"
	ensemblekv "github.com/donomii/ensemblekv"
	"github.com/donomii/frogpond"
)

const (
	DefaultBroadcastPort = 9999
	DefaultDataDir       = "./data"
	DefaultRF            = 3 // Default Replication Factor
)

type NodeID string

type Metadata struct {
	NodeID    NodeID `json:"node_id"`
	Timestamp int64  `json:"timestamp"`
}

// notifyFileListChanged signals all subscribers that the file list has changed
func (c *Cluster) notifyFileListChanged() {
	c.fileListMu.RLock()
	defer c.fileListMu.RUnlock()

	for ch := range c.fileListSubs {
		select {
		case ch <- struct{}{}:
		default: // Non-blocking send, skip if channel is full
		}
	}
}

type GossipMessage struct {
	From     NodeID   `json:"from"`
	Metadata Metadata `json:"metadata"`
}

// Cluster encapsulates one node's entire state & goroutines

type Cluster struct {
	// Identity & config
	ID            NodeID
	DataDir       string
	HTTPDataPort  int    // per-node HTTP data port
	DiscoveryPort int    // shared UDP announcement port
	BroadcastIP   net.IP // usually net.IPv4bcast
	Logger        *log.Logger
	Debug         bool
	NoStore       bool // client mode: don't store partitions locally

	// Discovery manager
	DiscoveryManager *discovery.DiscoveryManager
	ThreadManager    *threadmanager.ThreadManager

	// CRDT coordination layer
	frogpond *frogpond.Node

	// Partition system
	PartitionManager *partitionmanager.PartitionManager

	// File system layer
	FileSystem *ClusterFileSystem

	// HTTP clients for reuse (prevents goroutine leaks)
	httpClient     *http.Client // short-lived control traffic
	httpDataClient *http.Client // long-running data transfers

	// File list change notification system
	fileListSubs map[chan struct{}]bool
	fileListMu   sync.RWMutex

	// Server shutdown
	ctx    context.Context
	cancel context.CancelFunc
	server *http.Server

	// Profiling
	profilingActive bool
	profilingMutex  sync.Mutex

	// Optional local export directory for OS sharing (SMB/NFS/etc.)
	ExportDir string
	exporter  *exporter.Exporter

	// KV stores
	metadataKV ensemblekv.KvLike
	contentKV  ensemblekv.KvLike

	// initial full-sync flag
	initialSyncDone   atomic.Bool
	initialSyncTrig   chan struct{}
	initialSyncOnce   sync.Once
	initialSyncMu     sync.Mutex
	initialSyncCancel context.CancelFunc

	// Peer addresses
	peerAddrs *syncmap.SyncMap[NodeID, *discovery.PeerInfo]
}

type partitionDiscoveryAdapter struct {
	manager *discovery.DiscoveryManager
}

func (a partitionDiscoveryAdapter) GetPeers() []*partitionmanager.PeerInfo {
	if a.manager == nil {
		return nil
	}
	raw := a.manager.GetPeers()
	peers := make([]*partitionmanager.PeerInfo, 0, len(raw))
	for _, peer := range raw {
		if peer == nil {
			continue
		}
		peers = append(peers, &partitionmanager.PeerInfo{
			NodeID:   peer.NodeID,
			Address:  peer.Address,
			HTTPPort: peer.HTTPPort,
		})
	}
	return peers
}

type ClusterOpts struct {
	ID            string
	DataDir       string
	UDPListenPort int
	HTTPDataPort  int

	DiscoveryPort int
	BroadcastIP   net.IP
	Logger        *log.Logger
	ExportDir     string // if set, mirror files to this directory for OS sharing
	NoStore       bool   // if true, don't store partitions locally (client mode)
}

func NewCluster(opts ClusterOpts) *Cluster {

	id := opts.ID
	if id == "" {
		// If no node ID specified, try to load from a base data directory
		base := opts.DataDir
		if base == "" {
			base = DefaultDataDir
		}
		id = loadNodeIDFromDataDir(base)
		// If still no ID, generate one
		if id == "" {
			host, _ := os.Hostname()
			if host == "" {
				host = "host"
			}
			if i := strings.IndexByte(host, '.'); i >= 0 {
				host = host[:i]
			}
			re := regexp.MustCompile(`[^a-zA-Z0-9_-]+`)
			host = re.ReplaceAllString(host, "-")
			id = fmt.Sprintf("%s-%05d", host, rand.Intn(100000))
		}
	}

	if opts.Logger == nil {
		opts.Logger = log.New(os.Stdout, "("+id+") ", log.LstdFlags|log.Lshortfile)
	}

	if opts.DiscoveryPort == 0 {
		opts.DiscoveryPort = broadcastPortFromEnv()
	}
	if opts.UDPListenPort == 0 {
		opts.UDPListenPort = 30000 + rand.Intn(1000)
	}
	if opts.HTTPDataPort == 0 {
		opts.HTTPDataPort = 30000 + rand.Intn(1000)
	}

	ctx, cancel := context.WithCancel(context.Background())

	if opts.BroadcastIP == nil {
		opts.BroadcastIP = net.IPv4bcast
	}

	c := &Cluster{
		ID:              NodeID(id),
		DataDir:         opts.DataDir,
		HTTPDataPort:    opts.HTTPDataPort,
		DiscoveryPort:   opts.DiscoveryPort,
		BroadcastIP:     opts.BroadcastIP,
		Logger:          opts.Logger,
		ExportDir:       opts.ExportDir,
		NoStore:         opts.NoStore,
		initialSyncTrig: make(chan struct{}, 1),
		peerAddrs:       syncmap.NewSyncMap[NodeID, *discovery.PeerInfo](),

		fileListSubs: map[chan struct{}]bool{},

		ctx:    ctx,
		cancel: cancel,
	}
	c.debugf("Initialized cluster struct\n")

	// Treat DataDir as a base directory; always place data under a per-node subdir
	{
		base := opts.DataDir
		if base == "" {
			base = DefaultDataDir
		}
		opts.DataDir = filepath.Join(base, id)
	}

	// Ensure the data directory exists immediately
	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		if opts.Logger != nil {
			opts.Logger.Fatalf("Failed to create data directory %s: %v", opts.DataDir, err)
		} else {
			log.Fatalf("Failed to create data directory %s: %v", opts.DataDir, err)
		}
	}

	// Persist and validate node-id marker in the per-node data directory
	if err := storeNodeIDInDataDir(opts.DataDir, id); err != nil {
		if opts.Logger != nil {
			opts.Logger.Fatalf("Failed to store node id marker in %s: %v", opts.DataDir, err)
		} else {
			log.Fatalf("Failed to store node id marker in %s: %v", opts.DataDir, err)
		}
	}

	// Verify the directory was created and is writable
	if stat, err := os.Stat(opts.DataDir); err != nil {
		log.Fatalf("Data directory verification failed %s: %v", opts.DataDir, err)
	} else if !stat.IsDir() {
		log.Fatalf("Data path exists but is not a directory: %s", opts.DataDir)
	}

	// Ensure Cluster.DataDir reflects the resolved per-node path even when no data dir was specified
	c.DataDir = opts.DataDir

	c.debugf("Created data directory: %s\n", opts.DataDir)

	// Initialize thread manager
	c.ThreadManager = threadmanager.NewThreadManager(id, opts.Logger)
	c.debugf("Initialized thread manager\n")

	// Create HTTP clients with connection pooling and differentiated timeouts
	transport := &http.Transport{
		MaxIdleConns:        50, // Limit idle connections
		MaxIdleConnsPerHost: 5,  // Limit per-host connections
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
		DisableKeepAlives:   false,
	}
	c.httpClient = &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}
	dataTransport := &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 5,
		IdleConnTimeout:     2 * time.Minute,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableKeepAlives:   false,
	}
	c.httpDataClient = &http.Client{
		Timeout:   5 * time.Minute,
		Transport: dataTransport,
	}
	c.debugf("Initialized HTTP clients\n")

	// Initialize discovery manager
	c.DiscoveryManager = discovery.NewDiscoveryManager(id, opts.HTTPDataPort, opts.DiscoveryPort, c.ThreadManager, opts.Logger)
	c.debugf("Initialized discovery manager\n")

	// Initialize frogpond CRDT node
	c.frogpond = frogpond.NewNode()
	c.debugf("Initialized frogpond node\n")

	// Initialize KV stores
	filesKVPath := filepath.Join(opts.DataDir, "kv_metadata")
	crdtKVPath := filepath.Join(opts.DataDir, "kv_content")
	// Use ensemble over bolt for stability
	c.metadataKV = ensemblekv.SimpleEnsembleCreator("extent", "", filesKVPath, 8*1024*1024, 32, 256*1024*1024)
	c.contentKV = ensemblekv.SimpleEnsembleCreator("extent", "", crdtKVPath, 2*1024*1024, 16, 64*1024*1024)
	// Enforce hard-fail if storage cannot be opened to avoid split-brain directories
	if c.metadataKV == nil {
		c.Logger.Fatalf("[STORAGE] Failed to initialize files KV at %s; exiting", filesKVPath)
	}
	if c.contentKV == nil {
		c.Logger.Fatalf("[STORAGE] Failed to initialize CRDT KV at %s; exiting", crdtKVPath)
	}
	c.debugf("Initialized KV stores\n")
	// Load CRDT state from KV (if any) before applying defaults
	c.loadCRDTFromFile()
	c.debugf("Loaded CRDT state from KV\n")

	// Initialize partition manager
	deps := partitionmanager.Dependencies{
		NodeID:         partitionmanager.NodeID(c.ID),
		NoStore:        c.NoStore,
		Logger:         c.Logger,
		Debugf:         c.debugf,
		MetadataKV:     c.metadataKV,
		ContentKV:      c.contentKV,
		HTTPDataClient: c.httpDataClient,
		Discovery:      partitionDiscoveryAdapter{manager: c.DiscoveryManager},
		LoadPeer: func(id partitionmanager.NodeID) (*partitionmanager.PeerInfo, bool) {
			peer, ok := c.peerAddrs.Load(NodeID(id))
			if !ok || peer == nil {
				return nil, false
			}
			return &partitionmanager.PeerInfo{
				NodeID:   peer.NodeID,
				Address:  peer.Address,
				HTTPPort: peer.HTTPPort,
			}, true
		},
		Frogpond:              c.frogpond,
		SendUpdatesToPeers:    c.sendUpdatesToPeers,
		NotifyFileListChanged: c.notifyFileListChanged,
		GetCurrentRF:          c.getCurrentRF,
	}
	c.PartitionManager = partitionmanager.NewPartitionManager(deps)
	c.debugf("Initialized partition manager\n")

	// Initialize file system
	c.FileSystem = NewClusterFileSystem(c)
	c.debugf("Initialized file system\n")

	// Initialize exporter if configured
	if opts.ExportDir != "" {
		if exp, err := exporter.New(opts.ExportDir, c.Logger, c.FileSystem); err != nil {
			c.Logger.Printf("[EXPORT] Failed to init exporter for %s: %v", opts.ExportDir, err)
		} else {
			c.exporter = exp
			c.Logger.Printf("[EXPORT] Mirroring files to %s for OS sharing", opts.ExportDir)
		}
	}
	c.debugf("Initialized exporter (if configured)\n")

	// Initialize cluster settings (replication factor)
	c.initializeClusterSettings()
	c.debugf("Initialized cluster settings\n")

	// Store our node metadata in frogpond
	c.updateNodeMetadata()
	c.debugf("Stored initial node metadata in frogpond\n")

	// Store node ID in data directory for future reference
	if err := storeNodeIDInDataDir(opts.DataDir, id); err != nil {
		c.Logger.Printf("[WARNING] Failed to store node ID in data directory: %v", err)
	}
	c.debugf("Stored node ID in data directory\n")

	// Debug: log the data directory being used
	if opts.Logger != nil {
		c.debugf("Using data directory: %s", opts.DataDir)
	} else {
		c.debugf("Using data directory: %s", opts.DataDir)
	}

	return c
}

// loadNodeIDFromDataDir scans a base data directory for existing node IDs.
// If exactly one subdirectory exists, its name is returned as the node ID.
// If more than one subdirectory exists, the process exits with a fatal error to avoid split-brain.
// If the directory does not exist or contains no node IDs, returns empty string.
func loadNodeIDFromDataDir(base string) string {
	fi, err := os.Stat(base)
	if err == nil && fi.IsDir() {
		// If base itself has a .node-id, treat base as the node directory
		marker := filepath.Join(base, ".node-id")
		if b, err := os.ReadFile(marker); err == nil {
			id := strings.TrimSpace(string(b))
			if id != "" {
				return id
			}
		}
		// Otherwise, scan immediate subdirectories for .node-id markers
		entries, err := os.ReadDir(base)
		if err == nil {
			var found []string
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				nodeDir := filepath.Join(base, e.Name())
				marker := filepath.Join(nodeDir, ".node-id")
				if b, err := os.ReadFile(marker); err == nil {
					id := strings.TrimSpace(string(b))
					if id != "" {
						found = append(found, id)
					}
				}
			}
			if len(found) == 1 {
				return found[0]
			}
			if len(found) > 1 {
				log.Fatalf("Multiple node IDs found in data directory %s (markers): %v. Refusing to start to avoid split storage.", base, found)
			}
			// Fallback: infer from single subdirectory name if no markers present
			var dirs []string
			for _, e := range entries {
				if e.IsDir() {
					dirs = append(dirs, e.Name())
				}
			}
			if len(dirs) == 1 {
				return dirs[0]
			}
			if len(dirs) > 1 {
				log.Fatalf("Multiple node directories found in data directory %s: %v. Refusing to start to avoid split storage.", base, dirs)
			}
		}
	}
	return ""
}

// debugf logs a debug message if Debug is enabled
func (c *Cluster) debugf(format string, v ...interface{}) {
	if !c.Debug {
		return
	}
	// Use Logger.Output with a call depth so the log shows the
	// caller of debugf (file:line), not this wrapper function.
	// calldepth=2: Output -> debugf -> caller
	msg := fmt.Sprintf(format, v...)
	_ = c.Logger.Output(2, msg)
}

func broadcastPortFromEnv() int {
	if v := os.Getenv("CLUSTER_BCAST_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 && p < 65536 {
			return p
		}
	}
	return DefaultBroadcastPort
}

// loadNodeIDFromDataDir checks for existing node IDs in the data directory and errors if multiple found
// (legacy alternative loader removed)

// storeNodeIDInDataDir stores the node ID in the data directory for future reference
func storeNodeIDInDataDir(dataDir, nodeID string) error {
	if dataDir == "" || nodeID == "" {
		return nil
	}
	// Ensure dir exists
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	nodeIDFile := filepath.Join(dataDir, ".node-id")
	// If marker exists, validate it matches
	if b, err := os.ReadFile(nodeIDFile); err == nil {
		existing := strings.TrimSpace(string(b))
		if existing != "" && existing != nodeID {
			return fmt.Errorf("node-id mismatch in %s (found %q, expected %q)", dataDir, existing, nodeID)
		}
		return nil
	}
	// Write marker
	return os.WriteFile(nodeIDFile, []byte(nodeID+"\n"), 0o644)
}

// ---------- Lifecycle ----------

func (c *Cluster) Start() {
	c.Logger.Printf("Starting node %s (HTTP:%d)", c.ID, c.HTTPDataPort)
	c.debugf("Starting node %s (HTTP:%d)", c.ID, c.HTTPDataPort)

	// Start discovery manager
	if err := c.DiscoveryManager.Start(); err != nil {
		c.Logger.Fatalf("Failed to start discovery manager: %v", err)
	}
	c.debugf("Started discovery manager")

	c.debugf("Reconciled CRDT and local storage")

	// Start all threads using ThreadManager
	c.ThreadManager.StartThread("http-server", c.startHTTPServer)
	if c.exporter != nil {
		c.ThreadManager.StartThread("export-sync", c.runExportSync)
	}
	c.ThreadManager.StartThread("periodic-peer-sync", c.periodicPeerSync)
	c.ThreadManager.StartThread("frogpond-sync", c.periodicFrogpondSync)
	c.ThreadManager.StartThread("partition-check", c.PartitionManager.PeriodicPartitionCheck)
	c.debugf("Started all threads")
}

// runExportSync integrates the Exporter with the cluster lifecycle
func (c *Cluster) runExportSync(ctx context.Context) {
	if c.exporter == nil {
		return
	}
	c.Logger.Printf("[EXPORT] Starting export sync from %s", c.ExportDir)
	c.exporter.Run(ctx)
}

func (c *Cluster) Stop() {
	c.Logger.Printf("Stopping node %s", c.ID)
	c.debugf("Stopping node %s", c.ID)
	c.cancel()

	// Stop discovery manager
	c.DiscoveryManager.Stop()

	// Shutdown HTTP server
	if c.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		c.server.Shutdown(ctx)
		cancel()
	}

	// Close HTTP client transports to clean up connections
	if c.httpClient != nil && c.httpClient.Transport != nil {
		if transport, ok := c.httpClient.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
	if c.httpDataClient != nil && c.httpDataClient.Transport != nil {
		if transport, ok := c.httpDataClient.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}

	// Shutdown all threads via ThreadManager
	failedThreads := c.ThreadManager.Shutdown()
	if len(failedThreads) > 0 {
		c.Logger.Printf("Some threads failed to shutdown: %v", failedThreads)
		c.debugf("Some threads failed to shutdown: %v", failedThreads)
	}

	c.Logger.Printf("Node %s stopped", c.ID)
	c.debugf("Node %s stopped", c.ID)
}

// ---------- Repair ----------

// corsMiddleware adds CORS headers to allow browser access and logs requests
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Log all incoming requests
		log.Printf("[HTTP] %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)

		// Panic recovery
		defer func() {
			if err := recover(); err != nil {
				log.Printf("[HTTP_PANIC] %s %s: %v", r.Method, r.URL.Path, err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
			}
		}()

		// Add CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Max-Age", "86400") // 24 hours

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next(w, r)
	}
}

// startHTTPServer starts the HTTP data server with dynamic port allocation
func (c *Cluster) startHTTPServer(ctx context.Context) {
	// Try to find an available port, starting from the configured port
	port := c.HTTPDataPort
	if port == 0 {
		// If no port configured, start from random port above 30000
		port = 30000 + rand.Intn(30000)
	}

	// Try the configured port first, then random ports if it fails
	var server *http.Server
	var listener net.Listener
	var err error

	maxAttempts := 10
	for attempt := 0; attempt < maxAttempts; attempt++ {
		addr := fmt.Sprintf(":%d", port)
		listener, err = net.Listen("tcp", addr)
		if err == nil {
			// Successfully bound to port
			c.HTTPDataPort = port // Update the actual port being used
			break
		}

		// Port occupied, try a random port above 30000
		oldPort := port
		port = 30000 + rand.Intn(30000)
		c.debugf("Port %d occupied, trying port %d (attempt %d/%d)",
			oldPort, port, attempt+1, maxAttempts)
	}

	if err != nil {
		c.debugf("Failed to find available port after %d attempts: %v", maxAttempts, err)
		return
	}

	mux := http.NewServeMux()
	ui := frontend.New(c)
	mux.HandleFunc("/", corsMiddleware(ui.HandleWelcome))
	mux.HandleFunc("/status", corsMiddleware(c.handleStatus))
	// API reference page (exact path only to avoid clobbering other /api/* routes)
	mux.HandleFunc("/api", corsMiddleware(ui.HandleAPIDocs))
	mux.HandleFunc("/frogpond/update", corsMiddleware(c.handleFrogpondUpdate))
	mux.HandleFunc("/frogpond/fullsync", corsMiddleware(c.handleFrogpondFullSync))
	mux.HandleFunc("/frogpond/fullstore", corsMiddleware(c.handleFrogpondFullStore))
	mux.HandleFunc("/api/replication-factor", corsMiddleware(c.handleReplicationFactor))
	mux.HandleFunc("/flamegraph", corsMiddleware(c.handleFlameGraph))
	mux.HandleFunc("/profiling", corsMiddleware(ui.HandleProfilingPage))
	mux.HandleFunc("/api/profiling", corsMiddleware(c.handleProfilingAPI))
	// Add pprof endpoints manually
	mux.HandleFunc("/debug/pprof/", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		pprof.Index(w, r)
	}))
	mux.HandleFunc("/debug/pprof/cmdline", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		pprof.Cmdline(w, r)
	}))
	mux.HandleFunc("/debug/pprof/profile", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		pprof.Profile(w, r)
	}))
	mux.HandleFunc("/debug/pprof/symbol", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		pprof.Symbol(w, r)
	}))
	mux.HandleFunc("/debug/pprof/trace", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		pprof.Trace(w, r)
	}))
	mux.HandleFunc("/debug/pprof/heap", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		pprof.Handler("heap").ServeHTTP(w, r)
	}))
	mux.HandleFunc("/debug/pprof/goroutine", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		pprof.Handler("goroutine").ServeHTTP(w, r)
	}))
	mux.HandleFunc("/debug/pprof/block", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		pprof.Handler("block").ServeHTTP(w, r)
	}))
	mux.HandleFunc("/debug/pprof/mutex", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		pprof.Handler("mutex").ServeHTTP(w, r)
	}))
	mux.HandleFunc("/monitor", corsMiddleware(ui.HandleMonitorDashboard))
	mux.HandleFunc("/api/cluster-stats", corsMiddleware(c.handleClusterStats))
	mux.HandleFunc("/cluster-visualizer.html", corsMiddleware(ui.HandleVisualizer))
	// File system endpoints
	mux.HandleFunc("/files/", corsMiddleware(ui.HandleFiles))
	mux.HandleFunc("/loading", corsMiddleware(ui.HandleLoadingPage))
	// CRDT inspector UI + APIs
	mux.HandleFunc("/crdt", corsMiddleware(ui.HandleCRDTInspectorPageUI))
	mux.HandleFunc("/api/crdt/list", corsMiddleware(c.handleCRDTListAPI))
	mux.HandleFunc("/api/crdt/get", corsMiddleware(c.handleCRDTGetAPI))
	mux.HandleFunc("/api/crdt/search", corsMiddleware(c.handleCRDTSearchAPI))
	mux.HandleFunc("/api/files/", corsMiddleware(c.handleFilesAPI))
	// Partition sync endpoints
	mux.HandleFunc("/api/partition-sync/", corsMiddleware(c.handlePartitionSyncAPI))
	mux.HandleFunc("/api/partition-stats", corsMiddleware(c.handlePartitionStats))
	// Search API
	mux.HandleFunc("/api/search", corsMiddleware(c.handleSearchAPI))

	server = &http.Server{
		Handler: mux,
	}

	c.server = server
	c.debugf("HTTP data server listening on port %d (dir=%s)", port, c.DataDir)
	c.Logger.Printf("🐸 Node %s ready on http://localhost:%d (monitor: http://localhost:%d/monitor)", c.ID, port, port)

	// Start server in a goroutine so we can handle context cancellation
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Serve(listener)
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		c.Logger.Printf("HTTP server shutting down (context cancelled)")
		c.debugf("HTTP server shutting down (context cancelled)")
		// Graceful shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			c.Logger.Printf("HTTP server shutdown error: %v", err)
			c.debugf("HTTP server shutdown error: %v", err)
		}
	case err := <-serverDone:
		if err != nil && err != http.ErrServerClosed {
			c.Logger.Printf("HTTP server error: %v", err)
			c.debugf("HTTP server error: %v", err)
		}
	}
}

func (c *Cluster) handleStatus(w http.ResponseWriter, r *http.Request) {
	c.debugf("[STATUS] Handler called")

	// Get partition stats without holding the main mutex
	var partitionStats map[string]interface{}
	if c.PartitionManager != nil {
		c.debugf("[STATUS] Getting partition stats")
		partitionStats = c.PartitionManager.GetPartitionStats()
		c.debugf("[STATUS] Got partition stats: %+v", partitionStats)
	} else {
		c.debugf("[STATUS] PartitionManager is nil")
		partitionStats = map[string]interface{}{}
	}

	// Get replication factor safely
	var rf interface{} = DefaultRF
	if c.frogpond != nil {
		rf = c.getCurrentRF()
		c.debugf("[STATUS] Got RF: %v", rf)
	} else {
		c.debugf("[STATUS] frogpond is nil")
	}

	// Read basic fields without mutex - they're set once at startup
	status := map[string]interface{}{
		"node_id":            c.ID,
		"data_dir":           c.DataDir,
		"http_port":          c.HTTPDataPort,
		"replication_factor": rf,
		"partition_stats":    partitionStats,
	}

	// Debug: log what we're sending
	c.debugf("[STATUS] Returning status: node_id=%s, rf=%v, partition_stats=%+v", c.ID, status["replication_factor"], partitionStats)

	w.Header().Set("Content-Type", "application/json")
	c.debugf("[STATUS] Set headers")

	if err := json.NewEncoder(w).Encode(status); err != nil {
		c.debugf("[STATUS] JSON encode error: %v", err)
	} else {
		c.debugf("[STATUS] Response sent successfully")
	}
}

// handleCRDTList lists immediate children (dirs and keys) under a prefix, with pagination

// countCRDTKeys counts non-deleted keys under a given prefix in the CRDT store.

// handleCRDTSearch searches for keys containing a substring within a prefix.
// This is on-demand and may scan the prefix; use with reasonable prefixes.

// handleWelcome serves the welcome/root page

// handleVisualizer serves the cluster visualizer HTML
// moved to page_visualizer.go

// handleMonitorDashboard serves a simple cluster monitoring dashboard
// moved to page_monitor.go

// handleAPIDocs serves a human-friendly HTML page listing API endpoints
// moved to page_api_docs.go

// handleClusterStats provides cluster-wide statistics for monitoring
func (c *Cluster) handleClusterStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	peerList := c.getPeerList()
	stats := map[string]interface{}{
		"node_id":            c.ID,
		"http_port":          c.HTTPDataPort,
		"discovery_port":     c.DiscoveryPort,
		"data_dir":           c.DataDir,
		"timestamp":          time.Now().Unix(),
		"replication_factor": c.getCurrentRF(),
		"peer_list":          peerList,
	}

	// Debug: log what we're sending
	c.Logger.Printf("[CLUSTER_STATS] Returning stats: node_id=%s, peer_count=%d, rf=%v", c.ID, len(peerList), stats["replication_factor"])

	json.NewEncoder(w).Encode(stats)
}

// ---------- File System HTTP Handlers ----------

// handleFiles serves the file browser interface

// handleFilesAPI handles file system API operations

// handleFileGet handles GET requests for files/directories

// handleFilePut handles PUT requests for uploading files

// handleFilePost handles POST requests for creating directories

// handleFileDelete handles DELETE requests for files/directories

// generateFileBrowserHTML creates the full file browser interface

// ---------- Partition HTTP Handlers ----------

// handlePartitionSyncAPI serves partition data for peer synchronization
func (c *Cluster) handlePartitionSyncAPI(w http.ResponseWriter, r *http.Request) {
	// Extract partition ID from URL path
	path := strings.TrimPrefix(r.URL.Path, "/api/partition-sync/")
	if path == "" {
		http.Error(w, "missing partition ID", http.StatusBadRequest)
		return
	}

	partitionID := partitionmanager.PartitionID(path)
	c.PartitionManager.HandlePartitionSync(w, r, partitionID)
}

// handlePartitionStats returns partition statistics
func (c *Cluster) handlePartitionStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := c.PartitionManager.GetPartitionStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// ---------- Utilities ----------

// periodicPeerSync syncs peer information from discovery manager
func (c *Cluster) periodicPeerSync(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.syncPeersFromDiscovery()
		}
	}
}

// syncPeersFromDiscovery updates cluster peers from discovery manager
func (c *Cluster) syncPeersFromDiscovery() {
	discoveredPeers := c.DiscoveryManager.GetPeers()

	// Track which peers are new
	newPeers := make([]*discovery.PeerInfo, 0)

	// Update peer addresses with discovered peers
	for _, peer := range discoveredPeers {
		nodeID := NodeID(peer.NodeID)
		if nodeID != c.ID { // Don't add ourselves
			// Check if this is a new peer
			if _, exists := c.peerAddrs.Load(nodeID); !exists {
				newPeers = append(newPeers, peer)
			}
			c.peerAddrs.Store(nodeID, peer)
		}
	}

	// Remove stale peers (could be enhanced with timeout logic)
	activeNodeIDs := make(map[NodeID]bool)
	for _, peer := range discoveredPeers {
		activeNodeIDs[NodeID(peer.NodeID)] = true
	}
	activeNodeIDs[c.ID] = true // Keep ourselves

	// Clean up stale peers
	c.peerAddrs.Range(func(nodeID NodeID, peer *discovery.PeerInfo) bool {
		if !activeNodeIDs[nodeID] {
			c.peerAddrs.Delete(nodeID)
		}
		return true
	})

	// Update our own discovery info with current HTTP port (in case it changed)
	c.DiscoveryManager.UpdateNodeInfo(string(c.ID), c.HTTPDataPort)

	// Perform initial sync only once with the first peer we discover
	if len(newPeers) > 0 && !c.initialSyncDone.Load() {
		go c.performInitialSyncWithPeer(newPeers[0])
	}
}

// initializeClusterSettings sets up default cluster settings if not already present
func (c *Cluster) initializeClusterSettings() {
	// Initialize replication factor if not set
	data := c.frogpond.GetDataPoint("cluster/replication_factor")
	if data.Deleted || len(data.Value) == 0 {
		rfJSON, _ := json.Marshal(DefaultRF)
		updates := c.frogpond.SetDataPoint("cluster/replication_factor", rfJSON)
		if c.contentKV != nil {
			_ = c.contentKV.Put([]byte("cluster/replication_factor"), rfJSON)
		}
		c.sendUpdatesToPeers(updates)
		c.Logger.Printf("[INIT] Set default replication factor to %d", DefaultRF)
	}

}

// handleReplicationFactor handles GET/PUT requests for replication factor
func (c *Cluster) handleReplicationFactor(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		currentRF := c.getCurrentRF()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"replication_factor": currentRF,
		})

	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}

		var request map[string]interface{}
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		rfValue, ok := request["replication_factor"]
		if !ok {
			http.Error(w, "Missing replication_factor field", http.StatusBadRequest)
			return
		}

		rf, ok := rfValue.(float64)
		if !ok {
			http.Error(w, "replication_factor must be a number", http.StatusBadRequest)
			return
		}

		if rf < 1 {
			http.Error(w, "replication_factor must be at least 1", http.StatusBadRequest)
			return
		}

		c.setReplicationFactor(int(rf))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":            true,
			"replication_factor": int(rf),
		})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleProfilingPage serves the profiling control page

// handleProfilingAPI handles profiling control API
// moved to page_profiling.go

// startProfiling enables Go's built-in profiling
// moved to page_profiling.go

// stopProfiling disables Go's built-in profiling
// moved to page_profiling.go

// performInitialSyncWithPeer performs bidirectional full KV store sync with a new peer
func (c *Cluster) performInitialSyncWithPeer(peer *discovery.PeerInfo) {
	_ = peer // trigger is independent of which peer we saw first

	if !c.initialSyncDone.CompareAndSwap(false, true) {
		return
	}

	if err := c.ensureInitialSyncWorker(); err != nil {
		c.Logger.Printf("[INITIAL_SYNC] Failed to ensure worker: %v", err)
		c.initialSyncDone.Store(false)
		return
	}

	c.cancelInitialSyncRun()

	select {
	case c.initialSyncTrig <- struct{}{}:
	default:
	}

	if peer != nil {
		c.Logger.Printf("[INITIAL_SYNC] Triggered by discovery of %s", peer.NodeID)
	} else {
		c.Logger.Printf("[INITIAL_SYNC] Triggered initial sync")
	}
}

func (c *Cluster) ensureInitialSyncWorker() error {
	var err error
	c.initialSyncOnce.Do(func() {
		startErr := c.ThreadManager.StartThreadWithRestart("initial-sync-retry", c.initialSyncWorker, true, nil)
		if startErr != nil {
			err = startErr
		}
	})
	return err
}

func (c *Cluster) cancelInitialSyncRun() {
	c.initialSyncMu.Lock()
	if c.initialSyncCancel != nil {
		c.initialSyncCancel()
		c.initialSyncCancel = nil
	}
	c.initialSyncMu.Unlock()
}

func (c *Cluster) initialSyncWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.cancelInitialSyncRun()
			return
		case <-c.initialSyncTrig:
			runCtx, cancel := context.WithCancel(ctx)
			c.initialSyncMu.Lock()
			if c.initialSyncCancel != nil {
				c.initialSyncCancel()
			}
			c.initialSyncCancel = cancel
			c.initialSyncMu.Unlock()
			done := make(chan struct{})
			go func() {
				defer close(done)
				c.runInitialSyncCycle(runCtx)
			}()

			select {
			case <-ctx.Done():
				cancel()
			case <-done:
			}

			cancel()
			c.initialSyncMu.Lock()
			if c.initialSyncCancel != nil {
				c.initialSyncCancel = nil
			}
			c.initialSyncMu.Unlock()
		}
	}
}

func (c *Cluster) runInitialSyncCycle(ctx context.Context) {
	// Initial catch-up loop: keep trying until we complete one full sync or are cancelled.
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		peers := c.DiscoveryManager.GetPeers()
		if len(peers) == 0 {
			time.Sleep(10 * time.Second)
			continue
		}

		peer := peers[rand.Intn(len(peers))]
		c.Logger.Printf("[SYNC_RETRY] Attempting full sync with %s", peer.NodeID)
		if c.requestFullStoreFromPeer(peer) {
			c.Logger.Printf("[SYNC_RETRY] Successfully synced with %s", peer.NodeID)
			break
		}

		c.Logger.Printf("[SYNC_RETRY] Failed to sync with %s, retrying in 10s", peer.NodeID)
		time.Sleep(10 * time.Second)
	}

	// Wait 10 minutes before the next maintenance sync, respecting cancellation.
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Minute):
	}

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peers := c.DiscoveryManager.GetPeers()
			if len(peers) == 0 {
				continue
			}

			peer := peers[rand.Intn(len(peers))]
			c.Logger.Printf("[HOURLY_SYNC] Full sync with random peer %s", peer.NodeID)
			if !c.requestFullStoreFromPeer(peer) {
				c.Logger.Printf("[HOURLY_SYNC] Failed to sync with %s", peer.NodeID)
			}
		}
	}
}

// requestFullStoreFromPeer requests the complete frogpond store from a specific peer
// Returns true on success, false on failure
func (c *Cluster) requestFullStoreFromPeer(peer *discovery.PeerInfo) bool {
	fullStoreURL, err := urlutil.BuildHTTPURL(peer.Address, peer.HTTPPort, "/frogpond/fullstore")
	if err != nil {
		c.Logger.Printf("[INITIAL_SYNC] Failed to build full store URL for %s: %v", peer.NodeID, err)
		return false
	}

	// Create a client with no timeout for full sync
	client := &http.Client{
		Timeout: 0, // No timeout
	}

	resp, err := client.Get(fullStoreURL)
	if err != nil {
		c.Logger.Printf("[INITIAL_SYNC] Failed to request full store from %s: %v", peer.NodeID, err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.Logger.Printf("[INITIAL_SYNC] Peer %s returned %s", peer.NodeID, resp.Status)
		return false
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.Logger.Printf("[INITIAL_SYNC] Failed to read response from %s: %v", peer.NodeID, err)
		return false
	}

	// Parse and apply the full store
	var peerData []frogpond.DataPoint
	if err := json.Unmarshal(body, &peerData); err != nil {
		c.Logger.Printf("[INITIAL_SYNC] Failed to parse data from %s: %v", peer.NodeID, err)
		return false
	}

	// Apply the peer's data and get any resulting updates
	resultingUpdates := c.frogpond.AppendDataPoints(peerData)
	c.sendUpdatesToPeers(resultingUpdates)

	c.Logger.Printf("[INITIAL_SYNC] Successfully synced %d data points from %s", len(peerData), peer.NodeID)
	return true
}
