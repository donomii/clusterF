// cluster.go - Self-organizing P2P storage cluster
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/donomii/clusterF/discovery"
	"github.com/donomii/clusterF/filesync"
	"github.com/donomii/clusterF/filesystem"
	"github.com/donomii/clusterF/frontend"
	"github.com/donomii/clusterF/httpclient"
	"github.com/donomii/clusterF/indexer"
	"github.com/donomii/clusterF/metrics"
	"github.com/donomii/clusterF/partitionmanager"
	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/threadmanager"
	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
	"github.com/donomii/frogpond"
)

const (
	DefaultBroadcastPort = 9999
	DefaultDataDir       = "./data"
	DefaultRF            = 3 // Default Replication Factor
)

type Metadata struct {
	NodeID    types.NodeID `json:"node_id"`
	Timestamp int64        `json:"timestamp"`
}

func (c *Cluster) DiscoveryManager() types.DiscoveryManagerLike {
	return c.discoveryManager
}

func (c *Cluster) ThreadManager() *threadmanager.ThreadManager {
	return c.threadManager
}

func (c *Cluster) Exporter() types.ExporterLike {
	return c.filesync
}

func (c *Cluster) ID() types.NodeID {
	return c.NodeId
}

type GossipMessage struct {
	From     types.NodeID `json:"from"`
	Metadata Metadata     `json:"metadata"`
}

// Cluster encapsulates one node's entire state & goroutines

type Cluster struct {
	// Identity & config
	NodeId        types.NodeID
	DataDir       string
	HTTPDataPort  int    // per-node HTTP data port
	DiscoveryPort int    // shared UDP announcement port
	BroadcastIP   net.IP // usually net.IPv4bcast
	logger        *log.Logger
	Debug         bool
	noStore       bool // client mode: don't store partitions locally

	// Discovery manager
	discoveryManager types.DiscoveryManagerLike
	threadManager    *threadmanager.ThreadManager

	// CRDT coordination layer
	frogpond *frogpond.Node

	// Partition system
	partitionManager *partitionmanager.PartitionManager

	// File system layer
	FileSystem *filesystem.ClusterFileSystem

	// File indexer for fast searching
	indexer types.IndexerLike

	// HTTP clients for reuse (prevents goroutine leaks)
	httpClient     *http.Client // short-lived control traffic
	HttpDataClient *http.Client // long-running data transfers

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
	ExportDir  string
	ClusterDir string // Optional cluster path prefix to export (e.g., "/photos")
	ImportDir  string // Optional local directory to import files from
	filesync   types.ExporterLike

	// Optional transcoder for media files
	Transcoder *Transcoder

	// Peer addresses
	peerAddrs *syncmap.SyncMap[types.NodeID, *types.PeerInfo]

	// Current file being processed (for monitoring)
	currentFile atomic.Value // stores string

	partitionReIndexInterval time.Duration // time in between scans for partitions to re-index

	diskActiveUntil atomic.Int64
	lastDiskMetrics atomic.Value

	shuttingDown atomic.Bool
	startTime    time.Time
}

func (c *Cluster) SetTimings(partitionSyncInterval, partitionReIndexInterval time.Duration) {
	c.partitionReIndexInterval = partitionReIndexInterval
	c.SetPartitionSyncInterval(int(partitionSyncInterval.Seconds()))
}

func (c *Cluster) Logger() *log.Logger {
	return c.logger
}

func (c *Cluster) NoStore() bool {
	return c.noStore
}
func (c *Cluster) PartitionManager() types.PartitionManagerLike {
	return c.partitionManager
}

func (c *Cluster) ReplicationFactor() int {
	return c.getCurrentRF()
}

type StorageSettings struct {
	Program             string `json:"program"`
	URL                 string `json:"url"`
	StorageMajor        string `json:"storage_major"`
	StorageMinor        string `json:"storage_minor"`
	Version             string `json:"version"`
	EncryptedTestPhrase string `json:"encrypted_test_phrase,omitempty"`
}

// loadStorageSettings loads storage settings from settings.json in the data directory
func loadStorageSettings(dataDir string) (*StorageSettings, error) {
	settingsPath := filepath.Join(dataDir, "settings.json")
	data, err := os.ReadFile(settingsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var settings StorageSettings
	if err := json.Unmarshal(data, &settings); err != nil {
		return nil, err
	}
	return &settings, nil
}

// saveStorageSettings saves storage settings to settings.json in the data directory
func saveStorageSettings(dataDir string, settings StorageSettings) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	settingsPath := filepath.Join(dataDir, "settings.json")
	data, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(settingsPath, data, 0o644)
}

type ClusterOpts struct {
	ID            string
	DataDir       string
	UDPListenPort int
	HTTPDataPort  int

	DiscoveryPort int
	BroadcastIP   net.IP
	Logger        *log.Logger //  if not provided, will use log.New(os.Stderr, "", log.Lstd)
	ExportDir     string      // if set, mirror files to this directory for OS sharing
	ClusterDir    string      // if set, only export files with this path prefix
	ImportDir     string      // if set, import files from this directory to the cluster
	ExcludeDirs   string      // comma-separated list of directory names to exclude during import
	NoStore       bool        // if true, don't store partitions locally (client mode)
	StorageMajor  string      // storage format major (ensemble or bolt)
	StorageMinor  string      // storage format minor (ensemble or bolt)
	EncryptionKey string      // encryption key for at-rest encryption
	Debug         bool        // Enable debugging for all modules in the cluster
}

func NewCluster(opts ClusterOpts) *Cluster {
	// Enable allocation tracking for profiling
	runtime.MemProfileRate = 4096 // Default memory profiling rate

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
		NodeId:        types.NodeID(id),
		DataDir:       opts.DataDir,
		HTTPDataPort:  opts.HTTPDataPort,
		DiscoveryPort: opts.DiscoveryPort,
		BroadcastIP:   opts.BroadcastIP,
		logger:        opts.Logger,
		ExportDir:     opts.ExportDir,
		ClusterDir:    opts.ClusterDir,
		ImportDir:     opts.ImportDir,
		noStore:       opts.NoStore,
		peerAddrs:     syncmap.NewSyncMap[types.NodeID, *types.PeerInfo](),

		fileListSubs: map[chan struct{}]bool{},

		ctx:    ctx,
		cancel: cancel,
		Debug:  opts.Debug,
	}
	c.debugf("Initialized cluster struct\n")
	c.lastDiskMetrics.Store(diskMetricsSnapshot{})
	c.markDiskActive()
	c.startTime = time.Now()

	// Treat DataDir as a base directory; always place data under a per-node subdir
	{
		base := opts.DataDir
		if base == "" {
			base = DefaultDataDir
		}
		opts.DataDir = filepath.Join(base, id)
	}

	// Set default storage options if not specified
	if opts.StorageMajor == "" {
		opts.StorageMajor = "extent"
	}
	if opts.StorageMinor == "" {
		opts.StorageMinor = ""
	}

	// Load existing settings if present
	existingSettings, err := loadStorageSettings(opts.DataDir)
	if err != nil {
		if opts.Logger != nil {
			opts.Logger.Fatalf("Failed to load storage settings from %s: %v", opts.DataDir, err)
		} else {
			log.Fatalf("Failed to load storage settings from %s: %v", opts.DataDir, err)
		}
	}

	// Validate settings or create new ones
	if existingSettings != nil {
		// Settings file exists, validate that command line options match
		if opts.StorageMajor != existingSettings.StorageMajor {
			if opts.Logger != nil {
				opts.Logger.Printf("[WARNING] Ignoring --storage-major=%s, using existing setting: %s", opts.StorageMajor, existingSettings.StorageMajor)
			} else {
				log.Printf("[WARNING] Ignoring --storage-major=%s, using existing setting: %s", opts.StorageMajor, existingSettings.StorageMajor)
			}
			opts.StorageMajor = existingSettings.StorageMajor
		}
		if opts.StorageMinor != existingSettings.StorageMinor {
			if opts.Logger != nil {
				opts.Logger.Printf("[WARNING] Ignoring --storage-minor=%s, using existing setting: %s", opts.StorageMinor, existingSettings.StorageMinor)
			} else {
				log.Printf("[WARNING] Ignoring --storage-minor=%s, using existing setting: %s", opts.StorageMinor, existingSettings.StorageMinor)
			}
			opts.StorageMinor = existingSettings.StorageMinor
		}
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
	c.threadManager = threadmanager.NewThreadManager(id, opts.Logger)
	if c.threadManager == nil {
		log.Fatalf("Failed to create thread manager")
	}
	c.debugf("Initialized thread manager\n")

	// Initialize metrics collector
	metricsCollector := metrics.NewMetricsCollector(c.publishMetricsDataPoint, c.threadManager, string(c.NodeId))
	if metricsCollector == nil {
		log.Fatalf("Failed to create metrics collector")
	}
	metrics.SetGlobalCollector(metricsCollector)
	c.debugf("Initialized metrics collector\n")

	// Create HTTP clients with connection pooling and differentiated timeouts
	controlTransport := &http.Transport{
		MaxIdleConns:        50, // Limit idle connections
		MaxIdleConnsPerHost: 5,  // Limit per-host connections
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
		DisableKeepAlives:   false,
	}
	c.httpClient = &http.Client{
		Timeout:   30 * time.Second,
		Transport: controlTransport,
	}
	dataTransport := &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 5,
		IdleConnTimeout:     2 * time.Minute,
		TLSHandshakeTimeout: 10 * time.Second,
		DisableKeepAlives:   false,
	}
	c.HttpDataClient = &http.Client{
		// No hard deadline for large transfers; rely on transport- and context-level cancellation.
		Timeout:   0,
		Transport: dataTransport,
	}
	c.debugf("Initialized HTTP clients\n")

	// Initialize discovery manager
	c.discoveryManager = discovery.NewDiscoveryManager(id, opts.HTTPDataPort, opts.DiscoveryPort, c.threadManager, opts.Logger)
	c.debugf("Initialized discovery manager\n")

	// Initialize frogpond CRDT node
	c.frogpond = frogpond.NewNode()
	c.debugf("Initialized frogpond node\n")

	// Save settings only after successful KV store initialization
	if existingSettings == nil {
		newSettings := StorageSettings{
			Program:      "clusterF",
			URL:          "https://github.com/donomii/clusterF",
			StorageMajor: opts.StorageMajor,
			StorageMinor: opts.StorageMinor,
			Version:      version,
		}
		if err := saveStorageSettings(opts.DataDir, newSettings); err != nil {
			c.Logger().Printf("[WARNING] Failed to save storage settings: %v", err)
		}
	}
	// Load CRDT state from KV (if any) before applying defaults
	c.loadCRDTFromFile()
	c.debugf("Loaded CRDT state from KV\n")

	// Initialize file store based on storage major selection
	var fileStore types.FileStoreLike
	switch opts.StorageMajor {
	case "rawfile":
		fileStore = partitionmanager.NewDiskFileStore(filepath.Join(opts.DataDir, "rawfiles"))
	default:
		fileStore = partitionmanager.NewFileStore(filepath.Join(opts.DataDir, "partitions"), c.Debug, opts.StorageMajor, opts.StorageMinor)
	}

	// Handle encryption if key provided
	if opts.EncryptionKey != "" {
		encKey := []byte(opts.EncryptionKey)
		fileStore.SetEncryptionKey(encKey)

		// Check if this is a new repository
		if existingSettings == nil || existingSettings.EncryptedTestPhrase == "" {
			// New repository - encrypt and store test phrase
			testPhrase := "clusterF-encryption-test"
			encryptedTest := xorEncryptString(testPhrase, encKey)
			newSettings := StorageSettings{
				Program:             "clusterF",
				URL:                 "https://github.com/donomii/clusterF",
				StorageMajor:        opts.StorageMajor,
				StorageMinor:        opts.StorageMinor,
				Version:             version,
				EncryptedTestPhrase: encryptedTest,
			}
			if err := saveStorageSettings(opts.DataDir, newSettings); err != nil {
				c.Logger().Fatalf("Failed to save encryption settings: %v", err)
			}
		} else {
			// Existing repository - verify key
			if err := verifyEncryptionKey(existingSettings.EncryptedTestPhrase, encKey); err != nil {
				panic(fmt.Sprintf("Encryption key verification failed: %v", err))
			}
		}
	} else if existingSettings != nil && existingSettings.EncryptedTestPhrase != "" {
		// Repository was created with encryption but no key provided
		panic("This repository requires an encryption key (use --encryption-key)")
	}

	// Initialize indexer first (needed by partition manager)
	idx := indexer.NewIndexer(c.Logger())
	idx.ConfigurePartitionMembership(c.frogpond, types.NodeID(c.NodeId), c.sendUpdatesToPeers, c.noStore)
	c.indexer = idx
	c.debugf("Initialized indexer\n")

	// Initialize partition manager
	deps := partitionmanager.Dependencies{
		NodeID:         types.NodeID(c.NodeId),
		NoStore:        c.noStore,
		Logger:         c.Logger(),
		Debugf:         c.debugf,
		FileStore:      fileStore,
		HttpDataClient: c.HttpDataClient,
		Discovery:      c.discoveryManager,
		Cluster:        c,
		LoadPeer: func(id types.NodeID) (*types.PeerInfo, bool) {
			// First try peerAddrs (from Discovery)
			peer, ok := c.peerAddrs.Load(types.NodeID(id))
			if ok && peer != nil {
				return &types.PeerInfo{
					NodeID:   peer.NodeID,
					Address:  peer.Address,
					HTTPPort: peer.HTTPPort,
				}, true
			}
			// Fallback: try CRDT nodes/ table
			nodeData := c.GetNodeInfo(id)
			if nodeData != nil && nodeData.Address != "" {
				return &types.PeerInfo{
					NodeID:   types.NodeID(nodeData.NodeID),
					Address:  nodeData.Address,
					HTTPPort: nodeData.HTTPPort,
				}, true
			}
			return nil, false
		},
		Frogpond:           c.frogpond,
		SendUpdatesToPeers: c.sendUpdatesToPeers,
		GetCurrentRF:       c.getCurrentRF,
		Indexer:            c.indexer,
	}
	c.partitionManager = partitionmanager.NewPartitionManager(deps)
	c.debugf("Initialized partition manager\n")

	// Initialize file system
	c.FileSystem = filesystem.NewClusterFileSystem(c, c.Debug)
	c.debugf("Initialized file system\n")

	// Initialize filesync if configured
	if opts.ExportDir != "" {
		if opts.ClusterDir != "" {
			c.Logger().Printf("[EXPORT] Mirroring files with prefix %s to %s for OS sharing", opts.ClusterDir, opts.ExportDir)
		} else {
			fmt.Println("Cannot use --export-dir without --cluster-dir")
			os.Exit(1)
		}
	}
	if opts.ImportDir != "" {
		if opts.ClusterDir == "" {
			fmt.Println("Cannot use --import-dir without --cluster-dir")
			os.Exit(1)
		}
		c.Logger().Printf("[IMPORT] Importing files from %s to cluster prefix %s", opts.ImportDir, opts.ClusterDir)
	}
	if opts.ExportDir != "" || opts.ImportDir != "" {
		if fs, err := filesync.NewFileSyncer(opts.ExportDir, opts.ImportDir, opts.ClusterDir, opts.ExcludeDirs, c.Logger(), c.FileSystem); err != nil {
			c.Logger().Printf("[FILESYNC] Failed to init filesync: %v", err)
		} else {
			fs.SetCurrentFileCallback(func(path string) {
				c.currentFile.Store(path)
			})
			c.filesync = fs
		}
	}
	c.debugf("Initialized filesync (if configured)\n")

	// Initialize transcoder
	transcodeDir := filepath.Join(opts.DataDir, "transcode_cache")
	maxCacheSize := int64(1024 * 1024 * 1024) // 1GB cache
	c.Transcoder = NewTranscoder(transcodeDir, maxCacheSize, c.Logger())
	if c.Transcoder.checkFFmpegAvailable() {
		c.Logger().Printf("[TRANSCODE] ffmpeg available - transcoding enabled")
	} else {
		c.Logger().Printf("[TRANSCODE] ffmpeg not available - transcoding disabled")
	}
	c.debugf("Initialized transcoder\n")

	// Initialize cluster settings (replication factor)
	c.initializeClusterSettings()
	c.debugf("Initialized cluster settings\n")

	// Store our node metadata in frogpond
	c.updateNodeMetadata()
	c.debugf("Stored initial node metadata in frogpond\n")

	// Store node ID in data directory for future reference
	if err := storeNodeIDInDataDir(opts.DataDir, id); err != nil {
		c.Logger().Printf("[WARNING] Failed to store node ID in data directory: %v", err)
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
	_ = c.Logger().Output(2, msg)
}

// panicf logs a debug message and then panics
func (c *Cluster) panicf(format string, v ...interface{}) {
	// Use Logger.Output with a call depth so the log shows the
	// caller of debugf (file:line), not this wrapper function.
	// calldepth=2: Output -> debugf -> caller
	msg := fmt.Sprintf(format, v...)
	_ = c.Logger().Output(2, msg)
	panic(msg)
}

func broadcastPortFromEnv() int {
	if v := os.Getenv("CLUSTER_BCAST_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 && p < 65536 {
			return p
		}
	}
	return DefaultBroadcastPort
}

// xorEncryptString performs XOR encryption on a string and returns hex-encoded result
// If input looks like hex, it decodes it first (for decryption)
func xorEncryptString(data string, key []byte) string {
	if len(key) == 0 || len(data) == 0 {
		return data
	}

	// Check if input is hex-encoded (all hex chars and even length)
	dataBytes := []byte(data)
	if decoded, err := hex.DecodeString(data); err == nil {
		// Input was hex, use decoded bytes
		dataBytes = decoded
	}

	result := make([]byte, len(dataBytes))
	for i := range dataBytes {
		result[i] = dataBytes[i] ^ key[i%len(key)]
	}

	// If original input was hex, return string; otherwise return hex
	if _, err := hex.DecodeString(data); err == nil {
		return string(result)
	}
	return hex.EncodeToString(result)
}

// verifyEncryptionKey verifies that the provided key can decrypt the test phrase
func verifyEncryptionKey(encryptedTestPhrase string, key []byte) error {
	testPhrase := "clusterF-encryption-test"
	decryptedTest := xorEncryptString(encryptedTestPhrase, key)
	if decryptedTest != testPhrase {
		return fmt.Errorf("Encryption key verification failed - wrong key or corrupted settings")
	}
	return nil
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

func (c *Cluster) DataClient() *http.Client {
	return c.HttpDataClient
}

// ---------- Lifecycle ----------

func (c *Cluster) Start() {
	c.Logger().Printf("Starting node %s (HTTP:%d)", c.NodeId, c.HTTPDataPort)

	// Start all threads using ThreadManager
	c.threadManager.StartThreadOnce("full-startup-reindex", c.runFullStartupReindex)
	c.threadManager.StartThreadOnce("indexer-import", c.runIndexerImport)
	c.threadManager.StartThread("filesync", c.runFilesync)
	c.threadManager.StartThread("frogpond-sync", c.periodicFrogpondSync)
	c.threadManager.StartThread("partition-check", c.partitionManager.PeriodicPartitionCheck)
	c.threadManager.StartThread("node-pruning", c.periodicNodePruning)
	c.threadManager.StartThread("discovery-manager", c.runDiscoveryManager)
	c.threadManager.StartThread("peer-fullstore-sync", c.runPeerFullStoreSync)
	c.threadManager.StartThread("http-server", c.startHTTPServer)
	c.threadManager.StartThread("partition-reindex", c.runPartitionReindex)
	c.threadManager.StartThread("restart-monitor", c.runRestartMonitor)
	c.debugf("Started all threads")
}

func (c *Cluster) runDiscoveryManager(ctx context.Context) {
	if c.discoveryManager == nil {
		return
	}
	c.Logger().Printf("Starting discovery manager")
	if err := c.discoveryManager.Start(); err != nil {
		c.Logger().Printf("Discovery manager failed to start: %v", err)
		return
	}
	<-ctx.Done()
	c.Logger().Printf("Stopping discovery manager")
	c.discoveryManager.Stop()
}

func (c *Cluster) runPeerFullStoreSync(ctx context.Context) {
	if c.discoveryManager == nil {
		return
	}

	knownPeers := make(map[types.NodeID]bool)

	checkPeers := func() {
		for _, peer := range c.discoveryManager.GetPeers() {
			if peer == nil {
				continue
			}
			peerID := types.NodeID(peer.NodeID)
			if seen := knownPeers[peerID]; seen {
				continue
			}
			if c.requestFullStoreFromPeer(peer) {
				knownPeers[peerID] = true
			}
		}
	}

	checkPeers()

	ticker := time.NewTicker(time.Duration(c.GetPartitionSyncInterval()) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.logger.Printf("Started requestFullStoreFromPeer\n")
			checkPeers()
			c.logger.Printf("Finished requestFullStoreFromPeer\n")
			ticker.Reset(time.Duration(c.GetPartitionSyncInterval()) * time.Second)
		}
	}
}

// FullSyncAllPeers requests a full store sync from every currently discovered peer.
// Primarily used by tests to force deterministic synchronization.
func (c *Cluster) FullSyncAllPeers() {
	if c.discoveryManager == nil {
		return
	}
	for _, peer := range c.discoveryManager.GetPeers() {
		if peer == nil {
			continue
		}
		c.requestFullStoreFromPeer(peer)
	}
}

// runFilesync integrates the FileSyncer with the cluster lifecycle
func (c *Cluster) runFilesync(ctx context.Context) {
	if c.filesync == nil {
		return
	}
	c.Logger().Printf("[FILESYNC] Starting file synchronization")
	c.filesync.Run(ctx)
	// FIXME: stop threadmanager constantly restarting this, set a configurable retry time
	c.Logger().Printf("[FILESYNC] File synchronization stopped")
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(24 * time.Hour):
			c.Logger().Printf("[FILESYNC] File sync thread exiting; restarting")
		}
	}
}

func (c *Cluster) runFullStartupReindex(ctx context.Context) {
	c.logger.Printf("Started runFullStartupReindex\n")
	c.partitionManager.RunFullReindexAtStartup(ctx)
	c.logger.Printf("Finished runFullStartupReindex\n")
	<-ctx.Done()

}

func (c *Cluster) runIndexerImport(ctx context.Context) {
	c.logger.Printf("Started runIndexerImport\n")
	if err := c.indexer.ImportFilestore(ctx, c.partitionManager); err != nil {
		c.Logger().Printf("[WARNING] Failed to import filestore into indexer: %v", err)
	}
	c.logger.Printf("Finished runIndexerImport\n")
	<-ctx.Done()
}

func (c *Cluster) runPartitionReindex(ctx context.Context) {
	pm := c.PartitionManager()
	pm.RunReindex(ctx)
	if c.partitionReIndexInterval.Seconds() == 0 {
		c.partitionReIndexInterval = 500 * time.Second
	}
	ticker := time.NewTicker(c.partitionReIndexInterval)

	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.logger.Printf("Started RunReindex\n")
			pm.RunReindex(ctx)
			c.logger.Printf("Finished RunReindex\n")
			ticker.Reset(c.partitionReIndexInterval)
		}
	}
}

func (c *Cluster) AppContext() context.Context {
	return c.ctx
}

func (c *Cluster) Stop() {
	c.Logger().Printf("Stopping node %s", c.NodeId)

	// Set shutdown flag immediately to block new API requests
	c.shuttingDown.Store(true)

	// Cancel context
	c.cancel()

	// Flush CRDT state to disk before shutting down threads
	c.debugf("Flushing CRDT state before thread shutdown")
	c.persistCRDTToFile()

	// Shutdown all threads via ThreadManager
	c.debugf("Shutting down all threads")
	failedThreads := c.threadManager.Shutdown()
	if len(failedThreads) > 0 {
		c.Logger().Printf("Some threads failed to shutdown: %v", failedThreads)
		c.debugf("Some threads failed to shutdown: %v", failedThreads)
	}

	// Flush CRDT state to disk after threads have stopped
	c.debugf("Flushing CRDT state after thread shutdown")
	c.persistCRDTToFile()

	// Close HTTP client transports to clean up connections
	if c.httpClient != nil && c.httpClient.Transport != nil {
		c.debugf("Closing HTTP client transport")
		if transport, ok := c.httpClient.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
	if c.HttpDataClient != nil && c.HttpDataClient.Transport != nil {
		c.debugf("Closing HTTP data client transport")
		if transport, ok := c.HttpDataClient.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}

	// Close FileStore handles
	if c.partitionManager != nil {
		c.debugf("Closing partition file store")
		c.partitionManager.FileStore().Close()
	}

	c.Logger().Printf("Node %s stopped", c.NodeId)
}

// ---------- Repair ----------

// corsMiddleware adds CORS headers to allow browser access and logs requests
func corsMiddleware(debug bool, logger *log.Logger, cluster *Cluster, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check if shutting down and block new API requests
		if cluster.shuttingDown.Load() {
			http.Error(w, "Node is shutting down", http.StatusServiceUnavailable)
			return
		}

		if debug {
			//logger.Printf("[HTTP] %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
		}
		// Panic recovery
		defer func() {
			if err := recover(); err != nil {
				log.Printf("[HTTP_PANIC] %s %s: %v", r.Method, r.URL.Path, err)
				// Print stack trace
				buf := make([]byte, 1<<16)
				n := runtime.Stack(buf, false)
				log.Printf("[HTTP_PANIC] Stack trace:\n%s", string(buf[:n]))
				// Return 500 Internal Server Error
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
	mux.HandleFunc("/", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleWelcome))
	mux.HandleFunc("/status", corsMiddleware(c.Debug, c.Logger(), c, c.handleStatus))
	// API reference page (exact path only to avoid clobbering other /api/* routes)
	mux.HandleFunc("/api", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleAPIDocs))
	mux.HandleFunc("/frogpond/update", corsMiddleware(c.Debug, c.Logger(), c, c.handleFrogpondUpdate))
	mux.HandleFunc("/frogpond/fullstore", corsMiddleware(c.Debug, c.Logger(), c, c.handleFrogpondFullStore))
	mux.HandleFunc("/api/replication-factor", corsMiddleware(c.Debug, c.Logger(), c, c.handleReplicationFactor))
	mux.HandleFunc("/api/partition-sync-interval", corsMiddleware(c.Debug, c.Logger(), c, c.handlePartitionSyncInterval))
	mux.HandleFunc("/flamegraph", corsMiddleware(c.Debug, c.Logger(), c, c.handleFlameGraph))
	mux.HandleFunc("/memorygraph", corsMiddleware(c.Debug, c.Logger(), c, c.handleMemoryFlameGraph))
	mux.HandleFunc("/allocgraph", corsMiddleware(c.Debug, c.Logger(), c, c.handleAllocFlameGraph))
	mux.HandleFunc("/profiling", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleProfilingPage))
	mux.HandleFunc("/profiling.js", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleProfilingJS))
	mux.HandleFunc("/api/profiling", corsMiddleware(c.Debug, c.Logger(), c, c.handleProfilingAPI))
	// Add pprof endpoints manually
	mux.HandleFunc("/debug/pprof/", corsMiddleware(c.Debug, c.Logger(), c, func(w http.ResponseWriter, r *http.Request) {
		pprof.Index(w, r)
	}))
	mux.HandleFunc("/debug/pprof/cmdline", corsMiddleware(c.Debug, c.Logger(), c, func(w http.ResponseWriter, r *http.Request) {
		pprof.Cmdline(w, r)
	}))
	mux.HandleFunc("/debug/pprof/profile", corsMiddleware(c.Debug, c.Logger(), c, func(w http.ResponseWriter, r *http.Request) {
		pprof.Profile(w, r)
	}))
	mux.HandleFunc("/debug/pprof/symbol", corsMiddleware(c.Debug, c.Logger(), c, func(w http.ResponseWriter, r *http.Request) {
		pprof.Symbol(w, r)
	}))
	mux.HandleFunc("/debug/pprof/trace", corsMiddleware(c.Debug, c.Logger(), c, func(w http.ResponseWriter, r *http.Request) {
		pprof.Trace(w, r)
	}))
	mux.HandleFunc("/debug/pprof/heap", corsMiddleware(c.Debug, c.Logger(), c, func(w http.ResponseWriter, r *http.Request) {
		pprof.Handler("heap").ServeHTTP(w, r)
	}))
	mux.HandleFunc("/debug/pprof/goroutine", corsMiddleware(c.Debug, c.Logger(), c, func(w http.ResponseWriter, r *http.Request) {
		pprof.Handler("goroutine").ServeHTTP(w, r)
	}))
	mux.HandleFunc("/debug/pprof/block", corsMiddleware(c.Debug, c.Logger(), c, func(w http.ResponseWriter, r *http.Request) {
		pprof.Handler("block").ServeHTTP(w, r)
	}))
	mux.HandleFunc("/debug/pprof/mutex", corsMiddleware(c.Debug, c.Logger(), c, func(w http.ResponseWriter, r *http.Request) {
		pprof.Handler("mutex").ServeHTTP(w, r)
	}))
	mux.HandleFunc("/monitor", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleMonitorDashboard))
	mux.HandleFunc("/monitor.js", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleMonitorJS))
	mux.HandleFunc("/metrics", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleMetricsPage))
	mux.HandleFunc("/metrics.js", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleMetricsJS))
	mux.HandleFunc("/api/cluster-stats", corsMiddleware(c.Debug, c.Logger(), c, c.handleClusterStats))
	mux.HandleFunc("/api/metrics", corsMiddleware(c.Debug, c.Logger(), c, c.handleMetricsAPI))
	mux.HandleFunc("/cluster-visualizer.html", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleVisualizer))
	// File system endpoints
	mux.HandleFunc("/files/", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleFiles))
	mux.HandleFunc("/loading", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleLoadingPage))
	// CRDT inspector UI + APIs
	mux.HandleFunc("/crdt", corsMiddleware(c.Debug, c.Logger(), c, ui.HandleCRDTInspectorPageUI))
	mux.HandleFunc("/api/crdt/list", corsMiddleware(c.Debug, c.Logger(), c, c.handleCRDTListAPI))
	mux.HandleFunc("/api/crdt/get", corsMiddleware(c.Debug, c.Logger(), c, c.handleCRDTGetAPI))
	mux.HandleFunc("/api/crdt/search", corsMiddleware(c.Debug, c.Logger(), c, c.handleCRDTSearchAPI))
	mux.HandleFunc("/api/files/", corsMiddleware(c.Debug, c.Logger(), c, c.handleFilesAPI))
	mux.HandleFunc("/api/metadata/", corsMiddleware(c.Debug, c.Logger(), c, c.handleMetadataAPI))
	// Internal API endpoints for peer-to-peer communication
	mux.HandleFunc("/internal/files/", corsMiddleware(c.Debug, c.Logger(), c, c.handleInternalFilesAPI))
	mux.HandleFunc("/internal/metadata/", corsMiddleware(c.Debug, c.Logger(), c, c.handleInternalMetadataAPI))
	mux.HandleFunc("/internal/search", corsMiddleware(c.Debug, c.Logger(), c, c.handleInternalSearchAPI))
	// Partition sync endpoints
	mux.HandleFunc("/api/partition-sync/", corsMiddleware(c.Debug, c.Logger(), c, c.handlePartitionSyncAPI))
	mux.HandleFunc("/api/partition-stats", corsMiddleware(c.Debug, c.Logger(), c, c.handlePartitionStats))
	// Integrity check endpoint
	mux.HandleFunc("/api/integrity-check", corsMiddleware(c.Debug, c.Logger(), c, c.handleIntegrityCheck))
	// Search API
	mux.HandleFunc("/api/search", corsMiddleware(c.Debug, c.Logger(), c, c.handleSearchAPI))
	// Transcode API
	mux.HandleFunc("/api/transcode/", corsMiddleware(c.Debug, c.Logger(), c, c.handleTranscodeAPI))
	mux.HandleFunc("/api/transcode-stats", corsMiddleware(c.Debug, c.Logger(), c, c.handleTranscodeStats))
	// Partition sync pause API
	mux.HandleFunc("/api/partition-sync-pause", corsMiddleware(c.Debug, c.Logger(), c, c.handlePartitionSyncPause))
	// Cluster restart API
	mux.HandleFunc("/api/cluster-restart", corsMiddleware(c.Debug, c.Logger(), c, c.handleClusterRestart))

	server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		// Leave WriteTimeout unset so long-running downloads/uploads do not get cut mid-transfer.
		WriteTimeout: 0,
		IdleTimeout:  15 * time.Second, // important for keep-alive churn
	}

	c.server = server
	c.debugf("HTTP data server listening on port %d (dir=%s)", port, c.DataDir)
	c.Logger().Printf("🐸 Node %s ready on http://localhost:%d (monitor: http://localhost:%d/monitor)", c.NodeId, port, port)

	// Start server in a goroutine so we can handle context cancellation
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Serve(listener)
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		c.Logger().Printf("HTTP server shutting down (context cancelled)")
		c.debugf("HTTP server shutting down (context cancelled)")
		// Graceful shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			c.Logger().Printf("HTTP server shutdown error: %v", err)
			c.debugf("HTTP server shutdown error: %v", err)
		}
	case err := <-serverDone:
		if err != nil && err != http.ErrServerClosed {
			c.Logger().Printf("HTTP server error: %v", err)
			c.debugf("HTTP server error: %v", err)
		}
	}
}

func (c *Cluster) handleStatus(w http.ResponseWriter, r *http.Request) {
	//fmt.Printf("Entering handleStatus\n")
	//defer func() { fmt.Printf("Leaving handleStatus\n") }()
	// Get partition stats without holding the main mutex
	var partitionStats types.PartitionStatistics
	if c.partitionManager != nil {
		//c.debugf("[STATUS] Getting partition stats")

		partitionStats = c.partitionManager.GetPartitionStats()

		//c.debugf("[STATUS] Got partition stats: %+v", partitionStats)
	} else {
		c.debugf("[STATUS] PartitionManager is nil, initialisation may have failed")
		partitionStats = types.PartitionStatistics{}
	}

	// Get replication factor safely
	var rf interface{} = DefaultRF
	if c.frogpond != nil {
		rf = c.getCurrentRF()
		//c.debugf("[STATUS] Got RF: %v", rf)
	} else {
		c.debugf("[STATUS] frogpond is nil, initialisation may have failed")
	}

	// Read basic fields without mutex - they're set once at startup
	var currentFile string
	if cf := c.currentFile.Load(); cf != nil {
		currentFile = cf.(string)
	}

	// Get absolute path for data directory
	absDataDir, _ := filepath.Abs(c.DataDir)

	status := types.NodeStatus{
		Node_id:            string(c.NodeId),
		Data_dir:           absDataDir,
		Http_port:          c.HTTPDataPort,
		Replication_factor: rf.(int),
		Partition_stats:    partitionStats,
		Current_file:       currentFile,
	}

	// Debug: log what we're sending
	//c.debugf("[STATUS] Returning status: node_id=%s, rf=%v, partition_stats=%+v", c.NodeId, status.Replication_factor, partitionStats)

	w.Header().Set("Content-Type", "application/json")
	//c.debugf("[STATUS] Set headers")

	if err := json.NewEncoder(w).Encode(status); err != nil {
		//c.debugf("[STATUS] JSON encode error: %v", err)
	} else {
		//c.debugf("[STATUS] Response sent successfully")
	}
}

// handleClusterStats provides cluster-wide statistics for monitoring
func (c *Cluster) handleClusterStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Get absolute path for data directory
	absDataDir, _ := filepath.Abs(c.DataDir)

	peerList := c.getPeerList()
	stats := types.NodeStatus{
		Node_id:            string(c.NodeId),
		Http_port:          c.HTTPDataPort,
		Discovery_port:     c.DiscoveryPort,
		Data_dir:           absDataDir,
		Timestamp:          time.Now(),
		Replication_factor: c.getCurrentRF(),
		Peer_list:          peerList,
	}

	// Debug: log what we're sending
	//c.debugf("[CLUSTER_STATS] Returning stats: node_id=%s, peer_count=%d, rf=%v", c.NodeId, len(peerList), stats.Replication_factor)

	json.NewEncoder(w).Encode(stats)
}

func (c *Cluster) handleMetricsAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	response := struct {
		GeneratedAt int64                     `json:"generated_at"`
		Snapshots   []metrics.MetricsSnapshot `json:"snapshots"`
	}{
		GeneratedAt: time.Now().Unix(),
	}

	if c.frogpond == nil {
		json.NewEncoder(w).Encode(response)
		return
	}

	dataPoints := c.frogpond.GetAllMatchingPrefix("metrics/")
	snapshots := make([]metrics.MetricsSnapshot, 0, len(dataPoints))

	for _, dp := range dataPoints {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}
		var snapshot metrics.MetricsSnapshot
		if err := json.Unmarshal(dp.Value, &snapshot); err != nil {
			c.debugf("[METRICS] Failed to decode metrics entry %s: %v", dp.Key, err)
			continue
		}
		if snapshot.NodeID == "" {
			key := string(dp.Key)
			snapshot.NodeID = strings.TrimPrefix(key, "metrics/")
		}
		snapshots = append(snapshots, snapshot)
	}

	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].NodeID == snapshots[j].NodeID {
			return snapshots[i].Timestamp > snapshots[j].Timestamp
		}
		return snapshots[i].NodeID < snapshots[j].NodeID
	})

	response.Snapshots = snapshots
	json.NewEncoder(w).Encode(response)
}

// ---------- Partition HTTP Handlers ----------

// handlePartitionSyncAPI serves partition data for peer synchronization
func (c *Cluster) handlePartitionSyncAPI(w http.ResponseWriter, r *http.Request) {
	// Extract partition ID from URL path
	path := strings.TrimPrefix(r.URL.Path, "/api/partition-sync/")
	if path == "" {
		http.Error(w, "missing partition ID", http.StatusBadRequest)
		return
	}

	partitionID := types.PartitionID(path)
	c.partitionManager.HandlePartitionSync(w, r, partitionID)
}

// handlePartitionStats returns partition statistics
func (c *Cluster) handlePartitionStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := c.partitionManager.GetPartitionStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// handleIntegrityCheck performs file integrity verification
func (c *Cluster) handleIntegrityCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.Logger().Printf("[INTEGRITY] Starting file integrity check")
	results := c.partitionManager.VerifyStoredFileIntegrity()
	c.Logger().Printf("[INTEGRITY] Completed file integrity check")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// handleMetadataAPI handles external metadata requests by calling internal handler
func (c *Cluster) handleMetadataAPI(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/metadata")
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.debugf("[METADATA_API] External GET request for path: %s - forwarding to internal handler", path)

	// Get partition info to find which nodes hold this file
	partitionID := c.PartitionManager().CalculatePartitionName(path)
	partitionInfo := c.PartitionManager().GetPartitionInfo(types.PartitionID(partitionID))

	if partitionInfo == nil {
		c.debugf("[METADATA_API] No partition info found for %s (partition %s)", path, partitionID)
		http.Error(w, fmt.Sprintf("File not found in cluster: %s (no partition info found for partition %s)", path, partitionID), http.StatusNotFound)
		return
	}

	if len(partitionInfo.Holders) == 0 {
		c.debugf("[METADATA_API] No holders registered for partition %s (file %s)", partitionID, path)
		http.Error(w, fmt.Sprintf("File not found in cluster: %s (partition %s has no holders registered)", path, partitionID), http.StatusNotFound)
		return
	}

	// Get peer info for all holders
	peers := c.DiscoveryManager().GetPeers()
	peerLookup := make(map[types.NodeID]*types.PeerInfo)
	for _, peer := range peers {
		peerLookup[types.NodeID(peer.NodeID)] = peer
	}

	c.debugf("[METADATA_API] Partition %s for file %s has holders: %v", partitionID, path, partitionInfo.Holders)

	// Try each holder until we find the metadata
	var holderErrors []string
	for _, holderID := range partitionInfo.Holders {
		c.debugf("[METADATA_API] Trying holder %s for file %s", holderID, path)

		// Get peer info for this holder
		var peer *types.PeerInfo
		if holderID == c.ID() {

			c.debugf("[METADATA_API] Fetching metadata from localhost via HTTP")
			peer = &types.PeerInfo{
				NodeID:   c.ID(),
				Address:  c.DiscoveryManager().GetLocalAddress(),
				HTTPPort: c.HTTPPort(),
			}
		} else if p, ok := peerLookup[holderID]; ok {
			c.debugf("Fetching metadata from peer %+v", p)
			peer = p

		} else if p, ok := peerLookup[holderID]; ok {
			c.debugf("Fetching metadata from peer %+v", p)
			peer = p
		} else {
			c.debugf("[METADATA_API] No peer info available for holder %s", holderID)
			holderErrors = append(holderErrors, fmt.Sprintf("%s: no peer info", holderID))
			continue
		}

		// Build URL for this peer's internal metadata endpoint
		metadataURL, err := urlutil.BuildInternalMetadataURL(peer.Address, peer.HTTPPort, path)
		if err != nil {
			c.debugf("[METADATA_API] Failed to build URL for peer %s: %v", peer.NodeID, err)
			holderErrors = append(holderErrors, fmt.Sprintf("%s: URL build failed: %v", peer.NodeID, err))
			continue
		}

		body, headers, status, err := httpclient.SimpleGet(r.Context(), c.HttpDataClient, metadataURL,
			httpclient.WithHeader("X-ClusterF-Internal", "1"),
		)
		if err != nil {
			c.debugf("[METADATA_API] Failed to get metadata from peer %s: %v", peer.NodeID, err)
			holderErrors = append(holderErrors, fmt.Sprintf("%s: HTTP request failed: %v", peer.NodeID, err))
			continue
		}

		if status == http.StatusOK {
			c.debugf("[METADATA_API] Found metadata for %s on holder %s", path, peer.NodeID)

			// Copy all headers from peer response
			for key, values := range headers {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}

			w.WriteHeader(http.StatusOK)
			if _, err := w.Write(body); err != nil {
				c.debugf("[METADATA_API] Failed streaming metadata from %s: %v", peer.NodeID, err)
			}
			return
		}

		// Read error response body for more detailed error information
		msg := strings.TrimSpace(string(body))
		holderErrors = append(holderErrors, strings.TrimSpace(fmt.Sprintf("%s: %d %s", peer.NodeID, status, msg)))
		c.debugf("[METADATA_API] Holder %s returned %d for metadata %s: %s", peer.NodeID, status, path, msg)
	}

	// Metadata not found on any registered holder
	c.debugf("[METADATA_API] Metadata for %s not found on any registered holder for partition %s", path, partitionID)
	errorSummary := fmt.Sprintf("Metadata not found in cluster: %s", path)
	if len(holderErrors) > 0 {
		errorSummary += fmt.Sprintf(" (tried holders: %s)", strings.Join(holderErrors, ", "))
	}
	http.Error(w, errorSummary, http.StatusNotFound)
}

// handleInternalMetadataAPI handles internal peer-to-peer metadata requests
// Only queries local storage, never forwards to other peers
func (c *Cluster) handleInternalMetadataAPI(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/internal/metadata")
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.debugf("[INTERNAL_METADATA_API] GET request for path: %s", path)

	// Only check local storage, never forward to peers
	metadata, err := c.FileSystem.GetMetadata(path)
	if err != nil {
		c.debugf("[INTERNAL_METADATA_API] Not found locally: %v", path)
		if errors.Is(err, types.ErrFileNotFound) {
			http.Error(w, "Metadata not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to retrieve metadata: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(metadata)
}

// ---------- Utilities ----------

// FIXME obviously this isn't going to be seret on a cold start...
// initializeClusterSettings sets up default cluster settings if not already present
func (c *Cluster) initializeClusterSettings() {
	// Initialize replication factor if not set
	data := c.frogpond.GetDataPoint("cluster/replication_factor")
	if data.Deleted || len(data.Value) == 0 {
		rfJSON, _ := json.Marshal(DefaultRF)
		updates := c.frogpond.SetDataPoint("cluster/replication_factor", rfJSON)
		c.sendUpdatesToPeers(updates)
		c.Logger().Printf("[INIT] Set default replication factor to %d", DefaultRF)
	}

	// Initialize partition sync interval if not set
	data = c.frogpond.GetDataPoint("cluster/partition_sync_interval_seconds")
	if data.Deleted || len(data.Value) == 0 {
		intervalJSON, _ := json.Marshal(types.DefaultPartitionSyncIntervalSeconds)
		updates := c.frogpond.SetDataPoint("cluster/partition_sync_interval_seconds", intervalJSON)
		c.sendUpdatesToPeers(updates)
		c.Logger().Printf("[INIT] Set default partition sync interval to %d seconds", types.DefaultPartitionSyncIntervalSeconds)
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

// handlePartitionSyncInterval handles GET/PUT requests for partition sync interval seconds
func (c *Cluster) handlePartitionSyncInterval(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		interval := c.GetPartitionSyncInterval()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"partition_sync_interval_seconds": interval,
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

		value, ok := request["partition_sync_interval_seconds"]
		if !ok {
			http.Error(w, "Missing partition_sync_interval_seconds field", http.StatusBadRequest)
			return
		}

		intervalFloat, ok := value.(float64)
		if !ok {
			http.Error(w, "partition_sync_interval_seconds must be a number", http.StatusBadRequest)
			return
		}

		if intervalFloat < 1 {
			http.Error(w, "partition_sync_interval_seconds must be at least 1", http.StatusBadRequest)
			return
		}

		c.SetPartitionSyncInterval(int(intervalFloat))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":                         true,
			"partition_sync_interval_seconds": int(intervalFloat),
		})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePartitionSyncPause handles GET/PUT requests for partition sync pause state
func (c *Cluster) handlePartitionSyncPause(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		paused := c.GetPartitionSyncPaused()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"paused": paused,
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

		pausedValue, ok := request["paused"]
		if !ok {
			http.Error(w, "Missing paused field", http.StatusBadRequest)
			return
		}

		paused, ok := pausedValue.(bool)
		if !ok {
			http.Error(w, "paused must be a boolean", http.StatusBadRequest)
			return
		}

		c.setPartitionSyncPaused(paused)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"paused":  paused,
		})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleClusterRestart handles POST requests to restart the cluster
func (c *Cluster) handleClusterRestart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.setClusterRestart()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Cluster restart initiated",
	})
}

// requestFullStoreFromPeer requests the complete frogpond store from a specific peer
// Returns true on success, false on failure
func (c *Cluster) requestFullStoreFromPeer(peer *types.PeerInfo) bool {
	fullStoreURL, err := urlutil.BuildHTTPURL(peer.Address, peer.HTTPPort, "/frogpond/fullstore")
	if err != nil {
		c.Logger().Printf("[FULL_SYNC] Failed to build full store URL for %s: %v", peer.NodeID, err)
		return false
	}

	body, _, status, err := httpclient.SimpleGet(context.Background(), c.HttpDataClient, fullStoreURL)
	if err != nil {
		c.Logger().Printf("[FULL_SYNC] Failed to request full store from %s: %v", peer.NodeID, err)
		return false
	}

	if status != http.StatusOK {
		c.Logger().Printf("[FULL_SYNC] Peer %s returned status %d", peer.NodeID, status)
		return false
	}

	// Parse and apply the full store
	var peerData []frogpond.DataPoint
	if err := json.Unmarshal(body, &peerData); err != nil {
		c.Logger().Printf("[FULL_SYNC] Failed to parse data from %s: %v", peer.NodeID, err)
		return false
	}

	// Apply the peer's data and get any resulting updates
	resultingUpdates := c.frogpond.AppendDataPoints(peerData)
	c.sendUpdatesToPeers(resultingUpdates)

	c.Logger().Printf("[FULL_SYNC] Successfully synced %d data points from %s", len(peerData), peer.NodeID)
	return true
}

// initialPartitionMetadataUpdate updates metadata for all local partitions after CRDT sync
func (c *Cluster) initialPartitionMetadataUpdate(ctx context.Context) {
	c.Logger().Printf("[INITIAL_METADATA] Starting initial partition metadata update for all local partitions")
	c.partitionManager.UpdateAllLocalPartitionsMetadata(ctx)
	c.Logger().Printf("[INITIAL_METADATA] Completed initial partition metadata update")
}

// runRestartMonitor monitors tasks/restart in CRDT and restarts if needed
func (c *Cluster) runRestartMonitor(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.Logger().Printf("[DEBUG] runRestartMonitor context cancelled")
			return
		case <-ticker.C:

			dp := c.frogpond.GetDataPoint("tasks/restart")

			if dp.Deleted || len(dp.Value) == 0 {

				continue
			}

			var restartTime int64
			if err := json.Unmarshal(dp.Value, &restartTime); err != nil {

				continue
			}

			if c.startTime.Unix() < restartTime {
				c.Logger().Printf("[DEBUG] Restart requested, restarting node (start: %d < restart: %d)", c.startTime.Unix(), restartTime)

				// Get the current executable path
				executable, err := os.Executable()
				if err != nil {
					c.Logger().Printf("[DEBUG] Failed to get executable path: %v", err)
					os.Exit(1)
					return
				}

				// Get current command line arguments
				args := os.Args[1:] // Skip the program name
				c.Logger().Printf("[DEBUG] Restarting with executable: %s, args: %v", executable, args)

				// Replace current process with new instance
				err = syscall.Exec(executable, append([]string{executable}, args...), os.Environ())
				if err != nil {
					c.Logger().Printf("[DEBUG] Failed to exec restart: %v", err)
					os.Exit(1)
				}
			} else {

			}
		}
	}
}
