// main.go - Simple cluster node launcher
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/donomii/clusterF/webdav"
)

// Populated via -ldflags during build
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	// Parse command line flags
	simNodes := flag.Int("sim-nodes", 0, "Run in simulation mode with N nodes")
	basePort := flag.Int("base-port", 30000, "Base port for simulation nodes")
	discoveryPort := flag.Int("discovery-port", 9999, "Discovery port for all nodes")
	dataDir := flag.String("data-dir", "", "Base data directory for simulation")
	nodeID := flag.String("node-id", "", "Node ID (if not specified, will be loaded from or generated for the data directory)")
	noDesktop := flag.Bool("no-desktop", false, "Do not open the desktop drop window")
	mountPoint := flag.String("mount", "", "[DISABLED] FUSE mounting not supported")
	exportDir := flag.String("export-dir", "", "Mirror cluster files to this local directory (share via macOS File Sharing for SMB)")
	clusterDir := flag.String("cluster-dir", "", "Cluster path prefix to export (must be used with --export-dir)")
	importDir := flag.String("import-dir", "", "Import files from this local directory into the cluster (must be used with --cluster-dir)")
	excludeDirs := flag.String("exclude-dirs", "", "Comma-separated list of directory names to exclude during import")
	webdavDir := flag.String("webdav", "", "Serve cluster path prefix over WebDAV (e.g., '/photos')")
	httpPort := flag.Int("http-port", 0, "HTTP port to bind (0 = dynamic near 30000)")
	debug := flag.Bool("debug", false, "Enable verbose debug logging")
	noStore := flag.Bool("no-store", false, "Client mode: participate in CRDT but don't store partitions locally")
	profiling := flag.Bool("profiling", false, "Enable profiling immediately at startup")
	storageMajor := flag.String("storage-major", "mmapsingle", "Storage format major (ensemble or bolt)")
	storageMinor := flag.String("storage-minor", "", "Storage format minor (ensemble or bolt)")
	encryptionKey := flag.String("encryption-key", "", "Encryption key for at-rest encryption (required at init, then at every start)")
	showVersion := flag.Bool("version", false, "Print version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("clusterF %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	if *simNodes > 0 {
		runSimulation(*simNodes, *basePort, *discoveryPort, *dataDir, *profiling)
	} else {
		// Validate export options
		if *exportDir != "" && *clusterDir == "" {
			log.Fatal("Both --export-dir and --cluster-dir must be specified together, or neither")
		}

		// Validate import options
		if *importDir != "" && *clusterDir == "" {
			log.Fatal("--import-dir requires --cluster-dir to be specified")
		}

		// Validate exclude directories exist
		if *excludeDirs != "" {
			for _, dir := range strings.Split(*excludeDirs, ",") {
				dir = strings.TrimSpace(dir)
				if dir != "" {
					absDir, err := filepath.Abs(dir)
					if err != nil {
						log.Fatalf("Could not resolve exclude path %s: %v", dir, err)
					}
					if _, err := os.Stat(absDir); err != nil {
						log.Fatalf("Exclude directory does not exist: %s", absDir)
					}
				}
			}
		}

		runSingleNode(*noDesktop, *mountPoint, *exportDir, *clusterDir, *importDir, *excludeDirs, *webdavDir, *nodeID, *dataDir, *httpPort, *debug, *noStore, *profiling, *storageMajor, *storageMinor, *encryptionKey)
	}
}

// hasGraphicsEnvironment checks if a graphics environment is available
func hasGraphicsEnvironment() bool {
	switch runtime.GOOS {
	case "linux":
		// Check for X11 or Wayland
		if os.Getenv("DISPLAY") != "" || os.Getenv("WAYLAND_DISPLAY") != "" {
			return true
		}
		return false
	case "darwin":
		// macOS should always have graphics in typical use
		return true
	case "windows":
		// Windows should always have graphics in typical use
		return true
	default:
		// Conservative: assume no graphics for unknown platforms
		return false
	}
}

// runSimulation starts multiple nodes in simulation mode
func runSimulation(nodeCount int, basePort int, discoveryPort int, baseDataDir string, profiling bool) {
	log.Printf("🐸 Starting cluster simulation with %d nodes...", nodeCount)

	if baseDataDir == "" {
		baseDataDir = fmt.Sprintf("./sim-cluster-%d", time.Now().Unix())
	}

	// Create nodes using the test framework pattern
	nodes := make([]*Cluster, nodeCount)
	var wg sync.WaitGroup

	// Start nodes in parallel batches
	batchSize := 10
	for batch := 0; batch < nodeCount; batch += batchSize {
		end := batch + batchSize
		if end > nodeCount {
			end = nodeCount
		}

		now := time.Now()
		// Start this batch
		for i := batch; i < end; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()

				nodeID := fmt.Sprintf("sim-node-%03d", index)
				nodeDataDir := filepath.Join(baseDataDir, nodeID)

				node := NewCluster(ClusterOpts{
					ID:            nodeID,
					DataDir:       nodeDataDir,
					HTTPDataPort:  basePort + index,
					DiscoveryPort: discoveryPort,
				})

				// Set fast discovery timings for simulation
				node.DiscoveryManager().SetTimings(2*time.Second, 10*time.Second)

				// Store a demo file with node-specific content
				demoTimestamp := time.Now()
				demoContent := fmt.Sprintf("demo-data-from-%s-at-%d", nodeID, demoTimestamp.Unix())
				if _, err := node.FileSystem.StoreFileWithModTimeAndClusterUpdate(context.TODO(), fmt.Sprintf("/demo-%03d.txt", index), []byte(demoContent), "text/plain", demoTimestamp, now); err != nil {
					log.Print(logerrf("Failed to store demo file on %s: %v", nodeID, err))
				}

				nodes[index] = node
				node.Start()

				// Enable profiling if requested
				if profiling {
					if err := node.enableProfiling(); err != nil {
						log.Printf("[WARNING] Failed to enable profiling on %s: %v", nodeID, err)
					} else {
						log.Printf("[PROFILING] Enabled on %s", nodeID)
					}
				}
			}(i)
		}

		// Wait for this batch to complete
		wg.Wait()
		log.Printf("Started batch %d-%d (%d nodes)", batch, end-1, end-batch)

		// Small delay between batches
		if end < nodeCount {
			time.Sleep(500 * time.Millisecond)
		}
	}

	log.Printf("🎉 All %d simulation nodes started!", nodeCount)
	log.Printf("📊 Web interfaces available on ports %d-%d", basePort, basePort+nodeCount-1)
	if profiling {
		log.Printf("🔍 Profiling enabled on all nodes - access via /profiling on any node")
	}
	log.Printf("🔍 Try: http://localhost:%d/monitor (first node)", basePort)
	log.Printf("📈 Try: http://localhost:%d/cluster-visualizer.html (network view)", basePort)
	log.Printf("📁 Data directory: %s", baseDataDir)

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("\nPress Ctrl+C to stop all nodes...")
	<-sigChan

	log.Println("\n🛑 Shutting down simulation...")

	// Stop all nodes in parallel
	stopNodes(nodes)

	log.Printf("✅ Simulation stopped. Data preserved in: %s", baseDataDir)
	log.Println("Goodbye!")
}

// stopNodes shuts down multiple nodes in parallel
func stopNodes(nodes []*Cluster) {
	var wg sync.WaitGroup

	// Use semaphore to limit concurrency
	maxConcurrency := 20
	sem := make(chan struct{}, maxConcurrency)

	for _, node := range nodes {
		if node == nil {
			continue
		}

		wg.Add(1)
		go func(n *Cluster) {
			defer wg.Done()
			sem <- struct{}{}        // Acquire semaphore
			defer func() { <-sem }() // Release semaphore
			n.Stop()
		}(node)
	}

	wg.Wait()
}

// runSingleNode runs the original single-node mode
func runSingleNode(noDesktop bool, mountPoint string, exportDir string, clusterDir string, importDir string, excludeDirs string, webdavDir string, nodeID string, dataDir string, httpPort int, debug bool, noStore bool, profiling bool, storageMajor string, storageMinor string, encryptionKey string) {
	// Create a new cluster node with default settings
	cluster := NewCluster(ClusterOpts{
		ID:            nodeID,
		DataDir:       dataDir,
		ExportDir:     exportDir,
		ClusterDir:    clusterDir,
		ImportDir:     importDir,
		ExcludeDirs:   excludeDirs,
		HTTPDataPort:  httpPort,
		NoStore:       noStore,
		StorageMajor:  storageMajor,
		StorageMinor:  storageMinor,
		EncryptionKey: encryptionKey,
		Debug:         debug,
	})

	// Enable debug logging if requested
	cluster.Debug = debug

	// Log no-store mode if active
	if noStore {
		cluster.Logger().Printf("📱 Running in CLIENT MODE (--no-store): participating in CRDT but not storing files locally")
	}

	// Start the cluster
	cluster.Start()

	// Start WebDAV server if requested
	if webdavDir != "" {
		cluster.ThreadManager().StartThread("WebDav Server",
			func(ctx context.Context) {
				defer func() { _ = recover() }()
				webdavPort := 8080 // Default WebDAV port
				logger := cluster.Logger()
				logger.Printf("[WEBDAV] WebDAV server starting on port %d", webdavPort)
				if webdavDir != "" {
					logger.Printf("[WEBDAV] Serving cluster path: %s", webdavDir)
				} else {
					logger.Printf("[WEBDAV] Serving entire cluster")
				}

				server := webdav.NewServer(cluster.FileSystem, webdavDir, webdavPort, logger)

				errCh := make(chan error, 1)
				go func() {
					errCh <- server.Start()
				}()

				select {
				case <-ctx.Done():
					if err := server.Stop(); err != nil {
						logger.Printf("[WEBDAV] Error stopping WebDAV server: %v", err)
					}
					select {
					case err := <-errCh:
						if err != nil {
							logger.Printf("[WEBDAV] Server exited with error: %v", err)
						}
					case <-time.After(5 * time.Second):
						logger.Printf("[WEBDAV] Server shutdown timed out waiting for exit")
					}
				case err := <-errCh:
					if err != nil {
						logger.Printf("[WEBDAV] Server exited with error: %v", err)
					}
				}
			})
	}

	// Enable profiling if requested
	if profiling {
		if err := cluster.enableProfiling(); err != nil {
			cluster.Logger().Printf("[WARNING] Failed to enable profiling at startup: %v", err)
		} else {
			cluster.Logger().Printf("[PROFILING] Enabled at startup")
		}
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Print startup information
	fmt.Printf(`
🚀 Cluster node started!
   Node ID: %s
   HTTP API: http://localhost:%d
   Monitor: http://localhost:%d/monitor
   File Browser: http://localhost:%d/files/
   Data Dir: %s
   %s
   %s
   %s

Try these commands:
   # Upload a file
   curl -X PUT --data-binary @myfile.txt http://localhost:%d/api/files/myfile.txt

   # Download a file
   curl http://localhost:%d/api/files/myfile.txt

   # List files
   curl http://localhost:%d/api/files/

   # Check status
   curl http://localhost:%d/status

   # Delete a file
   curl -X DELETE http://localhost:%d/api/files/myfile.txt

   # Create directory
   curl -X POST -H "X-Create-Directory: true" http://localhost:%d/api/files/newfolder

🔧 Client Mode: Use --no-store to connect without local storage
🔍 Profiling: Use --profiling to enable profiling at startup

Press Ctrl+C to stop...
`, cluster.NodeId, cluster.HTTPDataPort, cluster.HTTPDataPort, cluster.HTTPDataPort, cluster.DataDir,

		func() string {
			if exportDir != "" {
				return fmt.Sprintf("   📤 Export Dir (share via SMB): %s", exportDir)
			}
			return ""
		}(),

		func() string {
			if webdavDir != "" {
				return fmt.Sprintf("   📂 WebDAV Server: http://localhost:8080 (serving %s)", webdavDir)
			}
			return ""
		}(),

		func() string {
			if importDir != "" {
				return fmt.Sprintf("   📥 Import Dir (backup source): %s", importDir)
			}
			return ""
		}(),
		cluster.HTTPDataPort, cluster.HTTPDataPort, cluster.HTTPDataPort, cluster.HTTPDataPort, cluster.HTTPDataPort, cluster.HTTPDataPort)

	// Attempt to open the desktop drop window by default. If it fails, continue silently.
	if !noDesktop {
		switch {
		case !desktopUISupported():
			cluster.Logger().Printf("[UI] Desktop UI support not compiled in, skipping desktop UI")
		case !hasGraphicsEnvironment():
			cluster.Logger().Printf("[UI] No graphics environment detected, skipping desktop UI")
		default:
			// Start signal handler in background
			go func() {
				<-sigChan
				fmt.Println("\nShutting down...")
				cluster.Stop()
				fmt.Println("Goodbye!")
				os.Exit(0)
			}()
			// macOS WebView must run on main thread; protect from panic to avoid crashing.
			func() {
				defer func() { _ = recover() }()
				StartDesktopUI(cluster.HTTPDataPort, cluster) // blocks until window closes
			}()
			// Desktop UI closed, shutdown
			fmt.Println("\nShutting down...")
			cluster.Stop()
			fmt.Println("Goodbye!")
			return
		}
	}

	// Wait for signal if desktop UI not running
	<-sigChan
	fmt.Println("\nShutting down...")
	cluster.Stop()
	fmt.Println("Goodbye!")
}

// CheckSuccessWithTimeout polls a condition function until it returns true or times out.
// Returns true if the condition succeeded, false if it timed out.
func CheckSuccessWithTimeout(f func() bool, checkIntervalMs int, timeoutMs int) bool {
	timeout := time.After(time.Duration(timeoutMs) * time.Millisecond)
	ticker := time.NewTicker(time.Duration(checkIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return false
		case <-ticker.C:
			if f() {
				return true
			}
		}
	}
}

// CheckSuccessWithErr polls a condition function until it returns nil or times out.
// Returns nil if the condition succeeded, error if it timed out.
func CheckSuccessWithError(f func() error, checkIntervalMs int, timeoutMs int) error {
	timeout := time.After(time.Duration(timeoutMs) * time.Millisecond)
	ticker := time.NewTicker(time.Duration(checkIntervalMs) * time.Millisecond)
	defer ticker.Stop()
	var lastError error

	for {
		select {
		case <-timeout:
			return fmt.Errorf("Timed out, last error was: %v", lastError)
		case <-ticker.C:
			err := f()
			if err == nil {
				return nil
			} else {
				lastError = err
			}
		}
	}
}

// WaitForConditionT is a test helper that waits for a condition with better error reporting.
// It fails the test if the condition isn't met within the timeout.
func WaitForConditionT(t *testing.T, description string, condition func() bool, checkIntervalMs int, timeoutMs int) {
	t.Helper()
	if !CheckSuccessWithTimeout(condition, checkIntervalMs, timeoutMs) {
		t.Fatalf("%s failed within %dms", description, timeoutMs)
	}
}

// WaitForConditionB is a test helper that waits for a condition with better error reporting.
// It fails the test if the condition isn't met within the timeout.
func WaitForConditionB(t *testing.B, description string, condition func() bool, checkIntervalMs int, timeoutMs int) {
	t.Helper()
	if !CheckSuccessWithTimeout(condition, checkIntervalMs, timeoutMs) {
		t.Fatalf("%s failed within %dms", description, timeoutMs)
	}
}
