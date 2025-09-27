// main.go - Simple cluster node launcher
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
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
	httpPort := flag.Int("http-port", 0, "HTTP port to bind (0 = dynamic near 30000)")
	debug := flag.Bool("debug", false, "Enable verbose debug logging")
	noStore := flag.Bool("no-store", false, "Client mode: participate in CRDT but don't store partitions locally")
	profiling := flag.Bool("profiling", false, "Enable profiling immediately at startup")
	showVersion := flag.Bool("version", false, "Print version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("clusterF %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	if *simNodes > 0 {
		runSimulation(*simNodes, *basePort, *discoveryPort, *dataDir, *profiling)
	} else {
		runSingleNode(*noDesktop, *mountPoint, *exportDir, *nodeID, *dataDir, *httpPort, *debug, *noStore, *profiling)
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
				if err := node.FileSystem.StoreFileWithModTime(fmt.Sprintf("/demo-%03d.txt", index), []byte(demoContent), "text/plain", demoTimestamp); err != nil {
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
func runSingleNode(noDesktop bool, mountPoint string, exportDir string, nodeID string, dataDir string, httpPort int, debug bool, noStore bool, profiling bool) {
	// Create a new cluster node with default settings
	cluster := NewCluster(ClusterOpts{
		ID:           nodeID,
		DataDir:      dataDir,
		ExportDir:    exportDir,
		HTTPDataPort: httpPort,
		NoStore:      noStore,
	})

	// Enable debug logging if requested
	cluster.Debug = debug

	// Log no-store mode if active
	if noStore {
		cluster.Logger().Printf("📱 Running in CLIENT MODE (--no-store): participating in CRDT but not storing files locally")
	}

	// Start the cluster
	cluster.Start()

	// Enable profiling if requested
	if profiling {
		if err := cluster.enableProfiling(); err != nil {
			cluster.Logger().Printf("[WARNING] Failed to enable profiling at startup: %v", err)
		} else {
			cluster.Logger().Printf("[PROFILING] Enabled at startup")
		}
	}
	// Attempt to open the desktop drop window by default. If it fails, continue silently.
	if !noDesktop {
		// Check if we have a display environment before attempting desktop UI
		if hasGraphicsEnvironment() {
			if runtime.GOOS == "darwin" {
				// macOS WebView must run on main thread; protect from panic to avoid crashing.
				func() {
					defer func() { _ = recover() }()
					StartDesktopUI(cluster.HTTPDataPort, cluster) // blocks until window closes
				}()
			} else {
				go func() {
				defer func() { _ = recover() }()
				StartDesktopUI(cluster.HTTPDataPort, cluster)
				}()
			}
		} else {
			cluster.Logger().Printf("[UI] No graphics environment detected, skipping desktop UI")
		}
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf(`
🚀 Cluster node started!
   Node ID: %s
   HTTP API: http://localhost:%d
   Monitor: http://localhost:%d/monitor
   File Browser: http://localhost:%d/files/
   Data Dir: %s
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
		cluster.HTTPDataPort, cluster.HTTPDataPort, cluster.HTTPDataPort, cluster.HTTPDataPort, cluster.HTTPDataPort, cluster.HTTPDataPort)

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
