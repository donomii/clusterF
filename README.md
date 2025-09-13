# clusterF🐸

The F stands for frog.

## Self-Organizing P2P Storage Cluster

A zero-config, peer-to-peer storage cluster that automatically discovers nodes, and replicates data.  Plug and play storage management.

 clusterF has no security at all.  It is designed for home networks.  Run on a trusted LAN, behind a firewall, and do not expose its ports to the internet.

## Features

- **Zero Configuration**: Just run the binary, nodes auto-discover each other
- **Self-Healing**: Automatically repairs data when nodes disappear
- **P2P Architecture**: No central servers or coordination points
- **UDP Discovery**: Uses broadcast/multicast for node discovery
- **HTTP API**: Simple REST API for data operations
- **File System Layer**: Full file and directory operations with partition-based storage
- **Configurable Replication**: Default RF=3 (3 copies across partitions)
- **Tombstone Deletion**: Consistent delete semantics across the cluster
- **No security**: Designed for home networks, this software has no security at all

Note: The UDP discovery mechanism will connect to any other computer on the network. If that network is your local cafe, then it will be announcing itself to strangers. You have been warned.

## Quick Start

### Single Node

```bash
go run .
```

Or download the binary and double click on the frog.

ClusterF will start and immediately join the cluster on your local network.  If there is no cluster, the first node will create the cluster.

### Multi-Node Cluster

```bash
# Terminal 1
sh build.sh
./cluster --node-id node1

# Terminal 2  
./cluster --node-id node2

# Terminal 3
./cluster --node-id node3
```

Pin the HTTP port with `--http-port` and print version info with `--version`.

## API Usage

### File System Operations

```bash
# Upload a file
curl -X PUT --data-binary @myfile.txt http://localhost:30000/api/files/myfile.txt

# Download a file
curl http://localhost:30000/api/files/myfile.txt

# List directory
curl http://localhost:30000/api/files/

# Create directory
curl -X POST -H "X-Create-Directory: true" http://localhost:30000/api/files/newfolder

# Delete file or directory
curl -X DELETE http://localhost:30000/api/files/myfile.txt
```

### Check Node Status
```bash
curl http://localhost:30000/status
```

Example `/status` response:
```json
{
  "id": "host-12345",
  "http_port": 30000,
  "replication_factor": 3,
  "files": 42
}
```

### Web Interface
```bash
# Open in browser
open http://localhost:30000

# File browser interface
open http://localhost:30000/files/

# Node monitoring dashboard
open http://localhost:30000/monitor
```

API Reference UI: open `http://localhost:30000/api`.

## ⚙️ Configuration

clusterF can be configured from the command line or through the web app.

### CLI Flags

- `--data-dir`: Base data directory; per-node data stored at `./data/<node-id>` by default
- `--node-id`: Explicit node ID; otherwise generated and persisted in `.node-id`
- `--export-dir`: Mirror cluster files to a local directory (e.g., for SMB sharing)
- `--discovery-port`: UDP discovery port (default `9999`); also respects `CLUSTER_BCAST_PORT`
- `--http-port`: HTTP port to bind (default dynamic near `30000` with fallback)
- `--desktop`: Open the native desktop drop window (requires CGO + WebKit on Linux)
- `--sim-nodes`: Start N simulated nodes (ports start at `--base-port`)
- `--base-port`: Base port for simulation HTTP servers
- `--max-chunk-size`: Deprecated; partition-based storage in use
- `--debug`: Enable verbose debug logging
- `--version`: Print version, commit, and build date and exit

### Port Behavior

The HTTP server binds to the configured `--http-port` if available; otherwise it falls back to a random high port near `30000`. The chosen port is printed after the server starts listening.

### Data Directory & Node IDs

- Default base data directory: `./data`
- On first run, clusterF generates a node ID like `host-12345`, creates `./data/<node-id>`, and persists it in `./data/<node-id>/.node-id`
- If `--data-dir` already contains a single node subdir or `.node-id`, that ID is reused

### Desktop UI Dependencies (optional)

The desktop drag-and-drop window uses WebKit via CGO. Building the headless server does not require these deps. In CI and headless builds, `CGO_ENABLED=0` disables the desktop UI.

- macOS (Homebrew): `brew install webkit2gtk` (only needed if you build with `--desktop`)
- Ubuntu/Debian (APT): `sudo apt-get install -y build-essential pkg-config libgtk-3-dev libwebkit2gtk-4.1-dev`
- Fedora (DNF): `sudo dnf install -y gcc gcc-c++ make pkgconf-pkg-config gtk3-devel webkit2gtk3-devel`

Headless build example:
```bash
CGO_ENABLED=0 go build .
```

## How It Works

### 1. Discovery
- Nodes broadcast their presence via UDP to `255.255.255.255:9999`
- Nodes listen on the same port to discover peers
- Handles multiple nodes on same machine

### 2. FrogPond Protocol
- CRDT table sends updates to peers
- Metadata includes: partition information, delete tombstones, write timestamps
- Uses timestamps for detecting data changes in partitions
- Eventually consistent across all nodes

### 3. Auto-Replication
- New files are immediately reflected in partition metadata
- Nodes track partition locations in the frogpond distributed index  
- When partitions are under-replicated (< RF copies), nodes sync from peers

### 4. Failure Handling
- Surviving nodes automatically create new copies to achieve replication factor

### 5. Conflict Handling
- CRDT means latest version wins

Delete semantics: deletions create tombstones which propagate; peers will not resurrect deleted files once tombstones are observed.

## Architecture

```
┌──────────────┐     Frogpond.     ┌──────────────┐
│   Node A     │◄─────────────────►│   Node B     │
│              │                   │              │
│ ┌──────────┐ │    HTTP Data      │ ┌──────────┐ │
│ │Files/    │ │◄─────────────────►│ │Files/    │ │
│ │Partitions│ │                   │ │Partitions│ │
│ └──────────┘ │                   │ └──────────┘ │
└──────────────┘                   └──────────────┘
       ▲                                 ▲
       │            UDP Discovery        │
       └─────────────────────────────────┘
              (Broadcast/Multicast)
```

## 🔧 Development

### Run Tests
```bash
go test -v
```

### Build Binary
```bash
go build -o cluster
./cluster
```

### Simulation environment
```bash
./cluster --sim-nodes 10
```

## Use Cases

- **Home NAS Replacement**: Plug in USB drives across multiple machines
- **IoT Storage**: Raspberry Pis with attached storage auto-forming clusters

## Roadmap


- [x] Web UI for cluster management  
- [x] File system layer with partition-based storage
- [ ] Desktop integration via WebDAV
- [ ] Docker/Kubernetes deployment
- [ ] Cross-platform binaries

## License

AGPL License - see LICENSE file for details.

---

*Built for people who want distributed storage that "just works" without the complexity of traditional cluster filesystems.*
