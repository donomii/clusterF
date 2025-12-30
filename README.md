# 🐸 clusterF

The F stands for frog

A self-organizing peer-to-peer distributed file storage cluster with CRDT-based replication.

## Features

- **Zero-Configuration P2P Architecture**: Nodes automatically discover each other via UDP broadcast and form a cluster
- **CRDT-Based Replication**: Conflict-free replicated data types ensure eventual consistency without coordination
- **Configurable Replication Factor**: At any time during operations, set the replication factor from 1 - single copy up to full mirroring on every node.
- **Partition-Based Storage**: Files are distributed across partitions with automatic balancing
- **HTTP/REST API**: Complete programmatic access to cluster operations
- **Web UI**: Built-in monitoring dashboard, file browser, and cluster visualizer
- **WebDAV Server**: Mount cluster storage as a network drive
- **Full-Text Search**: Built-in indexer for finding files by name and metadata
- **Media Transcoding**: Automatic ffmpeg-based transcoding for streaming
- **Local Import/Export**: Synchronize between cluster storage and local filesystems
- **Simulation Mode**: Test cluster behavior with multiple nodes in one process
- **Profiling Support**: Built-in pprof and flamegraph generation

## Installation

```bash
go install github.com/donomii/clusterF@latest
```

Or build from source:

```bash
git clone https://github.com/donomii/clusterF
cd clusterF
go build
```

## Quick Start

Start a single node:

```bash
./clusterF
```

The node will:
- Automatically generate a node ID
- Create a data directory (`./data/<node-id>`)
- Start HTTP API on a random port (typically 30000-60000)
- Begin broadcasting for peer discovery on UDP port 9999
- Open a web dashboard

Access the dashboard at `http://localhost:<port>/monitor` (port shown in startup output).

## Usage Examples

### Basic Operations

Start a node with specific configuration:

```bash
./clusterF --node-id mynode --data-dir /var/clusterF --http-port 8080
```

Upload a file:

```bash
curl -X PUT --data-binary @photo.jpg http://localhost:8080/api/files/photos/photo.jpg
```

Download a file:

```bash
curl http://localhost:8080/api/files/photos/photo.jpg -o photo.jpg
```

List files:

```bash
curl http://localhost:8080/api/files/photos/
```

Search for files:

```bash
curl "http://localhost:8080/api/search?q=vacation"
```

### Advanced Features

#### WebDAV Server

Serve cluster files over WebDAV:

```bash
./clusterF --webdav /photos
```

Mount on macOS:

```bash
open "http://localhost:8080"
```

#### Import/Export

Mirror cluster files to a local directory:

```bash
./clusterF --export-dir /mnt/share --cluster-dir /photos
```

Import files from local directory to cluster:

```bash
./clusterF --import-dir /home/user/photos --cluster-dir /backup
```

#### Client Mode

Join cluster without storing data locally:

```bash
./clusterF --no-store
```

#### Simulation Mode

Test cluster with multiple nodes:

```bash
./clusterF --sim-nodes 10 --base-port 30000
```

## Architecture

### Components

- **CRDT Layer (frogpond)**: Manages distributed state with eventual consistency
- **Discovery Manager**: UDP broadcast-based peer discovery
- **Partition Manager**: Distributes files across partitions with configurable replication
- **File System**: Unified interface for file operations across the cluster
- **Indexer**: Full-text search and metadata indexing
- **File Sync**: Bidirectional synchronization with local filesystems
- **Thread Manager**: Lifecycle management for background subsystems
- **Metrics Collector**: Performance monitoring and statistics

### Storage Options

clusterF currently supports file-based disk storage, files are visible and accessible from the command line.  Specialised data stores are possible but not integrated yet.

Select backend with `--storage-major`:

```bash
./clusterF --storage-major bolt
```

### Replication

Files are distributed across partitions based on path hash. Each partition is replicated to RF nodes (default RF=3). The system automatically:

- Detects under-replicated partitions
- Selects replication targets
- Synchronizes partition data between nodes
- Handles node failures gracefully

Adjust replication factor via API:

```bash
curl -X PUT -H "Content-Type: application/json" \
  -d '{"replication_factor": 5}' \
  http://localhost:8080/api/replication-factor
```

## API Reference

### File Operations

- `GET /api/files/<path>` - Download file
- `PUT /api/files/<path>` - Upload file
- `DELETE /api/files/<path>` - Delete file
- `POST /api/files/<path>` - Create directory (with `X-Create-Directory: true` header)
- `GET /api/metadata/<path>` - Get file metadata

### Search

- `GET /api/search?q=<query>` - Search files by name/metadata

### Cluster Management

- `GET /status` - Node status and statistics
- `GET /api/cluster-stats` - Cluster-wide statistics
- `GET /api/partition-stats` - Partition distribution
- `GET /api/replication-factor` - Get RF
- `PUT /api/replication-factor` - Set RF
- `GET /api/under-replicated` - List under-replicated partitions
- `POST /api/integrity-check` - Verify stored file integrity

### Monitoring

- `GET /monitor` - Web-based monitoring dashboard
- `GET /api/metrics` - Prometheus-compatible metrics
- `GET /cluster-visualizer.html` - Network topology visualization

### Profiling

- `GET /profiling` - Profiling control panel
- `GET /flamegraph` - CPU flame graph
- `GET /memorygraph` - Memory flame graph
- `GET /debug/pprof/*` - Go pprof endpoints

## Configuration

### Command-Line Options

```
--node-id           Node identifier (auto-generated if not specified)
--data-dir          Base data directory (default: ./data)
--http-port         HTTP API port (0 = auto)
--discovery-port    UDP discovery port (default: 9999)
--webdav            Serve cluster path over WebDAV
--export-dir        Mirror cluster files to local directory
--import-dir        Import files from local directory
--cluster-dir       Cluster path prefix for import/export
--exclude-dirs      Comma-separated directories to exclude from import
--no-store          Client mode: don't store partitions locally
--storage-major     Storage format (extent|bolt|sqlite|rawfile)
--storage-minor     Storage format minor version
--encryption-key    Encryption key for at-rest encryption
--no-desktop        Don't open desktop UI
--debug             Enable verbose debug logging
--profiling         Enable profiling at startup
--version           Print version and exit
```

### Simulation Mode

```
--sim-nodes         Number of nodes to simulate
--base-port         Base HTTP port for simulation nodes
```

## Web UI

The web interface provides:

- **Dashboard** (`/monitor`): Real-time cluster metrics, peer status, partition distribution
- **File Browser** (`/files/`): Navigate and manage cluster files
- **Visualizer** (`/cluster-visualizer.html`): Interactive network topology
- **CRDT Inspector** (`/crdt`): Examine distributed state
- **Metrics** (`/metrics`): Performance graphs and statistics
- **Profiling** (`/profiling`): CPU and memory profiling tools

## Development

### Building

```bash
go build
```

### Testing

```bash
go test ./...
```

Run large-scale cluster tests:

```bash
go test -run TestLargeCluster -v
```

### Project Structure

```
clusterF/
├── main.go                 # Entry point and cluster lifecycle
├── cluster.go              # Core cluster implementation
├── discovery/              # Peer discovery
├── partitionmanager/       # Partition distribution and replication
├── filesystem/             # File system abstraction
├── filesync/               # Import/export synchronization
├── indexer/                # Search indexing
├── metrics/                # Performance monitoring
├── frontend/               # Web UI
├── webdav/                 # WebDAV server
└── types/                  # Shared types and interfaces
```

## Performance

- Nodes handle thousands of concurrent connections
- Partitions sync in parallel across multiple nodes

## Troubleshooting

### Nodes not discovering each other

- Verify UDP port 9999 is not blocked by firewall
- Check nodes are on same subnet for broadcast discovery
- Try explicit discovery port: `--discovery-port 9999`

### Under-replicated partitions

- Check `/api/under-replicated` for report
- Verify sufficient nodes are online
- Increase partition sync interval: `curl -X PUT -d '{"partition_sync_interval_seconds": 30}' http://localhost:8080/api/partition-sync-interval`

### High memory usage

- Reduce partition sync parallelism (currently hardcoded)
- Enable profiling: `--profiling` and check `/memorygraph`
- Consider client mode for some nodes: `--no-store`

### Data directory errors

- Ensure write permissions on data directory
- Storage format is locked after first start (cannot change `--storage-major`)
- Verify encryption key matches if repository was created with encryption

## License

GNU Affero General Public License v3.0 (AGPL-3.0)

See LICENSE file for full text.

## Contributing

This project follows strict coding conventions:


## Links

- Repository: https://github.com/donomii/clusterF
- Issues: https://github.com/donomii/clusterF/issues
