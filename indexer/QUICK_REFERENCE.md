# Indexer Quick Reference

## Creation

```go
// Default (Trie-based)
idx := indexer.NewIndexer(logger)

// Explicit type
idx := indexer.NewIndexerWithType(logger, indexer.IndexTypeTrie)  // Default
idx := indexer.NewIndexerWithType(logger, indexer.IndexTypeFlat)  // Fallback
```

## Operations

```go
// Add or update file
metadata := types.FileMetadata{
    Name:        "file.txt",
    Path:        "/docs/file.txt",
    Size:        100,
    ContentType: "text/plain",
    ModifiedAt:  time.Now(),
    Checksum:    "abc123",
}
idx.AddFile("/docs/file.txt", metadata)

// Delete file
idx.DeleteFile("/docs/file.txt")

// Search by prefix
results := idx.PrefixSearch("/docs/")
for _, result := range results {
    fmt.Printf("%s (%d bytes)\n", result.Path, result.Size)
}

// Import from filestore
err := idx.ImportFilestore(partitionManager)
```

## Index Types

```go
// Check current type
indexType := idx.GetIndexType()

// Types available
indexer.IndexTypeTrie  // Default: O(m) search
indexer.IndexTypeFlat  // Fallback: O(n) search
```

## Performance Characteristics

| Operation | Trie    | Flat    |
|-----------|---------|---------|
| Search    | O(m)    | O(n)    |
| Add       | O(m)    | O(1)    |
| Delete    | O(m)    | O(1)    |
| Import    | O(n×m)  | O(n)    |
| Memory    | Medium  | Low     |

Where:
- m = path length (typically 10-100 chars)
- n = number of files (can be millions)

## Thread Safety

All operations are thread-safe:
- **Reads** (PrefixSearch): Concurrent via RLock
- **Writes** (Add/Delete): Exclusive via Lock

## Integration Points

### Automatic (Cluster)
```go
// Cluster automatically:
// 1. Creates indexer
// 2. Imports filestore on Start()
// 3. Updates on file changes
cluster := NewCluster(opts)
cluster.Start()
```

### Manual (Custom)
```go
// Create indexer
idx := indexer.NewIndexer(logger)

// Import existing files
err := idx.ImportFilestore(partitionManager)

// Hook into file operations
partitionManager.SetIndexer(idx)
```

## Common Patterns

### Directory Listing
```go
// Search for all files in a directory
results := idx.PrefixSearch("/docs/")

// Results include files and subdirectories
for _, result := range results {
    if strings.HasSuffix(result.Path, "/") {
        fmt.Printf("DIR:  %s\n", result.Path)
    } else {
        fmt.Printf("FILE: %s\n", result.Path)
    }
}
```

### Recursive Search
```go
// Find all files under a path
allFiles := idx.PrefixSearch("/")

// Filter by directory
docsFiles := idx.PrefixSearch("/docs/")

// Filter by subdirectory
subFiles := idx.PrefixSearch("/docs/subdir/")
```

### File Existence Check
```go
results := idx.PrefixSearch("/docs/file.txt")
if len(results) > 0 {
    fmt.Println("File exists")
}
```

## Best Practices

1. **Use Trie by default** - Faster for most workloads
2. **Let cluster manage it** - Automatic integration
3. **Don't persist index** - Rebuilt on startup
4. **Trust the callbacks** - Automatically stays in sync
5. **Use flat for small datasets** - If < 1000 files

## Troubleshooting

### Slow Searches
```go
// Check index type
if idx.GetIndexType() == indexer.IndexTypeFlat {
    // Switch to Trie for better performance
    newIdx := indexer.NewIndexerWithType(logger, indexer.IndexTypeTrie)
    newIdx.ImportFilestore(partitionManager)
}
```

### Out of Sync
```go
// Rebuild index
idx := indexer.NewIndexer(logger)
err := idx.ImportFilestore(partitionManager)
```

### High Memory Usage
```go
// Use flat index (lower memory)
idx := indexer.NewIndexerWithType(logger, indexer.IndexTypeFlat)
```

## Examples

### Basic Usage
```go
package main

import (
    "log"
    "github.com/donomii/clusterF/indexer"
    "github.com/donomii/clusterF/types"
)

func main() {
    logger := log.New(os.Stdout, "[INDEXER] ", log.LstdFlags)
    
    // Create trie-based indexer
    idx := indexer.NewIndexer(logger)
    
    // Add files
    idx.AddFile("/docs/readme.txt", types.FileMetadata{
        Name: "readme.txt",
        Path: "/docs/readme.txt",
        Size: 1024,
    })
    
    // Search
    results := idx.PrefixSearch("/docs/")
    fmt.Printf("Found %d files\n", len(results))
}
```

### With Cluster
```go
// Automatically integrated in cluster
cluster := NewCluster(ClusterOpts{
    ID:      "node-1",
    DataDir: "./data",
})

// Indexer is created and managed automatically
cluster.Start()

// Search via cluster API
results := cluster.ListDirectoryUsingSearch("/docs/")
```

## API Reference

### Constructor Functions
- `NewIndexer(logger)` - Create with default (Trie)
- `NewIndexerWithType(logger, indexType)` - Create with specific type

### Methods
- `PrefixSearch(prefix) []SearchResult` - Find files by prefix
- `AddFile(path, metadata)` - Add or update file
- `DeleteFile(path)` - Remove file
- `ImportFilestore(pm) error` - Import from partition manager
- `GetIndexType() IndexType` - Get current implementation

### Types
- `IndexTypeTrie` - Trie-based (default)
- `IndexTypeFlat` - Map-based (fallback)

## See Also

- `/indexer/README.md` - Full documentation
- `/indexer/TRIE_MIGRATION.md` - Migration details
- `/indexer/COMPLETE_SUMMARY.md` - Complete overview
- `/indexer/indexer_test.go` - Usage examples
