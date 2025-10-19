# WebDAV Package - Technical Reference

## What This Does
WebDAV server that exposes the cluster filesystem using standard WebDAV protocol.

## Key Files
- `server.go` - WebDAV protocol handler
- `clusterfilesystem.go` - Adapter between WebDAV and cluster filesystem

## Architecture
```
WebDAV Client (Finder, Explorer, etc.)
    ↓ WebDAV Protocol (RFC 4918)
WebDAV Handler (golang.org/x/net/webdav)
    ↓ FileSystem interface
ClusterFileSystem adapter
    ↓ Cluster filesystem API
Partition Manager
```

## ClusterFileSystem Adapter
Implements `webdav.FileSystem` interface:
```go
type FileSystem interface {
    Mkdir(name string, perm os.FileMode) error
    OpenFile(name string, flag int, perm os.FileMode) (File, error)
    RemoveAll(name string) error
    Rename(oldName, newName string) error
    Stat(name string) (os.FileInfo, error)
}
```

Maps to cluster operations:
- `Mkdir()` → cluster.CreateDirectory()
- `OpenFile()` → cluster.GetFile() / cluster.StoreFileWithModTime()
- `RemoveAll()` → cluster.DeleteFile()
- `Stat()` → cluster.GetMetadata()
- `Rename()` → Get + Store + Delete (atomic rename doesn't exist)

## WebDAV Methods Supported
- **PROPFIND** - Directory listing / file properties
- **GET** - Download file
- **PUT** - Upload file
- **DELETE** - Delete file
- **MKCOL** - Create directory
- **MOVE** - Rename/move file

## Important Notes
- Reads go through partition manager (queries cluster if not local)
- Writes always go to local node (replication happens separately)
- Renames are NOT atomic (implemented as copy+delete)
- No locking support (WebDAV LOCK/UNLOCK ignored)
- Directory creation is synthetic (no actual directories stored)

## Mount Points
- macOS Finder: Connect to Server → `http://host:port/webdav/`
- Windows Explorer: Map Network Drive → `http://host:port/webdav/`
- Linux: `mount -t davfs http://host:port/webdav/ /mnt/point`

## Notes for Me
- WebDAV is STATELESS - each request independent
- No session management
- Authentication not implemented (add if needed)
- Large file uploads are streamed (don't load all in memory)
- Directory listings can be slow (synthesized from file paths)
- Some WebDAV clients are buggy - test thoroughly
- The golang.org/x/net/webdav package does most of the work
- We just provide the FileSystem adapter
