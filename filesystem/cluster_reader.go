package filesystem

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/donomii/clusterF/httpclient"
	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
)

func (fs *ClusterFileSystem) GetFileReaderFromCluster(ctx context.Context, filePath string) (io.ReadCloser, types.FileMetadata, error) {
	portSource, ok := fs.cluster.(interface{ HTTPPort() int })
	if !ok {
		return nil, types.FileMetadata{}, fmt.Errorf("cluster does not expose HTTPPort for reading %s", filePath)
	}
	fileURL, err := urlutil.BuildFilesURL("127.0.0.1", portSource.HTTPPort(), filePath)
	if err != nil {
		return nil, types.FileMetadata{}, err
	}
	response, err := httpclient.Get(ctx, fs.cluster.DataClient(), fileURL)
	if err != nil {
		return nil, types.FileMetadata{}, fmt.Errorf("read cluster file %s: %w", filePath, err)
	}
	if response.StatusCode != http.StatusOK {
		body, readErr := response.ReadAllAndClose()
		if response.StatusCode == http.StatusNotFound {
			return nil, types.FileMetadata{}, fmt.Errorf("%w: %s: %s", types.ErrFileNotFound, filePath, body)
		}
		return nil, types.FileMetadata{}, fmt.Errorf("read cluster file %s: HTTP %d: %s (body error: %v)", filePath, response.StatusCode, body, readErr)
	}
	modifiedAt, err := time.Parse(time.RFC3339Nano, response.Header.Get("X-ClusterF-Modified-At"))
	if err != nil {
		response.Close()
		return nil, types.FileMetadata{}, fmt.Errorf("invalid modification time for cluster file %s: %w", filePath, err)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, response.Header.Get("X-ClusterF-Created-At"))
	if err != nil {
		response.Close()
		return nil, types.FileMetadata{}, fmt.Errorf("invalid creation time for cluster file %s: %w", filePath, err)
	}
	metadata := types.FileMetadata{
		Path: filePath, Name: path.Base(filePath), Size: response.ContentLength,
		ContentType: response.Header.Get("Content-Type"), Checksum: response.Header.Get("X-ClusterF-Checksum"),
		ModifiedAt: modifiedAt, CreatedAt: createdAt,
	}
	return response.Body, metadata, nil
}
