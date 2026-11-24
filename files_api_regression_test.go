package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/donomii/clusterF/types"
	clusterwebdav "github.com/donomii/clusterF/webdav"
)

func storeLoopbackFile(t *testing.T, cluster *Cluster, filePath string, data []byte, modified time.Time) {
	t.Helper()
	if _, err := cluster.FileSystem.StoreFileWithModTimeDirect(context.Background(), filePath, data, "text/plain", modified); err != nil {
		t.Fatal(err)
	}
	setLoopbackHolders(cluster, filePath, cluster.ID())
}

func fileAPIRequest(t *testing.T, server *httptest.Server, method, filePath string, data []byte, modified time.Time) (int, http.Header, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+"/api/files"+filePath, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if !modified.IsZero() {
		request.Header.Set("X-ClusterF-Modified-At", modified.Format(time.RFC3339Nano))
	}
	request.Header.Set("Content-Type", "text/plain")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, response.Header, body
}

func TestReviewFileDelete(t *testing.T) {
	cluster, server := newLoopbackCluster(t, "delete", false)
	storeLoopbackFile(t, cluster, "/delete.txt", []byte("remove me"), time.Now().Add(-time.Second))
	status, _, body := fileAPIRequest(t, server, http.MethodDelete, "/delete.txt", nil, time.Time{})
	if status != http.StatusNoContent {
		t.Fatalf("DELETE returned %d: %s", status, body)
	}
	if _, err := cluster.FileSystem.GetMetadata("/delete.txt"); !errors.Is(err, types.ErrFileNotFound) {
		t.Fatalf("deleted file metadata error = %v", err)
	}
}

func TestReviewForwardedUploadRejectsMissingTimestamp(t *testing.T) {
	cluster, _ := newLoopbackCluster(t, "invalid-forwarded-upload", false)
	filePath := "/missing-timestamp.txt"
	metadata, err := json.Marshal(types.FileMetadata{Path: filePath, ContentType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, "/internal/files"+filePath, bytes.NewReader([]byte("content")))
	request.Header.Set("Content-Type", "text/plain")
	request.Header.Set("X-Forwarded-From", "source-node")
	request.Header.Set("X-ClusterF-Metadata", base64.StdEncoding.EncodeToString(metadata))
	response := httptest.NewRecorder()
	cluster.handleFilePutInternal(response, request, filePath)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid forwarded upload returned %d: %s", response.Code, response.Body.String())
	}
	if _, err := cluster.FileSystem.GetMetadata(filePath); !errors.Is(err, types.ErrFileNotFound) {
		t.Fatalf("invalid upload changed stored metadata: %v", err)
	}
}

func TestReviewReadsSkipUnavailableHolderAndReadFullDisk(t *testing.T) {
	gateway, server := newLoopbackCluster(t, "reader", true)
	unavailable, unavailableServer := newLoopbackCluster(t, "unavailable", false)
	holder, _ := newLoopbackCluster(t, "full-disk", false)
	modified := time.Now().Add(-time.Second)
	storeLoopbackFile(t, holder, "/available.txt", []byte("available"), modified)
	registerLoopbackPeer(t, gateway, unavailable, 99)
	registerLoopbackPeer(t, gateway, holder, 1)
	setLoopbackHolders(gateway, "/available.txt", unavailable.ID(), holder.ID())
	gateway.TripCircuitBreaker(unavailableServer.URL, errors.New("holder unavailable"))
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		status, headers, body := fileAPIRequest(t, server, method, "/available.txt", nil, time.Time{})
		if status != http.StatusOK {
			t.Fatalf("%s returned %d: %s", method, status, body)
		}
		if headers.Get("X-ClusterF-Modified-At") != modified.Format(time.RFC3339Nano) {
			t.Fatalf("%s lost timestamp precision: %s", method, headers.Get("X-ClusterF-Modified-At"))
		}
		if method == http.MethodGet && string(body) != "available" {
			t.Fatalf("GET returned %q", body)
		}
	}
}

func TestReviewStreamingReplicasAndRepeatedUpload(t *testing.T) {
	gateway, server := newLoopbackCluster(t, "writer", true)
	first, firstServer := newLoopbackCluster(t, "first", false)
	second, _ := newLoopbackCluster(t, "second", false)
	registerLoopbackPeer(t, gateway, first, 99)
	registerLoopbackPeer(t, gateway, second, 99)
	gateway.frogpond.SetDataPoint("cluster/replication_factor", []byte("2"))
	modified := time.Now().Add(-time.Second)
	for attempt := 0; attempt < 2; attempt++ {
		setLoopbackHolders(gateway, "/replicas.txt", first.ID(), second.ID())
		status, _, body := fileAPIRequest(t, server, http.MethodPut, "/replicas.txt", []byte("first content"), modified)
		if status != http.StatusCreated {
			t.Fatalf("upload %d returned %d: %s", attempt, status, body)
		}
	}
	for _, holder := range []*Cluster{first, second} {
		data, _, err := holder.FileSystem.GetFile("/replicas.txt")
		if err != nil || string(data) != "first content" {
			t.Fatalf("holder %s has %q, error %v", holder.ID(), data, err)
		}
	}
	gateway.TripCircuitBreaker(firstServer.URL, errors.New("first holder unavailable"))
	setLoopbackHolders(gateway, "/replicas.txt", first.ID(), second.ID())
	status, _, body := fileAPIRequest(t, server, http.MethodPut, "/replicas.txt", []byte("newer content"), modified.Add(time.Millisecond))
	if status != http.StatusInternalServerError {
		t.Fatalf("incomplete replication returned %d: %s", status, body)
	}
}

func TestReviewWebDAVRemoteFilesAndDirectories(t *testing.T) {
	gateway, _ := newLoopbackCluster(t, "webdav-reader", true)
	holder, _ := newLoopbackCluster(t, "webdav-holder", false)
	registerLoopbackPeer(t, gateway, holder, 99)
	storeLoopbackFile(t, holder, "/photos/nested/photo.txt", []byte("photo content"), time.Now().Add(-time.Second))
	setLoopbackHolders(gateway, "/photos/nested/photo.txt", holder.ID())
	fs := clusterwebdav.NewClusterFileSystem(gateway.FileSystem, "/photos")
	for _, directory := range []string{"/", "/nested", "/nested/"} {
		info, err := fs.Stat(context.Background(), directory)
		if err != nil || !info.IsDir {
			t.Fatalf("Stat(%q) = %v, %v", directory, info, err)
		}
	}
	reader, err := fs.Open(context.Background(), "/nested/photo.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(data) != "photo content" {
		t.Fatalf("remote WebDAV read = %q, %v", data, err)
	}
	if _, err := fs.Copy(context.Background(), "/nested", "/copied", nil); err != nil {
		t.Fatal(err)
	}
	data, _, err = holder.FileSystem.GetFile("/photos/copied/photo.txt")
	if err != nil || string(data) != "photo content" {
		t.Fatalf("copied remote directory content = %q, %v", data, err)
	}
	if _, err := fs.Move(context.Background(), "/nested", "/moved", nil); err != nil {
		t.Fatal(err)
	}
	data, _, err = holder.FileSystem.GetFile("/photos/moved/photo.txt")
	if err != nil || string(data) != "photo content" {
		t.Fatalf("moved remote directory content = %q, %v", data, err)
	}
	if _, err := holder.FileSystem.GetMetadata("/photos/nested/photo.txt"); !errors.Is(err, types.ErrFileNotFound) {
		t.Fatalf("moved source metadata error = %v", err)
	}
}
