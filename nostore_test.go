package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/donomii/clusterF/partitionmanager"
	"github.com/donomii/clusterF/testenv"
)

func TestCluster_NoStoreMode(t *testing.T) {
	testenv.RequireUDPSupport(t)

	t.Logf("🧪 Testing no-store client mode functionality")

	// Create 3 nodes: 2 normal storage nodes + 1 no-store client
	nodes := make([]*Cluster, 3)

	// Create normal storage nodes
	for i := 0; i < 2; i++ {
		nodeID := fmt.Sprintf("storage-node-%d", i)
		dataDir := fmt.Sprintf("./test-data/nostore-test/%s", nodeID)

		nodes[i] = NewCluster(ClusterOpts{
			ID:            nodeID,
			DataDir:       dataDir,
			HTTPDataPort:  30000 + i,
			DiscoveryPort: 9999,
			NoStore:       false, // Normal storage nodes
		})
		nodes[i].Debug = true
		nodes[i].DiscoveryManager.SetTimings(1*time.Second, 5*time.Second)
		t.Logf("Created storage node %s", nodeID)
	}

	// Create no-store client node
	clientNodeID := "client-node"
	nodes[2] = NewCluster(ClusterOpts{
		ID:            clientNodeID,
		DataDir:       fmt.Sprintf("./test-data/nostore-test/%s", clientNodeID),
		HTTPDataPort:  30002,
		DiscoveryPort: 9999,
		NoStore:       true, // No-store client mode
	})
	nodes[2].Debug = true
	nodes[2].DiscoveryManager.SetTimings(1*time.Second, 5*time.Second)
	t.Logf("Created no-store client node %s", clientNodeID)

	// Start all nodes
	for i, node := range nodes {
		node.Start()
		t.Logf("Started node %d: %s", i, node.NodeId)
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	// Wait for discovery
	t.Logf("Waiting for node discovery...")
	time.Sleep(3 * time.Second)

	client := &http.Client{Timeout: 10 * time.Second}
	testData := []byte("Test file content for no-store validation")
	testFileName := "/test-nostore-file.txt"

	// Test 1: Store file on storage node 0
	t.Logf("📤 Test 1: Storing file on storage node 0")
	storeURL := fmt.Sprintf("http://localhost:%d/api/files%s", nodes[0].HTTPDataPort, testFileName)
	uploadTime := time.Now()
	req, _ := http.NewRequest(http.MethodPut, storeURL, bytes.NewReader(testData))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-ClusterF-Modified-At", uploadTime.Format(time.RFC3339Nano))

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to store file on storage node: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Expected 201, got %d", resp.StatusCode)
	}
	t.Logf("✅ File stored successfully on storage node 0")

	// Wait for replication
	t.Logf("Waiting for file replication...")
	time.Sleep(3 * time.Second)

	// Test 2: Check that storage nodes have the file locally
	t.Logf("📋 Test 2: Checking local storage on storage nodes")
	for i := 0; i < 2; i++ {
		hasFile := false
		// Check if file exists in local storage
		if content, _, err := nodes[i].FileSystem.GetFile(testFileName); err == nil {
			if string(content) == string(testData) {
				hasFile = true
			}
		}
		t.Logf("Storage node %d has file locally: %v", i, hasFile)

		// At least one storage node should have it (the one we uploaded to)
		if i == 0 && !hasFile {
			t.Errorf("Storage node 0 should have the file locally")
		}
	}

	// Test 3: Check that no-store client does NOT have file locally
	t.Logf("📋 Test 3: Verifying no-store client has no local storage")
	// Check actual local storage, not file system interface
	partitionID := partitionmanager.HashToPartition(testFileName)
	fileKey := fmt.Sprintf("partition:%s:file:%s", partitionID, testFileName)
	_, err = nodes[2].metadataKV.Get([]byte(fileKey))
	clientHasFile := (err == nil)

	t.Logf("No-store client has file in local KV: %v", clientHasFile)

	if clientHasFile {
		t.Errorf("No-store client should NOT have the file stored locally")
	} else {
		t.Logf("✅ No-store client correctly has no local storage")
	}

	// Test 4: Fetch file from no-store client via HTTP (should get from peers)
	t.Logf("📥 Test 4: Fetching file from no-store client (should get from peers)")
	clientURL := fmt.Sprintf("http://localhost:%d/api/files%s", nodes[2].HTTPDataPort, testFileName)

	WaitForConditionT(t, "File retrieval from no-store client", func() bool {
		resp, err := client.Get(clientURL)
		if err != nil {
			t.Logf("Fetch attempt failed: %v", err)
			return false
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Logf("Fetch attempt returned %d", resp.StatusCode)
			return false
		}

		content, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Logf("Failed to read response: %v", err)
			return false
		}

		if string(content) != string(testData) {
			t.Logf("Content mismatch: got %q, want %q", string(content), string(testData))
			return false
		}

		t.Logf("✅ No-store client successfully retrieved file from peers")
		return true
	}, 1000, 10000)

	// Test 5: Store file TO no-store client (should be sent to storage nodes)
	t.Logf("📤 Test 5: Storing file TO no-store client (should redirect to storage nodes)")
	clientStoreData := []byte("File uploaded to no-store client")
	clientStoreFileName := "/client-upload-test.txt"
	clientStoreURL := fmt.Sprintf("http://localhost:%d/api/files%s", nodes[2].HTTPDataPort, clientStoreFileName)

	uploadTime2 := time.Now()
	req, _ = http.NewRequest(http.MethodPut, clientStoreURL, bytes.NewReader(clientStoreData))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-ClusterF-Modified-At", uploadTime2.Format(time.RFC3339Nano))

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("Failed to store file via no-store client: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Expected 201 when storing via no-store client, got %d", resp.StatusCode)
	}
	t.Logf("✅ File upload to no-store client accepted")

	// Wait for the file to be stored somewhere
	time.Sleep(2 * time.Second)

	// Test 6: Verify file uploaded via no-store client is NOT stored locally on client
	t.Logf("📋 Test 6: Verifying file uploaded via no-store client is not stored locally")
	// Check actual local storage, not file system interface
	clientStorePartitionID := partitionmanager.HashToPartition(clientStoreFileName)
	clientStoreFileKey := fmt.Sprintf("partition:%s:file:%s", clientStorePartitionID, clientStoreFileName)
	_, err = nodes[2].metadataKV.Get([]byte(clientStoreFileKey))
	clientHasUploadedFile := (err == nil)

	if clientHasUploadedFile {
		t.Errorf("No-store client should NOT store uploaded files locally")
	} else {
		t.Logf("✅ No-store client correctly did not store uploaded file locally")
	}

	// Test 7: Verify file uploaded via no-store client is available from storage nodes (forwarded)
	t.Logf("📥 Test 7: Verifying uploaded file was forwarded to storage nodes")
	fileFoundOnStorageNode := false

	for i := 0; i < 2; i++ {
		storageURL := fmt.Sprintf("http://localhost:%d/api/files%s", nodes[i].HTTPDataPort, clientStoreFileName)
		if resp, err := client.Get(storageURL); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				if content, err := io.ReadAll(resp.Body); err == nil {
					if string(content) == string(clientStoreData) {
						fileFoundOnStorageNode = true
						t.Logf("✅ File uploaded via no-store client found on storage node %d", i)
						break
					}
				}
			}
		}
	}

	if !fileFoundOnStorageNode {
		t.Errorf("File uploaded via no-store client should be available from storage nodes after forwarding")
	} else {
		t.Logf("✅ Upload forwarding from no-store client working correctly")
	}

	t.Logf("🎉 All no-store mode tests passed!")
}
