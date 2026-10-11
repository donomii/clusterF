package main

import (
	"context"
	"testing"
	"time"
)

func TestReviewJoiningNodeKeepsConfiguredReplicationFactor(t *testing.T) {
	established, _ := newLoopbackCluster(t, "established", false)
	established.setReplicationFactor(5)
	joining, _ := newLoopbackCluster(t, "joining", false)
	registerLoopbackPeer(t, joining, established, 99)
	registerLoopbackPeer(t, established, joining, 99)
	peer, ok := joining.LoadPeer(established.ID())
	if !ok || !joining.requestFullStoreFromPeer(peer) {
		t.Fatal("joining node could not fetch established state")
	}
	peer, ok = established.LoadPeer(joining.ID())
	if !ok || !established.requestFullStoreFromPeer(peer) {
		t.Fatal("established node could not fetch joining state")
	}
	if established.GetCurrentRF() != 5 || joining.GetCurrentRF() != 5 {
		t.Fatalf("joining changed replication factor: established=%d joining=%d", established.GetCurrentRF(), joining.GetCurrentRF())
	}
}

func TestReviewClientReceivesPublishedConfiguration(t *testing.T) {
	storage, _ := newLoopbackCluster(t, "storage", false)
	client, _ := newLoopbackCluster(t, "client", true)
	registerLoopbackPeer(t, storage, client, 99)
	storage.setReplicationFactor(5)
	if !CheckSuccessWithTimeout(func() bool { return client.GetCurrentRF() == 5 }, 10, 3000) {
		t.Fatalf("client did not receive replication factor 5; got %d", client.GetCurrentRF())
	}
}

func TestReviewRepeatedFullSyncRepairsMissedUpdate(t *testing.T) {
	reader, _ := newLoopbackCluster(t, "reader", false)
	writer, _ := newLoopbackCluster(t, "writer", false)
	registerLoopbackPeer(t, reader, writer, 99)
	reader.SetPartitionSyncInterval(1)
	writer.frogpond.SetDataPoint("review/missed-update", []byte("first"))
	ctx, cancel := context.WithCancel(reader.AppContext())
	done := make(chan struct{})
	go func() {
		defer close(done)
		reader.runPeerFullStoreSync(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("full-state synchronization did not stop after cancellation")
		}
	})
	if !CheckSuccessWithTimeout(func() bool {
		return string(reader.frogpond.GetDataPoint("review/missed-update").Value) == "first"
	}, 10, 3000) {
		t.Fatal("initial full-state synchronization did not complete")
	}
	writer.frogpond.SetDataPoint("review/missed-update", []byte("second"))
	if !CheckSuccessWithTimeout(func() bool {
		return string(reader.frogpond.GetDataPoint("review/missed-update").Value) == "second"
	}, 10, 4000) {
		t.Fatal("periodic synchronization did not repair an update missed after the first successful fetch")
	}
}

func TestReviewReadablePeersIncludeFullDiskAndLocalNode(t *testing.T) {
	local, _ := newLoopbackCluster(t, "local", false)
	full, _ := newLoopbackCluster(t, "full", false)
	registerLoopbackPeer(t, local, full, 4)
	local.DiscoveryManager().GetPeerMap().Delete(string(local.ID()))
	if _, exists := local.GetAvailablePeerMap()[full.ID()]; exists {
		t.Fatal("full peer was selected for new storage")
	}
	readable := local.GetReadablePeerMap()
	if _, exists := readable[full.ID()]; !exists {
		t.Fatal("existing files on full peer cannot be read")
	}
	if peer, exists := readable[local.ID()]; !exists || peer.Address != "127.0.0.1" || peer.HTTPPort != local.HTTPDataPort {
		t.Fatalf("local readable peer missing without self discovery: %+v", peer)
	}
}
