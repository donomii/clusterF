package clusterstate

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/donomii/frogpond"
)

func TestSequentialWritesWithinOneSecond(t *testing.T) {
	node := NewNode()
	previous := time.Now().Add(time.Hour).UTC()
	node.AppendDataPoint(frogpond.DataPoint{Key: []byte("setting"), Value: []byte("first"), Updated: previous})
	for _, value := range []string{"second", "third"} {
		updates := node.SetDataPoint("setting", []byte(value))
		point := node.GetDataPoint("setting")
		if len(updates) != 1 || string(point.Value) != value || !point.Updated.After(previous) {
			t.Fatalf("write %q was not retained: updates=%v point=%+v previous=%v", value, updates, point, previous)
		}
		previous = point.Updated
	}
}

func TestConcurrentWritesAreSerialized(t *testing.T) {
	node := NewNode()
	var workers sync.WaitGroup
	updates := make(chan frogpond.DataPoint, 32)
	for i := 0; i < cap(updates); i++ {
		workers.Add(1)
		go func(value byte) {
			defer workers.Done()
			updates <- node.SetDataPoint("setting", []byte{value})[0]
		}(byte(i))
	}
	workers.Wait()
	close(updates)
	seen := make(map[time.Time]bool)
	for point := range updates {
		if seen[point.Updated] {
			t.Fatalf("concurrent writes reused timestamp %v", point.Updated)
		}
		seen[point.Updated] = true
	}
}

func TestEqualTimestampConvergesRegardlessOfArrivalOrder(t *testing.T) {
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 42, time.UTC)
	first := frogpond.DataPoint{Key: []byte("setting"), Value: []byte("first"), Updated: stamp}
	second := frogpond.DataPoint{Key: []byte("setting"), Value: []byte("second"), Updated: stamp}
	for _, deleted := range []bool{false, true} {
		second.Deleted = deleted
		left, right := NewNode(), NewNode()
		left.AppendDataPoints([]frogpond.DataPoint{first, second})
		right.AppendDataPoints([]frogpond.DataPoint{second, first})
		if !bytes.Equal(left.JsonDump(), right.JsonDump()) {
			t.Fatalf("equal timestamps failed to converge: left=%s right=%s", left.JsonDump(), right.JsonDump())
		}
	}
}

func TestDefaultsDoNotOverrideExistingConfiguration(t *testing.T) {
	cluster, joining := NewNode(), NewNode()
	configured := cluster.SetDataPoint("cluster/replication_factor", []byte("5"))
	defaults := joining.SetDefaultDataPoint("cluster/replication_factor", []byte("3"))
	joining.AppendDataPoints(configured)
	cluster.AppendDataPoints(defaults)
	for name, node := range map[string]*Node{"cluster": cluster, "joining": joining} {
		if actual := string(node.GetDataPoint("cluster/replication_factor").Value); actual != "5" {
			t.Fatalf("%s retained replication factor %s; expected 5", name, actual)
		}
	}
}

func TestDeletionWinsOverOlderState(t *testing.T) {
	node := NewNode()
	original := node.SetDataPoint("partition/one", []byte("data"))
	deleted := node.DeleteAllMatchingPrefix("partition/")
	if len(deleted) != 1 || len(node.DeleteAllMatchingPrefix("partition/")) != 0 {
		t.Fatal("expected one stable deletion update")
	}
	node.AppendDataPoints(original)
	if !node.GetDataPoint("partition/one").Deleted {
		t.Fatal("older state resurrected a deleted entry")
	}
}
