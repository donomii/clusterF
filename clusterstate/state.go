package clusterstate

import (
	"bytes"
	"sync"
	"time"

	"github.com/donomii/frogpond"
)

type Node struct {
	*frogpond.Node
	mu sync.Mutex
}

func NewNode() *Node {
	return &Node{Node: frogpond.NewNode()}
}

func (n *Node) SetDataPoint(key string, value []byte) []frogpond.DataPoint {
	n.mu.Lock()
	defer n.mu.Unlock()
	updated := time.Now().UTC()
	if previous, exists := n.DataPool.Get(key); exists && !updated.After(previous.Updated) {
		updated = previous.Updated.Add(time.Nanosecond)
	}
	point := frogpond.DataPoint{Key: []byte(key), Value: value, Updated: updated}
	n.DataPool.Set(key, point)
	return []frogpond.DataPoint{point.DeepCopy()}
}

func (n *Node) SetDefaultDataPoint(key string, value []byte) []frogpond.DataPoint {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.DataPool.Get(key); exists {
		return nil
	}
	point := frogpond.DataPoint{Key: []byte(key), Value: value, Updated: time.Unix(0, 0).UTC()}
	n.DataPool.Set(key, point)
	return []frogpond.DataPoint{point.DeepCopy()}
}

func (n *Node) AppendDataPoint(point frogpond.DataPoint) []frogpond.DataPoint {
	return n.AppendDataPoints([]frogpond.DataPoint{point})
}

func (n *Node) AppendDataPoints(points []frogpond.DataPoint) []frogpond.DataPoint {
	n.mu.Lock()
	defer n.mu.Unlock()
	var updates []frogpond.DataPoint
	for _, point := range points {
		previous, exists := n.DataPool.Get(string(point.Key))
		if !exists || newer(point, previous) {
			n.DataPool.Set(string(point.Key), point)
			updates = append(updates, point.DeepCopy())
		}
	}
	return updates
}

func newer(candidate, previous frogpond.DataPoint) bool {
	if !candidate.Updated.Equal(previous.Updated) {
		return candidate.Updated.After(previous.Updated)
	}
	if candidate.Deleted != previous.Deleted {
		return candidate.Deleted
	}
	if comparison := bytes.Compare(candidate.Value, previous.Value); comparison != 0 {
		return comparison > 0
	}
	return candidate.Name > previous.Name
}

func (n *Node) DeleteAllMatchingPrefix(prefix string) []frogpond.DataPoint {
	n.mu.Lock()
	defer n.mu.Unlock()
	var updates []frogpond.DataPoint
	for _, point := range n.GetAllMatchingPrefix(prefix) {
		if point.Deleted {
			continue
		}
		updated := time.Now().UTC()
		if !updated.After(point.Updated) {
			updated = point.Updated.Add(time.Nanosecond)
		}
		point.Updated = updated
		point.Deleted = true
		n.DataPool.Set(string(point.Key), point)
		updates = append(updates, point.DeepCopy())
	}
	return updates
}
