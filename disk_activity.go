package main

import (
	"time"

	"github.com/donomii/clusterF/types"
)

const diskActiveWindow = 10 * time.Minute

type diskMetricsSnapshot struct {
	bytesStored int64
	diskSize    int64
	diskFree    int64
	updatedAt   time.Time
}

func (c *Cluster) RecordDiskActivity(level types.DiskActivityLevel) {
	if level == types.DiskActivityEssential {
		c.markDiskActive()
	}
}

func (c *Cluster) CanRunNonEssentialDiskOp() bool {
	return c.DiskIsActive()
}

func (c *Cluster) DiskIsActive() bool {
	expiresAt := time.Unix(0, c.diskActiveUntil.Load())
	return time.Now().Before(expiresAt)
}

func (c *Cluster) markDiskActive() {
	now := time.Now()
	newExpiry := now.Add(diskActiveWindow).UnixNano()

	for {
		current := c.diskActiveUntil.Load()

		if current >= newExpiry {
			return
		}

		if c.diskActiveUntil.CompareAndSwap(current, newExpiry) {
			return
		}
	}
}

func (c *Cluster) recordDiskMetrics(bytesStored, diskSize, diskFree int64) {
	snapshot := diskMetricsSnapshot{
		bytesStored: bytesStored,
		diskSize:    diskSize,
		diskFree:    diskFree,
		updatedAt:   time.Now(),
	}
	c.lastDiskMetrics.Store(snapshot)
}

func (c *Cluster) loadDiskMetrics() diskMetricsSnapshot {
	if value := c.lastDiskMetrics.Load(); value != nil {
		if snapshot, ok := value.(diskMetricsSnapshot); ok {
			return snapshot
		}
	}
	return diskMetricsSnapshot{}
}
