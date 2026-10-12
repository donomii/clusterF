package partitionmanager

import "github.com/donomii/clusterF/types"

func (pm *PartitionManager) logPartitionReindexFailure(partitionID types.PartitionID, err error) {
	pm.logf("[PARTITION] Node %s could not reindex partition %s; retaining holder state and retrying: %v", pm.deps.NodeID, partitionID, err)
}
