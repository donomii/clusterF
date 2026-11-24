package discovery

import "net"

func (dm *DiscoveryManager) logLocalAnnouncementFailure(address *net.UDPAddr, err error) {
	dm.Debugf("Local discovery announcement from %s to %s failed: %v", dm.nodeID, address, err)
}
