package discovery

import (
	"context"
	"fmt"
	"net"
	"sync"
)

var localDiscovery = struct {
	sync.Mutex
	listeners map[int]map[*DiscoveryManager]*net.UDPAddr
}{listeners: make(map[int]map[*DiscoveryManager]*net.UDPAddr)}

func (dm *DiscoveryManager) openSockets() error {
	var listener, sender *net.UDPConn
	var err error
	switch dm.mode {
	case "local":
		listener, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err == nil {
			sender, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		}
	case "udp":
		var packet net.PacketConn
		packet, err = newListenConfig().ListenPacket(context.Background(), "udp", fmt.Sprintf(":%d", dm.broadcastPort))
		if err == nil {
			var ok bool
			listener, ok = packet.(*net.UDPConn)
			if !ok {
				packet.Close()
				return fmt.Errorf("discovery listener on port %d is not a UDP connection", dm.broadcastPort)
			}
			sender, err = net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4bcast, Port: dm.broadcastPort})
		}
	default:
		return fmt.Errorf("discovery mode %q is invalid: expected udp or local", dm.mode)
	}
	if err != nil {
		if listener != nil {
			listener.Close()
		}
		return fmt.Errorf("cannot open %s discovery sockets for node %s on port %d: %w", dm.mode, dm.nodeID, dm.broadcastPort, err)
	}
	dm.mu.Lock()
	dm.listenConn, dm.broadcastConn = listener, sender
	dm.mu.Unlock()
	if dm.mode == "local" {
		registerLocalDiscovery(dm, listener.LocalAddr().(*net.UDPAddr))
	}
	return nil
}

func registerLocalDiscovery(manager *DiscoveryManager, address *net.UDPAddr) {
	localDiscovery.Lock()
	defer localDiscovery.Unlock()
	listeners := localDiscovery.listeners[manager.broadcastPort]
	if listeners == nil {
		listeners = make(map[*DiscoveryManager]*net.UDPAddr)
		localDiscovery.listeners[manager.broadcastPort] = listeners
	}
	listeners[manager] = address
}

func unregisterLocalDiscovery(manager *DiscoveryManager) {
	localDiscovery.Lock()
	defer localDiscovery.Unlock()
	listeners := localDiscovery.listeners[manager.broadcastPort]
	delete(listeners, manager)
	if len(listeners) == 0 {
		delete(localDiscovery.listeners, manager.broadcastPort)
	}
}

func localDiscoveryAddresses(port int) []*net.UDPAddr {
	localDiscovery.Lock()
	defer localDiscovery.Unlock()
	var addresses []*net.UDPAddr
	for _, address := range localDiscovery.listeners[port] {
		addresses = append(addresses, address)
	}
	return addresses
}
