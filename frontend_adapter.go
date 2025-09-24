package main

// NodeID returns the string identifier for the cluster node.
func (c *Cluster) NodeID() string { return string(c.NodeId) }

// HTTPPort returns the HTTP data port in use.
func (c *Cluster) HTTPPort() int { return c.HTTPDataPort }

// DiscoveryPort exposes the UDP discovery port.
func (c *Cluster) DiscoveryPortVal() int { return c.DiscoveryPort }

// DataDirPath returns the configured data directory path.
func (c *Cluster) DataDirPath() string { return c.DataDir }
