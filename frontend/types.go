package frontend

// MetadataProvider exposes the cluster metadata used by the UI handlers.
type MetadataProvider interface {
	NodeID() string
	HTTPPort() int
	DiscoveryPortVal() int
	DataDirPath() string
}

// Frontend hosts the HTTP handlers for the HTML UI.
type Frontend struct {
	provider MetadataProvider
}

// New returns a frontend bound to the given metadata provider.
func New(provider MetadataProvider) *Frontend {
	return &Frontend{provider: provider}
}
