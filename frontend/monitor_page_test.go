package frontend

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type stubProvider struct {
	nodeID        string
	httpPort      int
	discoveryPort int
	dataDir       string
}

func (s stubProvider) NodeID() string        { return s.nodeID }
func (s stubProvider) HTTPPort() int         { return s.httpPort }
func (s stubProvider) DiscoveryPortVal() int { return s.discoveryPort }
func (s stubProvider) DataDirPath() string   { return s.dataDir }

func TestMonitorPageHTMLSnapshot(t *testing.T) {
	ui := New(stubProvider{
		nodeID:        "test-node",
		httpPort:      1234,
		discoveryPort: 4567,
		dataDir:       "/tmp/data",
	})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/monitor", nil)

	ui.HandleMonitorDashboard(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	html := rr.Body.String()
	if len(html) == 0 {
		t.Fatalf("monitor HTML empty")
	}

	t.Logf("monitor html preview: %s", truncate(html, 120))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
