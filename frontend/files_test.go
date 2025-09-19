package frontend

import (
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"
)

func TestHandleFilesServesEmbeddedHTML(t *testing.T) {
    ui := New(stubProvider{})
    rr := httptest.NewRecorder()
    req := httptest.NewRequest(http.MethodGet, "/files/", nil)

    ui.HandleFiles(rr, req)

    if rr.Code != http.StatusOK {
        t.Fatalf("expected status 200, got %d", rr.Code)
    }
    body := rr.Body.String()
    if body == "" {
        t.Fatalf("empty response body")
    }
    if !strings.Contains(body, "Cluster File Browser") {
        t.Fatalf("unexpected files page body: %s", body[:min(len(body), 60)])
    }
}
