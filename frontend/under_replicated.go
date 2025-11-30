package frontend

import "net/http"

// HandleUnderReplicatedPage serves the under-replicated report.
func (f *Frontend) HandleUnderReplicatedPage(w http.ResponseWriter, r *http.Request) {
	content, err := loadAsset("under-replicated.html")
	if err != nil {
		http.Error(w, "Under-replicated page not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(content)
}

// HandleUnderReplicatedJS serves the under-replicated report JavaScript.
func (f *Frontend) HandleUnderReplicatedJS(w http.ResponseWriter, r *http.Request) {
	content, err := loadAsset("under-replicated.js")
	if err != nil {
		http.Error(w, "Under-replicated JS not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Write(content)
}
