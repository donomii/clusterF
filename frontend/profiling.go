package frontend

import "net/http"

// HandleProfilingPage serves the profiling control page.
func (f *Frontend) HandleProfilingPage(w http.ResponseWriter, r *http.Request) {
	content, err := loadAsset("profiling.html")
	if err != nil {
		http.Error(w, "Profiling page not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(content)
}

// HandleProfilingJS serves the profiling JavaScript.
func (f *Frontend) HandleProfilingJS(w http.ResponseWriter, r *http.Request) {
	content, err := loadAsset("profiling.js")
	if err != nil {
		http.Error(w, "Profiling JS not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Write(content)
}
