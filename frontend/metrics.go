package frontend

import "net/http"

// HandleMetricsPage serves the metrics dashboard.
func (f *Frontend) HandleMetricsPage(w http.ResponseWriter, r *http.Request) {
	content, err := loadAsset("metrics.html")
	if err != nil {
		http.Error(w, "Metrics page not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(content)
}

// HandleMetricsJS serves the metrics dashboard JavaScript.
func (f *Frontend) HandleMetricsJS(w http.ResponseWriter, r *http.Request) {
	content, err := loadAsset("metrics.js")
	if err != nil {
		http.Error(w, "Metrics JS not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Write(content)
}
