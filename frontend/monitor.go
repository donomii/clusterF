package frontend

import "net/http"

// HandleMonitorDashboard serves the cluster monitoring dashboard.
func (f *Frontend) HandleMonitorDashboard(w http.ResponseWriter, r *http.Request) {
	content, err := loadAsset("monitor.html")
	if err != nil {
		http.Error(w, "Monitor page not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(content)
}

// HandleMonitorJS serves the monitor JavaScript.
func (f *Frontend) HandleMonitorJS(w http.ResponseWriter, r *http.Request) {
	content, err := loadAsset("monitor.js")
	if err != nil {
		http.Error(w, "Monitor JS not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Write(content)
}
