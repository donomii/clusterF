module github.com/donomii/clusterF/exporter

go 1.25.1

require (
	github.com/donomii/clusterF/syncmap v0.0.0
	github.com/fsnotify/fsnotify v1.7.0
)

require golang.org/x/sys v0.4.0 // indirect

replace github.com/donomii/clusterF/syncmap => ../syncmap
