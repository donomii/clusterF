module github.com/donomii/clusterF

go 1.25.1

require (
	github.com/donomii/clusterF/frontend v0.0.0-20250923003059-d35f4c0bb184
	github.com/donomii/clusterF/syncmap v0.0.0
	github.com/donomii/frogpond v0.0.0-20251018170253-f2571ed50b51
	github.com/donomii/go-webdav v0.0.0-20240304173232-8e8f80405603
	github.com/webview/webview_go v0.0.0-20240831120633-6173450d4dd6
	golang.org/x/sys v0.30.0
)

require (
	github.com/donomii/ensemblekv v0.0.0-20251018202937-37d3d95ea284
	github.com/fsnotify/fsnotify v1.7.0
	github.com/tchap/go-patricia v1.0.1
)

replace github.com/donomii/clusterF/exporter => ./exporter

replace github.com/donomii/clusterF/frontend => ./frontend

replace github.com/donomii/clusterF/syncmap => ./syncmap

replace github.com/donomii/clusterF/filesystem => ./filesystem

replace github.com/donomii/clusterF/types => ./types

replace github.com/donomii/clusterF/partitionmanager => ./partitionmanager

replace github.com/donomii/clusterF/webdav => ./webdav

require (
	github.com/OneOfOne/xxhash v1.2.8 // indirect
	github.com/aws/aws-sdk-go v1.55.7 // indirect
	github.com/donomii/goof v0.0.0-20241124064022-84f417f466df // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/iand/gonudb v0.4.4 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mostlygeek/arp v0.0.0-20170424181311-541a2129847a // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/petermattis/goid v0.0.0-20250813065127-a731cc31b4fe // indirect
	github.com/recoilme/pudge v1.0.3 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/sasha-s/go-deadlock v0.3.6 // indirect
	go.etcd.io/bbolt v1.3.11 // indirect
	golang.org/x/sync v0.17.0 // indirect
	modernc.org/gc/v3 v3.0.0-20240107210532-573471604cb6 // indirect
	modernc.org/libc v1.55.3 // indirect
	modernc.org/mathutil v1.6.0 // indirect
	modernc.org/memory v1.8.0 // indirect
	modernc.org/sqlite v1.32.0 // indirect
	modernc.org/strutil v1.2.0 // indirect
	modernc.org/token v1.1.0 // indirect
)
