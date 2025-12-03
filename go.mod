module github.com/donomii/clusterF

go 1.25.1

require (
	github.com/donomii/clusterF/frontend v0.0.0-20251203152000-0c578d3a7344
	github.com/donomii/clusterF/syncmap v0.0.0
	github.com/donomii/frogpond v0.0.0-20251203153250-eb57110e8ed3
	github.com/donomii/go-webdav v0.0.0-20240304173232-8e8f80405603
	github.com/webview/webview_go v0.0.0-20240831120633-6173450d4dd6
	golang.org/x/sys v0.38.0
)

require (
	github.com/donomii/ensemblekv v0.0.0-20251101201742-ddcb082cecfa
	github.com/fsnotify/fsnotify v1.9.0
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
	github.com/aws/aws-sdk-go v1.55.8 // indirect
	github.com/donomii/goof v0.0.0-20241124064022-84f417f466df // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/iand/gonudb v0.4.4 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mostlygeek/arp v0.0.0-20170424181311-541a2129847a // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/petermattis/goid v0.0.0-20251121121749-a11dd1a45f9a // indirect
	github.com/recoilme/pudge v1.0.3 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/sasha-s/go-deadlock v0.3.6 // indirect
	go.etcd.io/bbolt v1.4.3 // indirect
	golang.org/x/exp v0.0.0-20251125195548-87e1e737ad39 // indirect
	golang.org/x/sync v0.18.0 // indirect
	modernc.org/libc v1.67.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	modernc.org/sqlite v1.40.1 // indirect
)
