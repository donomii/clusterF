module github.com/donomii/clusterF

go 1.25.1

require (
	github.com/donomii/clusterF/syncmap v0.0.0
	github.com/donomii/ensemblekv v0.0.0-20250427181959-74bb6dcf7c35
	github.com/donomii/frogpond v0.0.0-20250913173135-4515c9c7f901
	github.com/webview/webview_go v0.0.0-20240831120633-6173450d4dd6
	golang.org/x/sys v0.30.0
)

require github.com/fsnotify/fsnotify v1.7.0 // indirect

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
	github.com/donomii/clusterF/frontend v0.0.0-20250923003059-d35f4c0bb184 // indirect
	github.com/donomii/go-webdav v0.0.0-20240304173232-8e8f80405603 // indirect
	github.com/donomii/goof v0.0.0-20241124064022-84f417f466df // indirect
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/iand/gonudb v0.4.4 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/mostlygeek/arp v0.0.0-20170424181311-541a2129847a // indirect
	github.com/recoilme/pudge v1.0.3 // indirect
	go.etcd.io/bbolt v1.3.11 // indirect
	golang.org/x/sync v0.17.0 // indirect
)
