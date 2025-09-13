#!/bin/bash

set -euo pipefail

# Optional system package hints (uncomment and adjust per distro)
# sudo xbps-install pkg-config gtk+3-devel webkit2gtk-devel webkit2gtk-common webkit2gtk libwebkit2gtk41-devel libwebkit2gtk41

# Version metadata for embedding. Override by exporting env vars before running.
VERSION=${VERSION:-v0.1.0}
COMMIT=${COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}
DATE=${DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}

LDFLAGS="-X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}"

echo "Building clusterF ${VERSION} (${COMMIT} @ ${DATE})"
go build -ldflags "$LDFLAGS" . > build_output.txt 2>&1
cat build_output.txt
