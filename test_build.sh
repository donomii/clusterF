#!/bin/bash
cd /Users/user/git/clusterF
go build . 2>&1
echo "Build completed with exit code: $?"
