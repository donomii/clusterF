package main

import (
	"errors"

	"github.com/donomii/clusterF/partitionmanager"
)

// Shared sentinel errors for filesystem operations.
var (
	ErrFileNotFound = partitionmanager.ErrFileNotFound
	ErrIsDirectory  = errors.New("path is a directory")
)
