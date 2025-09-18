package main

import "errors"

// Shared sentinel errors for filesystem operations.
var (
	ErrFileNotFound = errors.New("file not found")
	ErrIsDirectory  = errors.New("path is a directory")
)
