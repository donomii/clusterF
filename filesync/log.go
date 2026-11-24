package filesync

func (e *Syncer) logWatchFailure(operation, path string, err error) {
	e.logger.Printf("[EXPORT] Failed to %s %s: %v", operation, path, err)
}
