package main

import "log"

func logInvalidDiscoveryMode(mode string) {
	log.Fatalf("Invalid discovery mode %q: expected udp or local", mode)
}

func logDiscoveryModeConfigurationFailure(mode string, err error) {
	log.Fatalf("Cannot configure discovery mode %q: %v", mode, err)
}
