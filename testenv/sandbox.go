package testenv

import (
	"os"
	"strings"
	"sync"
	"testing"
)

var (
	sandboxOnce sync.Once
	sandbox     bool
)

// IsSandboxed reports whether tests should avoid features that rely on UDP broadcast/listen.
func IsSandboxed() bool {
	sandboxOnce.Do(func() {
		sandbox = detectSandbox()
	})
	return sandbox
}

// RequireUDPSupport skips the calling test when UDP networking is not available.
func RequireUDPSupport(tb testing.TB) {
	tb.Helper()
	if IsSandboxed() {
		tb.Skip("skipping UDP-dependent test in sandboxed environment (set CLUSTERF_SANDBOX=0 to override)")
	}
}

func detectSandbox() bool {
	if val, ok := os.LookupEnv("CLUSTERF_SANDBOX"); ok {
		return isTruthy(val)
	}

	// Known Codex/CI hints
	for _, key := range []string{
		"CODEX_SANDBOX_NETWORK_DISABLED",
		"CODEX_SANDBOX",
		"SANDBOX_MODE",
		"SANDBOX",
		"APP_SANDBOX_CONTAINER_ID",
	} {
		if val, ok := os.LookupEnv(key); ok {
			return isTruthy(val)
		}
	}

	if os.Getenv("CI") != "" && strings.EqualFold(os.Getenv("RUNNER_OS"), "macOS") {
		return true
	}

	return false
}

func isTruthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "0", "false", "no", "off":
		return false
	default:
		return true
	}
}
