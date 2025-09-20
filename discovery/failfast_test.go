package discovery

import (
	"flag"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	_ = flag.Set("test.failfast", "true")
	os.Exit(m.Run())
}
