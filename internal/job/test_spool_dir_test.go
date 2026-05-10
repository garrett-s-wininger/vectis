package job

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "vectis-job-spool-test-*")
	if err != nil {
		panic(err)
	}

	SetLogSpoolDirForTest(dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}
