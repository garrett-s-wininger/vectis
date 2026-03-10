package supervisor

import (
	"os"
	"os/exec"
	"path/filepath"
)

func FindBinary(name string) (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", err
	}

	dir := filepath.Dir(self)
	binaryPath := filepath.Join(dir, name)

	if info, err := os.Stat(binaryPath); err == nil && !info.IsDir() {
		return binaryPath, nil
	}

	return exec.LookPath(name)
}
