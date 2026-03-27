package utils

import (
	"os"
	"path/filepath"
)

func DataHome() string {
	if dataHome := os.Getenv("XDG_DATA_HOME"); dataHome != "" {
		return dataHome
	}

	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".local", "share")
	}

	return filepath.Join(os.TempDir(), ".local", "share")
}
