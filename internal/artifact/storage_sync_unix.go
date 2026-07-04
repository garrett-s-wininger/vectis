//go:build !windows

package artifact

import "os"

func syncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(f)

	return f.Sync()
}
