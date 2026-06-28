//go:build unix && !darwin && !(linux && (amd64 || arm64))

package actionlauncher

import "syscall"

func resetSignalDisposition(syscall.Signal) error {
	return nil
}
