//go:build unix

package actionlauncher

import (
	"runtime"

	"golang.org/x/sys/unix"
)

const privateActionUmask = 0o077

func prepare() error {
	runtime.LockOSThread()
	if err := resetSignalDispositions(); err != nil {
		return err
	}

	if err := resetSignalMask(); err != nil {
		return err
	}

	unix.Umask(privateActionUmask)
	return nil
}

func execTarget(target string, argv, env []string) error {
	return unix.Exec(target, argv, env)
}
