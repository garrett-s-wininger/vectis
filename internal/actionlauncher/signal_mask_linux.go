//go:build linux

package actionlauncher

import "golang.org/x/sys/unix"

func resetSignalMask() error {
	var empty unix.Sigset_t
	return unix.PthreadSigmask(unix.SIG_SETMASK, &empty, nil)
}
