//go:build darwin || freebsd

package workercore

import (
	"net"

	"golang.org/x/sys/unix"
)

func unixPeerUID(conn net.Conn) (int, bool, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, false, nil
	}

	rawConn, err := unixConn.SyscallConn()
	if err != nil {
		return 0, true, err
	}

	var uid int
	var controlErr error
	if err := rawConn.Control(func(fd uintptr) {
		cred, err := unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if err != nil {
			controlErr = err
			return
		}

		uid = int(cred.Uid)
	}); err != nil {
		return 0, true, err
	}

	if controlErr != nil {
		return 0, true, controlErr
	}

	return uid, true, nil
}
