//go:build !(linux || darwin || freebsd)

package workercore

import "net"

func unixPeerUID(net.Conn) (int, bool, error) {
	return 0, false, nil
}
