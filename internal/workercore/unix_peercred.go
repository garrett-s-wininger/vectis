package workercore

import (
	"fmt"
	"net"
)

type peerCredentialListener struct {
	net.Listener
	allowedUID int
}

func newPeerCredentialListener(listener net.Listener, allowedUID int) net.Listener {
	if listener == nil {
		return nil
	}

	return &peerCredentialListener{
		Listener:   listener,
		allowedUID: allowedUID,
	}
}

func (l *peerCredentialListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}

		if err := verifyUnixPeerCredentials(conn, l.allowedUID); err != nil {
			_ = conn.Close()
			continue
		}

		return conn, nil
	}
}

func verifyUnixPeerCredentials(conn net.Conn, allowedUID int) error {
	if allowedUID < 0 {
		return nil
	}

	uid, ok, err := unixPeerUID(conn)
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	if uid != allowedUID {
		return fmt.Errorf("unix peer uid %d does not match allowed uid %d", uid, allowedUID)
	}

	return nil
}
