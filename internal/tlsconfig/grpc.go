package tlsconfig

import (
	"google.golang.org/grpc/credentials"
)

func (r *Reloader) ServerGRPC() (credentials.TransportCredentials, error) {
	cfg, err := r.ServerTLS()
	if err != nil {
		return nil, err
	}

	return credentials.NewTLS(cfg), nil
}

func (r *Reloader) ClientGRPC(serverName string) (credentials.TransportCredentials, error) {
	cfg, err := r.ClientTLS(serverName)
	if err != nil {
		return nil, err
	}

	return credentials.NewTLS(cfg), nil
}
