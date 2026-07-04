package localpki

import (
	"fmt"
	"os"
	"path/filepath"

	"vectis/internal/platform"
)

func Load(dir string) (*Material, error) {
	caCertPath := filepath.Join(dir, caCertFile)
	srvCertPath := filepath.Join(dir, serverCertFile)
	srvKeyPath := filepath.Join(dir, serverKeyFile)

	for _, p := range []string{caCertPath, srvCertPath, srvKeyPath} {
		if _, err := os.Stat(p); err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("localpki: missing %q; run vectis-local init first", p)
			}

			return nil, fmt.Errorf("localpki: stat %q: %w", p, err)
		}
	}

	return &Material{
		CAFile:     caCertPath,
		ServerCert: srvCertPath,
		ServerKey:  srvKeyPath,
	}, nil
}

func (m *Material) ServerTrustedBySystem() (bool, error) {
	if m == nil {
		return false, nil
	}

	return platform.ServerCertificateTrusted(m.ServerCert, "localhost")
}
