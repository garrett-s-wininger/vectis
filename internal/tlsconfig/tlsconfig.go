package tlsconfig

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

const defaultMinVersion = tls.VersionTLS12

type Options struct {
	ServerCert string
	ServerKey  string
	RootCA     string
	ClientCA   string
	ClientCert string
	ClientKey  string
	MinVersion uint16
}

type Reloader struct {
	opts       Options
	mu         sync.RWMutex
	serverLeaf *tls.Certificate
	clientCAs  *x509.CertPool
	rootCAs    *x509.CertPool
	clientLeaf *tls.Certificate
}

func NewReloader(opts Options) (*Reloader, error) {
	if opts.MinVersion == 0 {
		opts.MinVersion = defaultMinVersion
	}

	r := &Reloader{opts: opts}
	if err := r.Reload(); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Reloader) Reload() error {
	next, err := loadSnapshot(r.opts)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.serverLeaf = next.serverLeaf
	r.clientCAs = next.clientCAs
	r.rootCAs = next.rootCAs
	r.clientLeaf = next.clientLeaf
	r.mu.Unlock()

	return nil
}

type snapshot struct {
	serverLeaf *tls.Certificate
	clientCAs  *x509.CertPool
	rootCAs    *x509.CertPool
	clientLeaf *tls.Certificate
}

func loadSnapshot(opts Options) (*snapshot, error) {
	minV := opts.MinVersion
	if minV == 0 {
		minV = defaultMinVersion
	}

	s := &snapshot{}

	if opts.ServerCert != "" || opts.ServerKey != "" {
		if opts.ServerCert == "" || opts.ServerKey == "" {
			return nil, errors.New("tlsconfig: server cert and key paths must both be set")
		}

		cert, err := tls.LoadX509KeyPair(opts.ServerCert, opts.ServerKey)
		if err != nil {
			return nil, fmt.Errorf("tlsconfig: load server key pair: %w", err)
		}

		s.serverLeaf = &cert
	}

	if opts.ClientCA != "" {
		b, err := os.ReadFile(opts.ClientCA)
		if err != nil {
			return nil, fmt.Errorf("tlsconfig: read client CA: %w", err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("tlsconfig: parse client CA PEM %q", opts.ClientCA)
		}

		s.clientCAs = pool
	}

	if opts.RootCA != "" {
		b, err := os.ReadFile(opts.RootCA)
		if err != nil {
			return nil, fmt.Errorf("tlsconfig: read root CA: %w", err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("tlsconfig: parse root CA PEM %q", opts.RootCA)
		}

		s.rootCAs = pool
	}

	if opts.ClientCert != "" || opts.ClientKey != "" {
		if opts.ClientCert == "" || opts.ClientKey == "" {
			return nil, errors.New("tlsconfig: client cert and key paths must both be set")
		}

		cert, err := tls.LoadX509KeyPair(opts.ClientCert, opts.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("tlsconfig: load client key pair: %w", err)
		}

		s.clientLeaf = &cert
	}

	return s, nil
}

func (r *Reloader) ServerTLS() (*tls.Config, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.serverLeaf == nil {
		return nil, errors.New("tlsconfig: no server certificate loaded (set ServerCert/ServerKey)")
	}

	minV := r.opts.MinVersion
	if minV == 0 {
		minV = defaultMinVersion
	}

	cfg := &tls.Config{
		MinVersion: minV,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			r.mu.RLock()
			defer r.mu.RUnlock()

			if r.serverLeaf == nil {
				return nil, errors.New("tlsconfig: no server certificate")
			}

			return r.serverLeaf, nil
		},
	}

	if r.clientCAs != nil {
		cfg.ClientCAs = r.clientCAs
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		cfg.ClientAuth = tls.NoClientCert
	}

	return cfg, nil
}

func (r *Reloader) ClientTLS(serverName string) (*tls.Config, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	minV := r.opts.MinVersion
	if minV == 0 {
		minV = defaultMinVersion
	}

	cfg := &tls.Config{
		MinVersion: minV,
		RootCAs:    r.rootCAs,
		ServerName: serverName,
	}

	if r.clientLeaf != nil {
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			r.mu.RLock()
			defer r.mu.RUnlock()

			if r.clientLeaf == nil {
				return nil, nil
			}

			return r.clientLeaf, nil
		}
	}
	return cfg, nil
}

func (r *Reloader) RunReloadLoop(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("tlsconfig: reload interval must be positive")
	}

	paths := r.watchPaths()
	if len(paths) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}

	last := make(map[string]time.Time)
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("tlsconfig: stat %q: %w", p, err)
		}

		last[p] = fi.ModTime()
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			changed := false
			for _, p := range paths {
				fi, err := os.Stat(p)
				if err != nil {
					continue
				}

				if fi.ModTime().After(last[p]) {
					changed = true
				}
			}

			if !changed {
				continue
			}

			if err := r.Reload(); err != nil {
				continue
			}

			for _, p := range paths {
				if fi, err := os.Stat(p); err == nil {
					last[p] = fi.ModTime()
				}
			}
		}
	}
}

func (r *Reloader) watchPaths() []string {
	var out []string
	o := r.opts
	add := func(p string) {
		if p != "" {
			out = append(out, p)
		}
	}

	add(o.ServerCert)
	add(o.ServerKey)
	add(o.RootCA)
	add(o.ClientCA)
	add(o.ClientCert)
	add(o.ClientKey)

	return out
}
