package main

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"vectis/internal/tlsconfig"
)

func (b *uiBackend) configureTransport(publicURL, caFile string) error {
	if raw := strings.TrimSpace(publicURL); raw != "" {
		u, err := url.Parse(raw)
		if err != nil {
			return fmt.Errorf("invalid UI public URL: %w", err)
		}

		ip := net.ParseIP(u.Hostname())
		local := u.Hostname() == "localhost" || (ip != nil && ip.IsLoopback())
		if u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.Opaque != "" ||
			(u.Scheme != "https" && (u.Scheme != "http" || (!local && !b.allowNetworkHTTP))) {
			return fmt.Errorf("UI public URL must be an HTTPS origin (network HTTP requires --allow-insecure-http)")
		}

		b.publicURL = u
	}

	if caFile = strings.TrimSpace(caFile); caFile != "" {
		if b.apiURL.Scheme != "https" {
			return fmt.Errorf("API CA file requires an HTTPS API URL")
		}

		reloader, err := tlsconfig.NewReloader(tlsconfig.Options{RootCA: caFile})
		if err != nil {
			return err
		}

		cfg, err := reloader.ClientTLS(b.apiURL.Hostname())
		if err != nil {
			return err
		}

		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = cfg
		b.httpClient.Transport = transport
	}

	return nil
}
