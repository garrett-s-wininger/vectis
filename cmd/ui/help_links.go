package main

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const defaultDocsURL = "https://vectis.dev/"

func (b *uiBackend) configureDocs(rawURL string, port int) error {
	if port < 0 || port > 65535 {
		return fmt.Errorf("docs port must be between 0 and 65535")
	}

	b.docsPort = port
	if rawURL = strings.TrimSpace(rawURL); rawURL != "" {
		u, err := url.Parse(rawURL)
		if err != nil {
			return fmt.Errorf("invalid docs URL: %w", err)
		}

		relative := u.Scheme == "" && u.Host == "" && strings.HasPrefix(rawURL, "/") && !strings.HasPrefix(rawURL, "//")
		absolute := (u.Scheme == "https" || u.Scheme == "http") && u.Hostname() != ""
		if (!relative && !absolute) || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || strings.Contains(u.Path, "\\") {
			return fmt.Errorf("docs URL must be an HTTP(S) site URL or a same-origin absolute path")
		}

		b.docsURL = strings.TrimRight(u.String(), "/") + "/"
	}

	return nil
}

func (b *uiBackend) browserDocsURL(r *http.Request) string {
	if b.docsURL != "" {
		return b.docsURL
	}

	if b.docsPort == 0 {
		return defaultDocsURL
	}

	origin := b.publicURL
	if origin == nil {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}

		origin = &url.URL{Scheme: scheme, Host: r.Host}
	}

	return (&url.URL{Scheme: origin.Scheme, Host: net.JoinHostPort(origin.Hostname(), strconv.Itoa(b.docsPort)), Path: "/"}).String()
}
