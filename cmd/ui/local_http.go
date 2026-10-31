package main

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const uiLocalSessionCookieName = "vectis_ui_local_session"

func (b *uiBackend) configureLocalHTTP(bindHost string, allow bool) error {
	if b.allowNetworkHTTP {
		if b.publicURL == nil || b.publicURL.Scheme != "http" {
			return fmt.Errorf("--allow-insecure-http requires an explicit HTTP --public-url")
		}

		b.allowLocalHTTPCookies = true
		return nil
	}

	if allow {
		if !loopbackHost(bindHost) {
			return fmt.Errorf("insecure UI session cookies require a loopback bind host")
		}

		if b.publicURL != nil && (b.publicURL.Scheme != "http" || !loopbackHost(b.publicURL.Hostname())) {
			return fmt.Errorf("insecure UI session cookies require a local HTTP public URL")
		}
	}

	b.allowLocalHTTPCookies = allow
	return nil
}

func loopbackHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	if host == "localhost" {
		return true
	}

	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (b *uiBackend) localHTTPRequest(r *http.Request) bool {
	if !b.allowLocalHTTPCookies || r.TLS != nil {
		return false
	}

	if b.allowNetworkHTTP {
		return b.publicURL != nil && b.publicURL.Scheme == "http" && sameBrowserOrigin(&url.URL{Scheme: "http", Host: r.Host}, b.publicURL)
	}

	host := (&url.URL{Host: r.Host}).Hostname()
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	return err == nil && loopbackHost(host) && loopbackHost(peer)
}

func (b *uiBackend) sessionCookie(r *http.Request) *http.Cookie {
	name, secure := uiSessionCookieName, true
	if b.localHTTPRequest(r) {
		name, secure = uiLocalSessionCookieName, false
	}

	// #nosec G124 -- Only explicit development HTTP policies select Secure=false; network demos require a fixed origin and separate opt-in. HTTPS stays Secure.
	return &http.Cookie{Name: name, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: secure}
}
