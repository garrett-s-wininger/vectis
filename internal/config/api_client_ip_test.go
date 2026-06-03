package config

import (
	"crypto/tls"
	"net"
	"net/http"
	"testing"
)

func TestParseTrustedProxyIPNets_singleIPv4(t *testing.T) {
	t.Parallel()
	nets, err := ParseTrustedProxyIPNets([]string{"10.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}

	if len(nets) != 1 || !nets[0].Contains(net.ParseIP("10.0.0.1")) || nets[0].Contains(net.ParseIP("10.0.0.2")) {
		t.Fatalf("unexpected net: %#v", nets[0])
	}
}

func TestParseTrustedProxyIPNets_rejectsTrustAll(t *testing.T) {
	t.Parallel()
	for _, c := range []string{"0.0.0.0/0", "::/0"} {
		if _, err := ParseTrustedProxyIPNets([]string{c}); err == nil {
			t.Fatalf("expected error for %q", c)
		}
	}
}

func TestHTTPClientIP_ignoresForwardedWhenNotTrusted(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	req := mustNewRequest(t)
	req.RemoteAddr = "198.51.100.1:1234"
	req.Header.Set("X-Forwarded-For", "203.0.113.1")

	if got := HTTPClientIP(req); got != "198.51.100.1" {
		t.Fatalf("got %q want direct peer", got)
	}
}

func TestHTTPClientIP_usesForwardedWhenTrusted(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	req := mustNewRequest(t)
	req.RemoteAddr = "10.0.0.2:443"
	req.Header.Set("X-Forwarded-For", "203.0.113.1, 10.0.0.2")

	if got := HTTPClientIP(req); got != "203.0.113.1" {
		t.Fatalf("got %q want forwarded client", got)
	}
}

func TestHTTPClientIP_xRealIPFallback(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	req := mustNewRequest(t)
	req.RemoteAddr = "10.0.0.2:443"
	req.Header.Set("X-Real-IP", "203.0.113.2")

	if got := HTTPClientIP(req); got != "203.0.113.2" {
		t.Fatalf("got %q want X-Real-IP", got)
	}
}

func TestHTTPClientIP_xffSkipsInvalidUsesFirstValid(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	req := mustNewRequest(t)
	req.RemoteAddr = "10.0.0.2:443"
	req.Header.Set("X-Forwarded-For", "not-an-ip, 203.0.113.9, 198.51.100.1")

	if got := HTTPClientIP(req); got != "203.0.113.9" {
		t.Fatalf("got %q want first parsable XFF entry", got)
	}
}

func TestHTTPClientIP_xffBracketHostPort(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	req := mustNewRequest(t)
	req.RemoteAddr = "10.0.0.2:443"
	req.Header.Set("X-Forwarded-For", "[2001:db8::1]:443")

	if got := HTTPClientIP(req); got != "2001:db8::1" {
		t.Fatalf("got %q want bracketed IPv6 from host:port", got)
	}
}

func TestHTTPClientIP_trustedAllXffInvalidFallsBackToDirect(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	req := mustNewRequest(t)
	req.RemoteAddr = "10.0.0.2:443"
	req.Header.Set("X-Forwarded-For", "bogus, also-bogus")
	req.Header.Set("X-Real-IP", "not-valid")

	if got := HTTPClientIP(req); got != "10.0.0.2" {
		t.Fatalf("got %q want direct peer when forwarded headers unusable", got)
	}
}

func TestHTTPOriginalRequestSecure_directTLS(t *testing.T) {
	req := mustNewRequest(t)
	req.TLS = &tls.ConnectionState{}
	req.Header.Set("X-Forwarded-Proto", "http")

	if !HTTPOriginalRequestSecure(req) {
		t.Fatal("direct TLS request should be secure")
	}
}

func TestHTTPOriginalRequestSecure_ignoresForwardedProtoWithoutTrustedProxy(t *testing.T) {
	req := mustNewRequest(t)
	req.RemoteAddr = "198.51.100.1:1234"
	req.Header.Set("X-Forwarded-Proto", "https")

	if HTTPOriginalRequestSecure(req) {
		t.Fatal("unconfigured trusted proxies should not make forwarded proto trusted")
	}
}

func TestHTTPOriginalRequestSecure_ignoresForwardedProtoFromUntrustedPeer(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	req := mustNewRequest(t)
	req.RemoteAddr = "198.51.100.1:1234"
	req.Header.Set("X-Forwarded-Proto", "https")

	if HTTPOriginalRequestSecure(req) {
		t.Fatal("untrusted peer should not make forwarded proto trusted")
	}
}

func TestHTTPOriginalRequestSecure_usesXForwardedProtoFromTrustedProxy(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	req := mustNewRequest(t)
	req.RemoteAddr = "10.0.0.2:443"
	req.Header.Set("X-Forwarded-Proto", "HTTPS, http")

	if !HTTPOriginalRequestSecure(req) {
		t.Fatal("trusted X-Forwarded-Proto=https should mark original request secure")
	}
}

func TestHTTPOriginalRequestSecure_normalizesQuotedXForwardedProtoFromTrustedProxy(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	req := mustNewRequest(t)
	req.RemoteAddr = "10.0.0.2:443"
	req.Header.Set("X-Forwarded-Proto", ` "HTTPS" , http`)

	if !HTTPOriginalRequestSecure(req) {
		t.Fatal("trusted quoted X-Forwarded-Proto=https should mark original request secure")
	}
}

func TestHTTPOriginalRequestSecure_usesForwardedProtoFromTrustedProxy(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	req := mustNewRequest(t)
	req.RemoteAddr = "10.0.0.2:443"
	req.Header.Set("Forwarded", `for=203.0.113.5; proto="https"; host=api.example`)

	if !HTTPOriginalRequestSecure(req) {
		t.Fatal("trusted Forwarded proto=https should mark original request secure")
	}
}

func TestHTTPOriginalRequestSecure_httpForwardedProtoFromTrustedProxy(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "10.0.0.0/8")

	req := mustNewRequest(t)
	req.RemoteAddr = "10.0.0.2:443"
	req.Header.Set("X-Forwarded-Proto", "http")

	if HTTPOriginalRequestSecure(req) {
		t.Fatal("trusted X-Forwarded-Proto=http should not mark original request secure")
	}
}

func TestValidateAPIClientIPConfig_invalidEnv(t *testing.T) {
	t.Setenv(envAPIClientIPTrustedProxyCIDRs, "not-a-cidr")
	if err := ValidateAPIClientIPConfig(); err == nil {
		t.Fatal("expected error for invalid trusted proxy CIDR env")
	}
}

func TestParseTrustedProxyIPNets_cidr(t *testing.T) {
	t.Parallel()
	nets, err := ParseTrustedProxyIPNets([]string{"192.0.2.0/24"})
	if err != nil {
		t.Fatal(err)
	}
	if len(nets) != 1 || !nets[0].Contains(net.ParseIP("192.0.2.1")) || nets[0].Contains(net.ParseIP("192.0.3.1")) {
		t.Fatalf("unexpected net: %#v", nets[0])
	}
}

func mustNewRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}
