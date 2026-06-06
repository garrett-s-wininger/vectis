package config

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/viper"
)

const envAPIClientIPTrustedProxyCIDRs = "VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS"

// APIClientIPTrustedProxyCIDRStrings returns configured trusted reverse-proxy CIDRs (or single-host IPs).
func APIClientIPTrustedProxyCIDRStrings() []string {
	if v := strings.TrimSpace(os.Getenv(envAPIClientIPTrustedProxyCIDRs)); v != "" {
		return splitCommaNonEmpty(v)
	}

	if viper.IsSet("api.client_ip.trusted_proxy_cidrs") {
		raw := viper.Get("api.client_ip.trusted_proxy_cidrs")
		switch x := raw.(type) {
		case []string:
			return trimNonEmptyStrings(x)
		case []any:
			out := make([]string, 0, len(x))
			for _, e := range x {
				if s, ok := e.(string); ok {
					if t := strings.TrimSpace(s); t != "" {
						out = append(out, t)
					}
				}
			}

			if len(out) > 0 {
				return out
			}
		case string:
			if t := strings.TrimSpace(x); t != "" {
				return splitCommaNonEmpty(t)
			}
		}
	}

	return trimNonEmptyStrings(MustDefaults().API.ClientIP.TrustedProxyCIDRs)
}

// ParseTrustedProxyIPNets parses trusted proxy CIDR strings into IP nets. Single IPs become /32 or /128.
func ParseTrustedProxyIPNets(cidrs []string) ([]*net.IPNet, error) {
	var out []*net.IPNet
	for _, s := range cidrs {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}

		if _, n, err := net.ParseCIDR(s); err == nil {
			if isTrustAllCIDR(n) {
				return nil, fmt.Errorf("trusted proxy CIDR %q is not allowed (use specific LB/proxy ranges, not entire address space)", s)
			}
			out = append(out, n)
			continue
		}

		ip := net.ParseIP(s)
		if ip == nil {
			return nil, fmt.Errorf("trusted proxy CIDR %q is not a valid IP or CIDR", s)
		}

		if ip4 := ip.To4(); ip4 != nil {
			n := &net.IPNet{IP: ip4, Mask: net.CIDRMask(32, 32)}
			if isTrustAllCIDR(n) {
				return nil, fmt.Errorf("trusted proxy CIDR %q is not allowed", s)
			}

			out = append(out, n)
			continue
		}

		n := &net.IPNet{IP: ip, Mask: net.CIDRMask(128, 128)}
		if isTrustAllCIDR(n) {
			return nil, fmt.Errorf("trusted proxy CIDR %q is not allowed", s)
		}

		out = append(out, n)
	}

	return out, nil
}

// ValidateAPIClientIPConfig validates trusted proxy CIDR configuration.
func ValidateAPIClientIPConfig() error {
	_, err := ParseTrustedProxyIPNets(APIClientIPTrustedProxyCIDRStrings())
	return err
}

// HTTPOriginalRequestSecure reports whether the client-facing request used HTTPS.
// Direct TLS is always trusted. Forwarded scheme headers are trusted only when
// the TCP peer is inside api.client_ip.trusted_proxy_cidrs.
func HTTPOriginalRequestSecure(r *http.Request) bool {
	if r == nil {
		return false
	}

	if r.TLS != nil {
		return true
	}

	if !requestFromTrustedProxy(r) {
		return false
	}

	return forwardedProto(r) == "https"
}

// HTTPClientIP returns the client IP used for rate limiting, audit logs, and HTTP access logs.
// When the TCP peer is in a configured trusted-proxy CIDR, the left-most valid IP from
// X-Forwarded-For is preferred, then X-Real-IP; otherwise forwarded headers are ignored.
func HTTPClientIP(r *http.Request) string {
	if r == nil {
		return ""
	}

	directHost := hostFromRemoteAddr(r.RemoteAddr)
	directIP := net.ParseIP(directHost)
	if directIP == nil {
		return directHost
	}

	if !requestFromTrustedProxy(r) {
		return directIP.String()
	}

	if len(r.Header.Values("X-Forwarded-For")) > 1 {
		return directIP.String()
	}

	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		if ip, ok := firstForwardedForIP(xff); ok {
			return ip.String()
		}

		return directIP.String()
	}

	if len(r.Header.Values("X-Real-IP")) > 1 {
		return directIP.String()
	}

	if xr := strings.TrimSpace(r.Header.Get("X-Real-IP")); xr != "" {
		if ip := forwardedIPPart(xr, false); ip != nil {
			return ip.String()
		}
	}

	return directIP.String()
}

func requestFromTrustedProxy(r *http.Request) bool {
	if r == nil {
		return false
	}

	directIP := net.ParseIP(hostFromRemoteAddr(r.RemoteAddr))
	if directIP == nil {
		return false
	}

	nets, err := ParseTrustedProxyIPNets(APIClientIPTrustedProxyCIDRStrings())
	if err != nil || len(nets) == 0 {
		return false
	}

	for _, n := range nets {
		if n.Contains(directIP) {
			return true
		}
	}

	return false
}

func firstForwardedForIP(raw string) (net.IP, bool) {
	if raw == "" || raw != strings.TrimSpace(raw) {
		return nil, false
	}

	var first net.IP
	for part := range strings.SplitSeq(raw, ",") {
		ip := forwardedIPPart(strings.TrimSpace(part), true)
		if ip == nil {
			return nil, false
		}

		if first == nil {
			first = ip
		}
	}

	if first == nil {
		return nil, false
	}

	return first, true
}

func forwardedIPPart(value string, allowPort bool) net.IP {
	if value == "" || value != strings.TrimSpace(value) || strings.ContainsAny(value, " \t\r\n\"") {
		return nil
	}

	if allowPort {
		if host, _, err := net.SplitHostPort(value); err == nil {
			value = host
		}
	}

	if strings.HasPrefix(value, "[") || strings.HasSuffix(value, "]") {
		if !strings.HasPrefix(value, "[") || !strings.HasSuffix(value, "]") {
			return nil
		}

		value = strings.TrimPrefix(strings.TrimSuffix(value, "]"), "[")
		if strings.ContainsAny(value, "[]") {
			return nil
		}
	}

	return net.ParseIP(value)
}

func forwardedProto(r *http.Request) string {
	if r == nil {
		return ""
	}

	if len(r.Header.Values("X-Forwarded-Proto")) > 1 {
		return ""
	}

	if xfp := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); xfp != "" {
		proto, ok := singleForwardedProtoValue(xfp)
		if !ok {
			return ""
		}

		return proto
	}

	if len(r.Header.Values("Forwarded")) > 1 {
		return ""
	}

	forwarded := strings.TrimSpace(r.Header.Get("Forwarded"))
	if forwarded == "" {
		return ""
	}

	if strings.Contains(forwarded, ",") {
		return ""
	}

	var proto string
	for part := range strings.SplitSeq(forwarded, ";") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "proto") {
			continue
		}

		if proto != "" {
			return ""
		}

		normalized, ok := singleForwardedProtoValue(value)
		if !ok {
			return ""
		}

		proto = normalized
	}

	return proto
}

func singleForwardedProtoValue(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.Contains(raw, ",") {
		return "", false
	}

	proto := normalizeProtoValue(raw)
	return proto, proto == "http" || proto == "https"
}

func normalizeProtoValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	if value[0] == '"' || value[len(value)-1] == '"' {
		if len(value) < 2 || value[0] != '"' || value[len(value)-1] != '"' {
			return ""
		}

		value = strings.TrimSpace(value[1 : len(value)-1])
		if value == "" || strings.ContainsAny(value, "\"\r\n\t ") {
			return ""
		}
	}

	value = strings.TrimSpace(value)
	return strings.ToLower(value)
}

func hostFromRemoteAddr(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil || host == "" {
		return remoteAddr
	}

	return host
}

func isTrustAllCIDR(n *net.IPNet) bool {
	if n == nil {
		return false
	}

	ones, bits := n.Mask.Size()
	return ones == 0 && (bits == 32 || bits == 128)
}

func splitCommaNonEmpty(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}

	return out
}

func trimNonEmptyStrings(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if t := strings.TrimSpace(s); t != "" {
			out = append(out, t)
		}
	}

	return out
}
