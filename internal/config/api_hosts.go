package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

const (
	envAPIAllowedHosts         = "VECTIS_API_ALLOWED_HOSTS"
	envDocsAllowedHosts        = "VECTIS_DOCS_ALLOWED_HOSTS"
	envMetricsAllowedHosts     = "VECTIS_METRICS_ALLOWED_HOSTS"
	envCellIngressAllowedHosts = "VECTIS_CELL_INGRESS_ALLOWED_HOSTS"
)

type apiAllowedHost struct {
	host string
	port string
}

// APIAllowedHosts returns hostnames accepted in the browser-facing API Host header.
func APIAllowedHosts() []string {
	if v := strings.TrimSpace(os.Getenv(envAPIAllowedHosts)); v != "" {
		return normalizeAPIAllowedHostList(splitCommaNonEmpty(v))
	}

	if viper.IsSet("api.host_validation.allowed_hosts") {
		return normalizeAPIAllowedHostList(stringSliceFromViper("api.host_validation.allowed_hosts"))
	}

	defaults := normalizeAPIAllowedHostList(MustDefaults().API.HostValidation.AllowedHosts)
	if len(defaults) > 0 {
		return defaults
	}

	return defaultAllowedHosts(APIHost())
}

func ValidateAPIHostConfig() error {
	for _, host := range APIAllowedHosts() {
		if _, err := parseAPIAllowedHost(host); err != nil {
			return err
		}
	}

	return nil
}

func APIHostAllowed(hostHeader string) bool {
	return hostAllowed(hostHeader, APIAllowedHosts())
}

// DocsAllowedHosts returns hostnames accepted in the browser-facing docs Host header.
func DocsAllowedHosts(bindHost string) []string {
	if v := strings.TrimSpace(os.Getenv(envDocsAllowedHosts)); v != "" {
		return normalizeAPIAllowedHostList(splitCommaNonEmpty(v))
	}

	if viper.IsSet("allowed_hosts") {
		return normalizeAPIAllowedHostList(stringSliceFromViper("allowed_hosts"))
	}

	return defaultAllowedHosts(bindHost)
}

func ValidateDocsHostConfig(bindHost string) error {
	for _, host := range DocsAllowedHosts(bindHost) {
		if _, err := parseAPIAllowedHost(host); err != nil {
			return err
		}
	}

	return nil
}

func DocsHostAllowed(bindHost, hostHeader string) bool {
	return hostAllowed(hostHeader, DocsAllowedHosts(bindHost))
}

// MetricsAllowedHosts returns hostnames accepted by dedicated /metrics listeners.
func MetricsAllowedHosts(bindHost string) []string {
	if hosts := hostListFromViper("metrics_allowed_hosts"); len(hosts) > 0 {
		return normalizeAPIAllowedHostList(hosts)
	}

	if hosts := hostListFromViper("metrics.allowed_hosts"); len(hosts) > 0 {
		return normalizeAPIAllowedHostList(hosts)
	}

	if v := strings.TrimSpace(os.Getenv(envMetricsAllowedHosts)); v != "" {
		return normalizeAPIAllowedHostList(splitCommaNonEmpty(v))
	}

	return defaultAllowedHosts(bindHost)
}

func hostListFromViper(key string) []string {
	return cleanStringSlice(append(viper.GetStringSlice(key), viper.GetString(key)))
}

func ValidateMetricsHostConfig(bindHost string) error {
	for _, host := range MetricsAllowedHosts(bindHost) {
		if _, err := parseAPIAllowedHost(host); err != nil {
			return err
		}
	}

	return nil
}

func MetricsHostAllowed(bindHost, hostHeader string) bool {
	return hostAllowed(hostHeader, MetricsAllowedHosts(bindHost))
}

// CellIngressAllowedHosts returns hostnames accepted by the private cell ingress HTTP listener.
func CellIngressAllowedHosts(localCellID, bindHost string) []string {
	hosts, err := cellIngressAllowedHosts(localCellID, bindHost)
	if err != nil {
		return nil
	}

	return hosts
}

func ValidateCellIngressHostConfig(localCellID, bindHost string) error {
	hosts, err := cellIngressAllowedHosts(localCellID, bindHost)
	if err != nil {
		return err
	}

	for _, host := range hosts {
		if _, err := parseAPIAllowedHost(host); err != nil {
			return err
		}
	}

	return nil
}

func CellIngressHostAllowed(localCellID, bindHost, hostHeader string) bool {
	return hostAllowed(hostHeader, CellIngressAllowedHosts(localCellID, bindHost))
}

func cellIngressAllowedHosts(localCellID, bindHost string) ([]string, error) {
	if v := strings.TrimSpace(os.Getenv(envCellIngressAllowedHosts)); v != "" {
		return normalizeAPIAllowedHostList(splitCommaNonEmpty(v)), nil
	}

	if hosts := hostListFromViper("allowed_hosts"); len(hosts) > 0 {
		return normalizeAPIAllowedHostList(hosts), nil
	}

	if hosts := hostListFromViper("cell_ingress.allowed_hosts"); len(hosts) > 0 {
		return normalizeAPIAllowedHostList(hosts), nil
	}

	hosts := defaultAllowedHosts(bindHost)
	endpointHosts, err := cellIngressEndpointAllowedHosts(localCellID)
	if err != nil {
		return nil, err
	}

	return normalizeAPIAllowedHostList(append(hosts, endpointHosts...)), nil
}

func cellIngressEndpointAllowedHosts(localCellID string) ([]string, error) {
	endpoints, err := CellIngressEndpoints()
	if err != nil {
		return nil, err
	}

	localCellID = strings.TrimSpace(localCellID)
	if localCellID == "" {
		localCellID = CellID()
	}

	endpoint := strings.TrimSpace(endpoints[localCellID])
	if endpoint == "" {
		return nil, nil
	}

	host, err := cellIngressEndpointHost(endpoint)
	if err != nil {
		return nil, err
	}

	return []string{host}, nil
}

func cellIngressEndpointHost(endpoint string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return "", fmt.Errorf("cell ingress endpoint host validation: %w", err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("cell ingress endpoint host validation: endpoint must use http or https")
	}

	if strings.TrimSpace(u.Host) == "" {
		return "", fmt.Errorf("cell ingress endpoint host validation: endpoint host is required")
	}

	return u.Host, nil
}

func hostAllowed(hostHeader string, allowedHosts []string) bool {
	requestHost, err := parseAPIAllowedHost(hostHeader)
	if err != nil {
		return false
	}

	for _, raw := range allowedHosts {
		allowed, err := parseAPIAllowedHost(raw)
		if err != nil {
			continue
		}

		if requestHost.host != allowed.host {
			continue
		}

		if allowed.port == "" || allowed.port == requestHost.port {
			return true
		}
	}

	return false
}

func defaultAllowedHosts(bindHost string) []string {
	hosts := []string{bindHost, "localhost", "127.0.0.1", "::1"}
	out := make([]string, 0, len(hosts))

	for _, host := range hosts {
		parsed, err := parseAPIAllowedHost(host)
		if err != nil || isUnspecifiedAPIHost(parsed.host) {
			continue
		}

		out = append(out, parsed.String())
	}

	return normalizeAPIAllowedHostList(out)
}

func normalizeAPIAllowedHostList(hosts []string) []string {
	out := make([]string, 0, len(hosts))
	seen := make(map[string]struct{}, len(hosts))

	for _, raw := range hosts {
		parsed, err := parseAPIAllowedHost(raw)
		if err != nil {
			invalid := strings.ToLower(strings.TrimSpace(raw))
			if invalid != "" {
				out = append(out, invalid)
			}

			continue
		}

		normalized := parsed.String()
		if _, ok := seen[normalized]; ok {
			continue
		}

		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}

	return out
}

func parseAPIAllowedHost(raw string) (apiAllowedHost, error) {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return apiAllowedHost{}, fmt.Errorf("api host validation: host must not be empty")
	}

	if value == "*" || strings.Contains(value, "*") {
		return apiAllowedHost{}, fmt.Errorf("api host validation: wildcard hosts are not allowed")
	}

	if strings.Contains(value, "://") || strings.ContainsAny(value, "/?#@") || strings.ContainsFunc(value, isHostHeaderSpaceOrControl) {
		return apiAllowedHost{}, fmt.Errorf("api host validation: invalid host %q", raw)
	}

	host := value
	port := ""
	if h, p, err := net.SplitHostPort(value); err == nil {
		host = h
		port = p
	} else if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		host = strings.Trim(value, "[]")
	}

	host = strings.TrimSuffix(strings.Trim(host, "[]"), ".")
	if host == "" {
		return apiAllowedHost{}, fmt.Errorf("api host validation: host must not be empty")
	}

	if strings.Contains(host, ":") && net.ParseIP(host) == nil {
		return apiAllowedHost{}, fmt.Errorf("api host validation: invalid host %q", raw)
	}

	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n <= 0 || n > 65535 {
			return apiAllowedHost{}, fmt.Errorf("api host validation: invalid port in host %q", raw)
		}
	}

	return apiAllowedHost{host: host, port: port}, nil
}

func (h apiAllowedHost) String() string {
	if h.port == "" {
		return h.host
	}

	return net.JoinHostPort(h.host, h.port)
}

func isUnspecifiedAPIHost(host string) bool {
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return true
	default:
		return false
	}
}

func isHostHeaderSpaceOrControl(r rune) bool {
	return r <= ' ' || r == 0x7f
}
