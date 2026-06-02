package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/spf13/viper"
)

const envAPICORSAllowedOrigins = "VECTIS_API_CORS_ALLOWED_ORIGINS"

// APICORSAllowedOrigins returns exact browser origins allowed to read API responses.
func APICORSAllowedOrigins() []string {
	if v := strings.TrimSpace(os.Getenv(envAPICORSAllowedOrigins)); v != "" {
		return normalizeCORSOriginList(splitCommaNonEmpty(v))
	}

	if viper.IsSet("api.cors.allowed_origins") {
		raw := viper.Get("api.cors.allowed_origins")
		switch x := raw.(type) {
		case []string:
			return normalizeCORSOriginList(x)
		case []any:
			out := make([]string, 0, len(x))
			for _, e := range x {
				if s, ok := e.(string); ok {
					out = append(out, s)
				}
			}

			return normalizeCORSOriginList(out)
		case string:
			return normalizeCORSOriginList(splitCommaNonEmpty(x))
		}
	}

	return normalizeCORSOriginList(MustDefaults().API.CORS.AllowedOrigins)
}

func ValidateAPICORSConfig() error {
	for _, origin := range APICORSAllowedOrigins() {
		if _, err := parseCORSOrigin(origin); err != nil {
			return err
		}
	}

	return nil
}

func normalizeCORSOriginList(origins []string) []string {
	out := make([]string, 0, len(origins))
	seen := make(map[string]bool, len(origins))
	for _, origin := range origins {
		origin = strings.TrimRight(strings.TrimSpace(origin), "/")
		if origin == "" || seen[origin] {
			continue
		}

		seen[origin] = true
		out = append(out, origin)
	}

	return out
}

func parseCORSOrigin(origin string) (*url.URL, error) {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return nil, fmt.Errorf("api.cors.allowed_origins must not contain empty origins")
	}

	if origin == "*" || strings.EqualFold(origin, "null") {
		return nil, fmt.Errorf("api.cors.allowed_origins must use exact http(s) origins, not %q", origin)
	}

	u, err := url.Parse(origin)
	if err != nil {
		return nil, fmt.Errorf("api.cors.allowed_origins contains invalid origin %q: %w", origin, err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("api.cors.allowed_origins origin %q must use http or https", origin)
	}

	if u.Host == "" {
		return nil, fmt.Errorf("api.cors.allowed_origins origin %q must include a host", origin)
	}

	if u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("api.cors.allowed_origins origin %q must not include user info, path, query, or fragment", origin)
	}

	return u, nil
}
