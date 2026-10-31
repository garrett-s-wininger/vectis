package main

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"vectis/internal/config"
	"vectis/internal/interfaces"

	"github.com/spf13/viper"
)

func validateLocalHTTPDemo(browserTLSEnabled bool) error {
	if !viper.GetBool("allow_insecure_http") {
		return nil
	}

	if browserTLSEnabled {
		return fmt.Errorf("--allow-insecure-http requires HTTP; use --http-tls=off")
	}

	raw := strings.TrimSpace(viper.GetString("ui_public_url"))
	origin, err := url.Parse(raw)
	if err != nil || origin.Scheme != "http" || origin.Host == "" || origin.User != nil || origin.Opaque != "" || origin.RawQuery != "" || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") {
		return fmt.Errorf("--allow-insecure-http requires an HTTP --ui-public-url, for example http://dev-vm:8089")
	}

	return nil
}

func logLocalUIEndpoints(logger interfaces.Logger, scheme string) {
	uiPort := viper.GetInt("ui_port")
	logger.Info("UI listener: %s://%s", scheme, net.JoinHostPort(localHost(), strconv.Itoa(uiPort)))
	raw := strings.TrimSpace(viper.GetString("ui_public_url"))
	if raw == "" {
		return
	}

	origin, err := url.Parse(raw)
	if err != nil || origin.Host == "" {
		logger.Info("UI public origin is invalid; check --ui-public-url")
		return
	}

	logger.Info("UI browser origin: %s://%s", origin.Scheme, origin.Host)
	apiPort := config.APIEffectiveListenPort()
	if origin.Port() == strconv.Itoa(apiPort) && uiPort != apiPort {
		logger.Warn("UI public URL uses API port %d, but the UI listens on %d. --ui-public-url does not change the listener. For direct access use %s://%s", apiPort, uiPort, origin.Scheme, net.JoinHostPort(origin.Hostname(), strconv.Itoa(uiPort)))
	}
}
