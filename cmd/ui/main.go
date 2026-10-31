package main

import (
	"crypto/tls"
	"embed"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"vectis/internal/cli"
	"vectis/internal/httpsecurity"
	"vectis/internal/interfaces"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	defaultUIPort          = 8089
	defaultAPIURL          = "http://localhost:8080"
	defaultShutdownTimeout = 5 * time.Second
)

//go:embed all:embedded
var embeddedUI embed.FS

func runUI(cmd *cobra.Command, args []string) {
	logger := interfaces.NewAsyncLogger("ui")
	defer func() { _ = logger.Close() }()

	cli.SetLogLevel(logger)
	ui, source := uiHandler(viper.GetString("dir"), logger)
	devAssets := false
	if rawDevAssetsURL := strings.TrimSpace(viper.GetString("dev-assets-url")); rawDevAssetsURL != "" {
		devUI, devSource, err := devAssetsHandler(rawDevAssetsURL)
		if err != nil {
			logger.Fatal("Invalid Vite dev assets URL: %v", err)
		}

		ui, source = devUI, devSource
		devAssets = true
	}

	backend, err := newUIBackend(viper.GetString("api-url"))
	if err != nil {
		logger.Fatal("Invalid API URL: %v", err)
	}

	backend.allowNetworkHTTP = viper.GetBool("allow_insecure_http")
	if err := backend.configureTransport(viper.GetString("public-url"), viper.GetString("api-ca-file")); err != nil {
		logger.Fatal("Invalid UI transport: %v", err)
	}

	if err := backend.configureLocalHTTP(viper.GetString("host"), viper.GetBool("session_allow_insecure_cookies")); err != nil {
		logger.Fatal("Invalid UI session configuration: %v", err)
	}

	if backend.allowNetworkHTTP {
		logger.Info("HTTP network demo enabled for browser origin %s", backend.publicURL.String())
	}

	certFile, keyFile := strings.TrimSpace(viper.GetString("tls_cert_file")), strings.TrimSpace(viper.GetString("tls_key_file"))
	if backend.allowNetworkHTTP && certFile != "" {
		logger.Fatal("--allow-insecure-http requires an HTTP listener")
	}

	if (certFile == "") != (keyFile == "") {
		logger.Fatal("UI HTTPS requires both certificate and key files")
	}

	addr := net.JoinHostPort(viper.GetString("host"), fmt.Sprintf("%d", viper.GetInt("port")))
	srv := &http.Server{
		Addr:              addr,
		Handler:           backend.handler(ui, devAssets),
		MaxHeaderBytes:    httpsecurity.DefaultMaxHeaderBytes,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	scheme := "http"
	serve := srv.ListenAndServe
	if certFile != "" {
		scheme = "https"
		serve = func() error { return srv.ListenAndServeTLS(certFile, keyFile) }
	}

	logger.Info("UI listening on %s://%s (%s)", scheme, addr, source)
	if err := cli.ServeHTTP(
		cmd.Context(),
		srv,
		serve,
		defaultShutdownTimeout,
		"UI HTTP",
		logger,
	); err != nil {
		logger.Fatal("UI server failed: %v", err)
	}
}

func uiHandler(configuredDir string, logger interfaces.Logger) (http.Handler, string) {
	return uiHandlerWithFS(configuredDir, logger, embeddedUI)
}

func devAssetsHandler(rawURL string) (http.Handler, string, error) {
	target, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, "", err
	}

	if target.Scheme == "" || target.Host == "" {
		return nil, "", fmt.Errorf("must include scheme and host")
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	return proxy, fmt.Sprintf("proxying Vite dev assets from %s", target.String()), nil
}

func uiHandlerWithFS(configuredDir string, logger interfaces.Logger, uiFS fs.FS) (http.Handler, string) {
	if dir := strings.TrimSpace(configuredDir); dir != "" {
		if hasUIIndex(dir) {
			return spaFileServer(httpsecurity.LocalStaticFileSystem(dir)), fmt.Sprintf("serving %s", dir)
		}

		logger.Warn("configured UI dir %s does not contain index.html; falling back to embedded UI", dir)
	}

	if env := strings.TrimSpace(os.Getenv("VECTIS_UI_DIR")); env != "" && env != strings.TrimSpace(configuredDir) {
		if hasUIIndex(env) {
			return spaFileServer(httpsecurity.LocalStaticFileSystem(env)), fmt.Sprintf("serving %s", env)
		}

		logger.Warn("VECTIS_UI_DIR %s does not contain index.html; falling back to embedded UI", env)
	}

	sub, err := fs.Sub(uiFS, "embedded")
	if err == nil && hasUIIndexFS(sub) {
		return spaFileServer(http.FS(sub)), "serving embedded UI"
	}

	logger.Warn("embedded UI build not found; run mage buildUI or set VECTIS_UI_DIR")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Vectis UI</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 720px; margin: 4rem auto; line-height: 1.5; color: #202427; padding: 0 1rem; }
    code { background: #eef2f4; border: 1px solid #d3dde3; border-radius: 4px; padding: 0.1rem 0.25rem; }
  </style>
</head>
<body>
  <h1>Vectis UI</h1>
  <p>The UI server is running, but this binary does not include an embedded UI build.</p>
  <p>Rebuild with <code>mage buildUI</code>, or set <code>VECTIS_UI_DIR</code> to a Vite build directory.</p>
</body>
</html>`))
	}), "embedded UI not available"
}

func spaFileServer(fsys http.FileSystem) http.Handler {
	fsys = httpsecurity.StaticFileSystem(fsys)
	files := http.FileServer(fsys)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if name == "." || name == "" {
			name = "index.html"
		}

		file, err := fsys.Open(name)
		if err == nil {
			_ = file.Close()
			files.ServeHTTP(w, r)
			return
		}

		if isStaticAssetPath(r.URL.Path) || strings.HasSuffix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}

		r2 := r.Clone(r.Context())
		r2.URL.Path = "/"
		files.ServeHTTP(w, r2)
	})
}

func hasUIIndex(dir string) bool {
	clean, err := filepath.Abs(dir)
	if err != nil {
		return false
	}

	info, err := os.Stat(filepath.Join(clean, "index.html")) // #nosec G703 -- The operator configures the static UI root; this probes its fixed index file.
	return err == nil && !info.IsDir()
}

func hasUIIndexFS(fsys fs.FS) bool {
	info, err := fs.Stat(fsys, "index.html")
	return err == nil && !info.IsDir()
}

var rootCmd = &cobra.Command{
	Use:   "vectis-ui",
	Short: "Serve the Vectis web UI",
	Long: `Serve the Vectis web UI.

The server normally serves Vite static assets embedded in the binary.
Use --dir or VECTIS_UI_DIR to serve a local build directory instead.`,
	Run: runUI,
}

func init() {
	cli.ConfigureVersion(rootCmd)

	rootCmd.PersistentFlags().Int("port", defaultUIPort, "HTTP port for the UI")
	rootCmd.PersistentFlags().String("host", "localhost", "Host/IP for the UI to bind")
	rootCmd.PersistentFlags().String("dir", "", "Directory containing a UI build to serve instead of embedded UI")
	rootCmd.PersistentFlags().String("dev-assets-url", "", "Vite dev server URL for hot-reloaded UI assets")
	rootCmd.PersistentFlags().String("api-url", defaultAPIURL, "Base URL for proxied API requests")
	rootCmd.PersistentFlags().String("api-ca-file", "", "CA bundle for the upstream HTTPS API")
	rootCmd.PersistentFlags().String("public-url", "", "Browser-facing UI origin for a reverse proxy or explicit HTTP network demo")
	rootCmd.PersistentFlags().Bool("session-allow-insecure-cookies", false, "Allow HTTP browser sessions on a loopback listener for local development")
	rootCmd.PersistentFlags().Bool("allow-insecure-http", false, "Allow unencrypted network demo sessions; requires an explicit HTTP --public-url")
	rootCmd.PersistentFlags().String("tls-cert-file", "", "Certificate file for UI HTTPS")
	rootCmd.PersistentFlags().String("tls-key-file", "", "Private key file for UI HTTPS")
	rootCmd.PersistentFlags().String("log-level", "info", "Log level: debug, info, warn, error")

	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("dir", rootCmd.PersistentFlags().Lookup("dir"))
	_ = viper.BindPFlag("dev-assets-url", rootCmd.PersistentFlags().Lookup("dev-assets-url"))
	_ = viper.BindPFlag("api-url", rootCmd.PersistentFlags().Lookup("api-url"))
	for _, key := range []string{"api-ca-file", "public-url", "tls-cert-file", "tls-key-file"} {
		configKey := key
		if strings.HasPrefix(key, "tls-") {
			configKey = strings.ReplaceAll(key, "-", "_")
		}
		_ = viper.BindPFlag(configKey, rootCmd.PersistentFlags().Lookup(key))
		_ = viper.BindEnv(configKey, "VECTIS_UI_"+strings.ToUpper(strings.ReplaceAll(key, "-", "_")))
	}

	_ = viper.BindPFlag("allow_insecure_http", rootCmd.PersistentFlags().Lookup("allow-insecure-http"))
	_ = viper.BindEnv("allow_insecure_http", "VECTIS_UI_ALLOW_INSECURE_HTTP")
	_ = viper.BindPFlag("session_allow_insecure_cookies", rootCmd.PersistentFlags().Lookup("session-allow-insecure-cookies"))
	_ = viper.BindEnv("session_allow_insecure_cookies", "VECTIS_UI_SESSION_ALLOW_INSECURE_COOKIES")
	_ = viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindEnv("dir", "VECTIS_UI_DIR")
	_ = viper.BindEnv("dev-assets-url", "VECTIS_UI_DEV_ASSETS_URL")
	_ = viper.BindEnv("api-url", "VECTIS_UI_API_URL")
	viper.SetEnvPrefix("VECTIS_UI")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
