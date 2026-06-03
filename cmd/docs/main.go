package main

import (
	"context"
	"crypto/tls"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"vectis/internal/cli"
	"vectis/internal/httpsecurity"
	"vectis/internal/interfaces"
	"vectis/internal/tlsconfig"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	defaultDocsPort        = 8088
	defaultShutdownTimeout = 5 * time.Second
)

//go:embed all:embedded
var embeddedDocs embed.FS

func runDocs(cmd *cobra.Command, args []string) {
	logger := interfaces.NewAsyncLogger("docs")
	defer logger.Close()
	cli.SetLogLevel(logger)

	handler, source := docsHandler(viper.GetString("dir"), logger)

	addr := net.JoinHostPort(viper.GetString("host"), fmt.Sprintf("%d", viper.GetInt("port")))
	srv := docsHTTPServer(addr, handler)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Listen: %v", err)
	}

	ln, scheme, err := docsTLSListener(cmd.Context(), ln)
	if err != nil {
		logger.Fatal("Docs TLS: %v", err)
	}

	logger.Info("Docs listening on %s://%s (%s)", scheme, addr, source)
	if err := cli.ServeHTTP(cmd.Context(), srv, func() error { return srv.Serve(ln) }, defaultShutdownTimeout, "Docs HTTP", logger); err != nil {
		logger.Fatal("Docs server failed: %v", err)
	}
}

func docsHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           docsServerHandler(handler),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    httpsecurity.DefaultMaxHeaderBytes,
	}
}

func docsServerHandler(handler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /", handler)
	mux.Handle("GET /health/live", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}))

	return httpsecurity.HeaderMiddleware(httpsecurity.DocsHeaderPolicy(), mux)
}

func docsTLSEnabled() bool {
	return strings.TrimSpace(viper.GetString("tls_cert_file")) != "" || strings.TrimSpace(viper.GetString("tls_key_file")) != ""
}

func docsTLSOptions() tlsconfig.Options {
	return tlsconfig.Options{
		ServerCert: strings.TrimSpace(viper.GetString("tls_cert_file")),
		ServerKey:  strings.TrimSpace(viper.GetString("tls_key_file")),
	}
}

func docsTLSListener(ctx context.Context, ln net.Listener) (net.Listener, string, error) {
	if !docsTLSEnabled() {
		return ln, "http", nil
	}

	o := docsTLSOptions()
	if o.ServerCert == "" || o.ServerKey == "" {
		_ = ln.Close()
		return nil, "", errors.New("cert file and key file are required together")
	}

	reloader, err := tlsconfig.NewReloader(o)
	if err != nil {
		_ = ln.Close()
		return nil, "", err
	}

	cfg, err := reloader.ServerTLS()
	if err != nil {
		_ = ln.Close()
		return nil, "", err
	}

	if interval := viper.GetDuration("tls_reload_interval"); interval > 0 {
		go func() {
			if err := reloader.RunReloadLoop(ctx, interval); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "docs TLS reload loop error: %v\n", err)
			}
		}()
	}

	return tls.NewListener(ln, cfg), "https", nil
}

func docsHandler(configuredDir string, logger interfaces.Logger) (http.Handler, string) {
	return docsHandlerWithFS(configuredDir, logger, embeddedDocs)
}

func docsHandlerWithFS(configuredDir string, logger interfaces.Logger, docsFS fs.FS) (http.Handler, string) {
	if dir := strings.TrimSpace(configuredDir); dir != "" {
		if hasDocsIndex(dir) {
			return http.FileServer(http.Dir(dir)), fmt.Sprintf("serving %s", dir)
		}

		logger.Warn("configured docs dir %s does not contain index.html; falling back to embedded docs", dir)
	}

	if env := strings.TrimSpace(os.Getenv("VECTIS_DOCS_DIR")); env != "" && env != strings.TrimSpace(configuredDir) {
		if hasDocsIndex(env) {
			return http.FileServer(http.Dir(env)), fmt.Sprintf("serving %s", env)
		}

		logger.Warn("VECTIS_DOCS_DIR %s does not contain index.html; falling back to embedded docs", env)
	}

	sub, err := fs.Sub(docsFS, "embedded")
	if err == nil && hasDocsIndexFS(sub) {
		return http.FileServer(http.FS(sub)), "serving embedded docs"
	}

	logger.Warn("embedded docs build not found; rebuild vectis-docs without SKIP_WEB_BUILD=1")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Vectis Docs</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 720px; margin: 4rem auto; line-height: 1.5; color: #202427; padding: 0 1rem; }
    code { background: #eef2f4; border: 1px solid #d3dde3; border-radius: 4px; padding: 0.1rem 0.25rem; }
  </style>
</head>
<body>
  <h1>Vectis Docs</h1>
  <p>The docs server is running, but this binary does not include an embedded docs build.</p>
  <p>Rebuild with <code>make build</code>, or set <code>VECTIS_DOCS_DIR</code> to a Docusaurus build directory.</p>
</body>
</html>`))
	}), "embedded docs not available"
}

func hasDocsIndex(dir string) bool {
	clean, err := filepath.Abs(dir)
	if err != nil {
		return false
	}

	info, err := os.Stat(filepath.Join(clean, "index.html"))
	return err == nil && !info.IsDir()
}

func hasDocsIndexFS(fsys fs.FS) bool {
	info, err := fs.Stat(fsys, "index.html")
	return err == nil && !info.IsDir()
}

var rootCmd = &cobra.Command{
	Use:   "vectis-docs",
	Short: "Serve the Vectis documentation site",
	Long: `Serve the Vectis documentation site.

The server normally serves Docusaurus static assets embedded in the binary.
Use --dir or VECTIS_DOCS_DIR to serve a local build directory instead.`,
	Run: runDocs,
}

func init() {
	cli.ConfigureVersion(rootCmd)

	rootCmd.PersistentFlags().Int("port", defaultDocsPort, "HTTP port for the docs site")
	rootCmd.PersistentFlags().String("host", "localhost", "Host/IP for the docs site to bind")
	rootCmd.PersistentFlags().String("dir", "", "Directory containing a docs build to serve instead of embedded docs")
	rootCmd.PersistentFlags().String("log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().String("tls-cert-file", "", "Certificate file for docs HTTPS")
	rootCmd.PersistentFlags().String("tls-key-file", "", "Private key file for docs HTTPS")
	rootCmd.PersistentFlags().Duration("tls-reload-interval", 0, "How often to poll docs HTTPS cert/key files for reload; 0 disables polling")

	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("dir", rootCmd.PersistentFlags().Lookup("dir"))
	_ = viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("tls_cert_file", rootCmd.PersistentFlags().Lookup("tls-cert-file"))
	_ = viper.BindPFlag("tls_key_file", rootCmd.PersistentFlags().Lookup("tls-key-file"))
	_ = viper.BindPFlag("tls_reload_interval", rootCmd.PersistentFlags().Lookup("tls-reload-interval"))
	_ = viper.BindEnv("dir", "VECTIS_DOCS_DIR")
	_ = viper.BindEnv("tls_cert_file", "VECTIS_DOCS_TLS_CERT_FILE")
	_ = viper.BindEnv("tls_key_file", "VECTIS_DOCS_TLS_KEY_FILE")
	_ = viper.BindEnv("tls_reload_interval", "VECTIS_DOCS_TLS_RELOAD_INTERVAL")
	viper.SetEnvPrefix("VECTIS_DOCS")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
