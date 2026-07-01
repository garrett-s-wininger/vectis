package main

import (
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

	mux := http.NewServeMux()
	api := backend.apiProxyHandler()
	mux.Handle("GET /ui/api/context", http.HandlerFunc(backend.context))
	mux.Handle("POST /ui/api/setup/complete", http.HandlerFunc(backend.completeSetup))
	mux.Handle("POST /ui/api/login", http.HandlerFunc(backend.login))
	mux.Handle("POST /ui/api/logout", http.HandlerFunc(backend.logout))
	mux.Handle("GET /api/", api)
	mux.Handle("POST /api/", api)
	mux.Handle("PUT /api/", api)
	mux.Handle("PATCH /api/", api)
	mux.Handle("DELETE /api/", api)
	mux.Handle("GET /", backend.spaGateWithDevAssets(ui, devAssets))
	mux.Handle("GET /health/live", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}))

	addr := net.JoinHostPort(viper.GetString("host"), fmt.Sprintf("%d", viper.GetInt("port")))
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	logger.Info("UI listening on http://%s (%s)", addr, source)
	if err := cli.ServeHTTP(cmd.Context(), srv, srv.ListenAndServe, defaultShutdownTimeout, "UI HTTP", logger); err != nil {
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
			return spaFileServer(http.Dir(dir)), fmt.Sprintf("serving %s", dir)
		}

		logger.Warn("configured UI dir %s does not contain index.html; falling back to embedded UI", dir)
	}

	if env := strings.TrimSpace(os.Getenv("VECTIS_UI_DIR")); env != "" && env != strings.TrimSpace(configuredDir) {
		if hasUIIndex(env) {
			return spaFileServer(http.Dir(env)), fmt.Sprintf("serving %s", env)
		}

		logger.Warn("VECTIS_UI_DIR %s does not contain index.html; falling back to embedded UI", env)
	}

	sub, err := fs.Sub(uiFS, "embedded")
	if err == nil && hasUIIndexFS(sub) {
		return spaFileServer(http.FS(sub)), "serving embedded UI"
	}

	logger.Warn("embedded UI build not found; rebuild vectis-ui without SKIP_WEB_BUILD=1")
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
  <p>Rebuild with <code>make build</code>, or set <code>VECTIS_UI_DIR</code> to a Vite build directory.</p>
</body>
</html>`))
	}), "embedded UI not available"
}

func spaFileServer(fsys http.FileSystem) http.Handler {
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

	info, err := os.Stat(filepath.Join(clean, "index.html"))
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
	rootCmd.PersistentFlags().String("log-level", "info", "Log level: debug, info, warn, error")

	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("dir", rootCmd.PersistentFlags().Lookup("dir"))
	_ = viper.BindPFlag("dev-assets-url", rootCmd.PersistentFlags().Lookup("dev-assets-url"))
	_ = viper.BindPFlag("api-url", rootCmd.PersistentFlags().Lookup("api-url"))
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
