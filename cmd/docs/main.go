package main

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"vectis/internal/cli"
	"vectis/internal/interfaces"

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

	mux := http.NewServeMux()
	mux.Handle("GET /", handler)
	mux.Handle("GET /health/live", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}))

	addr := fmt.Sprintf(":%d", viper.GetInt("port"))
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	logger.Info("Docs listening on http://localhost:%d (%s)", viper.GetInt("port"), source)
	if err := cli.ServeHTTP(cmd.Context(), srv, srv.ListenAndServe, defaultShutdownTimeout, "Docs HTTP", logger); err != nil {
		logger.Fatal("Docs server failed: %v", err)
	}
}

func docsHandler(configuredDir string, logger interfaces.Logger) (http.Handler, string) {
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

	sub, err := fs.Sub(embeddedDocs, "embedded")
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
	rootCmd.PersistentFlags().String("dir", "", "Directory containing a docs build to serve instead of embedded docs")
	rootCmd.PersistentFlags().String("log-level", "info", "Log level: debug, info, warn, error")

	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("dir", rootCmd.PersistentFlags().Lookup("dir"))
	_ = viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindEnv("dir", "VECTIS_DOCS_DIR")
	viper.SetEnvPrefix("VECTIS_DOCS")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
