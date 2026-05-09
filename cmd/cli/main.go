package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	api "vectis/api/gen/go"
	podmanassets "vectis/deploy/podman"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/database"
	jobvalidation "vectis/internal/job/validation"
	"vectis/internal/utils"

	_ "vectis/internal/dbdrivers"
)

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Stream    int    `json:"stream"`
	Sequence  int64  `json:"sequence"`
	Data      string `json:"data"`
}

func newAPIRequest(method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, config.PublicAPIBaseURL()+path, body)
	if err != nil {
		return nil, err
	}

	if token := effectiveToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	return req, nil
}

var apiHTTPClient = &http.Client{Timeout: 30 * time.Second}

func doAPIRequest(req *http.Request) (*http.Response, error) {
	return apiHTTPClient.Do(req)
}

func cliTokenFilePath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "vectis", "token"), nil
}

func readPersistedToken() string {
	path, err := cliTokenFilePath()
	if err != nil {
		return ""
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(b))
}

func writePersistedToken(token string) error {
	path, err := cliTokenFilePath()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
		return err
	}

	return os.Chmod(path, 0o600)
}

func deletePersistedToken() error {
	path, err := cliTokenFilePath()
	if err != nil {
		return err
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

const (
	defaultPodmanKubeSpec    = "deploy/podman/kube-spec.yaml"
	defaultPodmanGrafanaSpec = "deploy/podman/grafana-configmaps.gen.yaml"
	podmanSecretName         = "vectis-podman-secrets"
	envDeployConfigDir       = "VECTIS_DEPLOY_CONFIG_DIR"
)

var (
	podmanNetwork     string
	podmanKubeSpec    string
	podmanGrafanaSpec string
	podmanRenderOut   string
	resetYes          bool
	resetDryRun       bool
	runListJobID      string
	runListLimit      int
	runListSince      int
)

type podmanSecrets struct {
	PostgresPassword string `json:"postgres_password"`
	BootstrapToken   string `json:"bootstrap_token"`
	PodDSN           string `json:"pod_database_dsn"`
	HostDSN          string `json:"host_database_dsn"`
}

func podmanDeployDir() (string, error) {
	if dir := os.Getenv(envDeployConfigDir); dir != "" {
		return filepath.Join(dir, "podman"), nil
	}

	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "vectis", "deploy", "podman"), nil
}

func podmanSecretsPath() (string, error) {
	dir, err := podmanDeployDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "secrets.json"), nil
}

func podmanRenderedPath() (string, error) {
	dir, err := podmanDeployDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "rendered.yaml"), nil
}

func randomSecret(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(b), nil
}

func loadOrCreatePodmanSecrets(rotate bool) (podmanSecrets, bool, error) {
	path, err := podmanSecretsPath()
	if err != nil {
		return podmanSecrets{}, false, err
	}

	if !rotate {
		b, err := os.ReadFile(path)
		if err == nil {
			var secrets podmanSecrets
			if err := json.Unmarshal(b, &secrets); err != nil {
				return podmanSecrets{}, false, fmt.Errorf("parse podman secrets %s: %w", path, err)
			}

			return secrets, false, nil
		}

		if !os.IsNotExist(err) {
			return podmanSecrets{}, false, err
		}
	}

	postgresPassword, err := randomSecret(32)
	if err != nil {
		return podmanSecrets{}, false, fmt.Errorf("generate postgres password: %w", err)
	}

	bootstrapToken, err := randomSecret(32)
	if err != nil {
		return podmanSecrets{}, false, fmt.Errorf("generate bootstrap token: %w", err)
	}

	secrets := podmanSecrets{
		PostgresPassword: postgresPassword,
		BootstrapToken:   bootstrapToken,
		PodDSN:           fmt.Sprintf("postgres://vectis:%s@127.0.0.1:5432/vectis?sslmode=verify-full&sslrootcert=/run/vectis/postgres-tls/ca.pem", postgresPassword),
		HostDSN:          fmt.Sprintf("postgres://vectis:%s@127.0.0.1:15432/vectis?sslmode=require", postgresPassword),
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return podmanSecrets{}, false, err
	}

	b, err := json.MarshalIndent(secrets, "", "  ")
	if err != nil {
		return podmanSecrets{}, false, err
	}

	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		return podmanSecrets{}, false, err
	}

	return secrets, true, os.Chmod(path, 0o600)
}

func yamlString(s string) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return strings.TrimSpace(b.String())
}

func renderPodmanSecret(secrets podmanSecrets) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
type: Opaque
stringData:
  POSTGRES_PASSWORD: %s
  VECTIS_DATABASE_DSN: %s
  VECTIS_API_AUTH_BOOTSTRAP_TOKEN: %s
---
`, podmanSecretName, yamlString(secrets.PostgresPassword), yamlString(secrets.PodDSN), yamlString(secrets.BootstrapToken))
}

func readDeployFile(path string) ([]byte, error) {
	switch path {
	case "", defaultPodmanKubeSpec:
		return podmanassets.KubeSpec, nil
	case defaultPodmanGrafanaSpec:
		return podmanassets.GrafanaConfigMaps, nil
	}

	b, err := os.ReadFile(path)
	if err == nil {
		return b, nil
	}

	return nil, fmt.Errorf("read %s: %w", path, err)
}

func renderPodmanManifest(rotate bool) ([]byte, podmanSecrets, bool, error) {
	secrets, created, err := loadOrCreatePodmanSecrets(rotate)
	if err != nil {
		return nil, podmanSecrets{}, false, err
	}

	kubeSpec, err := readDeployFile(podmanKubeSpec)
	if err != nil {
		return nil, podmanSecrets{}, false, err
	}

	grafanaSpec, err := readDeployFile(podmanGrafanaSpec)
	if err != nil {
		return nil, podmanSecrets{}, false, err
	}

	var out bytes.Buffer
	out.WriteString(renderPodmanSecret(secrets))
	out.Write(kubeSpec)
	if !bytes.HasSuffix(kubeSpec, []byte("\n")) {
		out.WriteByte('\n')
	}

	out.WriteString("---\n")
	out.Write(grafanaSpec)
	if !bytes.HasSuffix(grafanaSpec, []byte("\n")) {
		out.WriteByte('\n')
	}

	return out.Bytes(), secrets, created, nil
}

func writePodmanRenderedManifest(rotate bool) (string, podmanSecrets, bool, error) {
	manifest, secrets, created, err := renderPodmanManifest(rotate)
	if err != nil {
		return "", podmanSecrets{}, false, err
	}

	path, err := podmanRenderedPath()
	if err != nil {
		return "", podmanSecrets{}, false, err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", podmanSecrets{}, false, err
	}

	if err := os.WriteFile(path, manifest, 0o600); err != nil {
		return "", podmanSecrets{}, false, err
	}

	return path, secrets, created, nil
}

func runDeployPodmanInit(cmd *cobra.Command, args []string) {
	rotate, _ := cmd.Flags().GetBool("rotate")
	secrets, created, err := loadOrCreatePodmanSecrets(rotate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	path, _ := podmanSecretsPath()
	if created {
		fmt.Printf("Generated Podman deployment secrets: %s\n", path)
	} else {
		fmt.Printf("Podman deployment secrets already exist: %s\n", path)
	}

	fmt.Println("Host migration DSN is stored locally for deploy commands.")
	if secrets.BootstrapToken != "" {
		fmt.Println("API bootstrap token generated for use when auth is enabled.")
	}
}

func runDeployPodmanRender(cmd *cobra.Command, args []string) {
	rotate, _ := cmd.Flags().GetBool("rotate")
	manifest, _, _, err := renderPodmanManifest(rotate)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if podmanRenderOut == "" || podmanRenderOut == "-" {
		_, _ = os.Stdout.Write(manifest)
		return
	}

	if err := os.MkdirAll(filepath.Dir(podmanRenderOut), 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(podmanRenderOut, manifest, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Rendered Podman manifest: %s\n", podmanRenderOut)
}

func runDeployPodmanUp(cmd *cobra.Command, args []string) {
	manifestPath, secrets, created, err := writePodmanRenderedManifest(false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if created {
		fmt.Println("Generated Podman deployment secrets.")
	}

	playArgs := []string{"play", "kube", "--replace", manifestPath}
	if podmanNetwork != "" {
		playArgs = append(playArgs, "--network", podmanNetwork)
	}

	podman := exec.CommandContext(cmd.Context(), "podman", playArgs...)
	podman.Stdout = os.Stdout
	podman.Stderr = os.Stderr
	if err := podman.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: podman play kube failed: %v\n", err)
		os.Exit(1)
	}

	oldDriver, hadDriver := os.LookupEnv(database.EnvDatabaseDriver)
	oldDSN, hadDSN := os.LookupEnv(database.EnvDatabaseDSN)
	_ = os.Setenv(database.EnvDatabaseDriver, "pgx")
	_ = os.Setenv(database.EnvDatabaseDSN, secrets.HostDSN)
	defer func() {
		if hadDriver {
			_ = os.Setenv(database.EnvDatabaseDriver, oldDriver)
		} else {
			_ = os.Unsetenv(database.EnvDatabaseDriver)
		}

		if hadDSN {
			_ = os.Setenv(database.EnvDatabaseDSN, oldDSN)
		} else {
			_ = os.Unsetenv(database.EnvDatabaseDSN)
		}
	}()

	if err := database.Migrate(secrets.HostDSN); err != nil {
		fmt.Fprintf(os.Stderr, "Error: migrations failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Podman deployment is up and migrations are applied.")
}

func runDeployPodmanDown(cmd *cobra.Command, args []string) {
	manifestPath, _, _, err := writePodmanRenderedManifest(false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	playArgs := []string{"play", "kube", "--down", manifestPath}
	podman := exec.CommandContext(cmd.Context(), "podman", playArgs...)
	podman.Stdout = os.Stdout
	podman.Stderr = os.Stderr
	if err := podman.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: podman play kube --down failed: %v\n", err)
		os.Exit(1)
	}
}

func runDeployPodmanStatus(cmd *cobra.Command, args []string) {
	podman := exec.CommandContext(cmd.Context(), "podman", "pod", "ps", "--filter", "name=vectis", "--format", "table {{.Name}}\t{{.Status}}")
	podman.Stdout = os.Stdout
	podman.Stderr = os.Stderr
	if err := podman.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: podman status failed: %v\n", err)
		os.Exit(1)
	}
}

func resetTargets() ([]string, error) {
	var targets []string
	add := func(path string) {
		if path == "" {
			return
		}
		targets = append(targets, filepath.Clean(path))
	}

	configDir, err := os.UserConfigDir()
	if err != nil {
		return nil, fmt.Errorf("resolve user config directory: %w", err)
	}
	add(filepath.Join(configDir, "vectis"))

	add(filepath.Join(utils.DataHome(), "vectis"))

	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return nil, fmt.Errorf("resolve user cache directory: %w", err)
	}
	add(filepath.Join(cacheDir, "vectis"))

	if deployConfigDir := os.Getenv(envDeployConfigDir); deployConfigDir != "" {
		add(filepath.Join(deployConfigDir, "podman"))
	}

	sort.Strings(targets)
	unique := targets[:0]
	for _, target := range targets {
		if len(unique) == 0 || unique[len(unique)-1] != target {
			unique = append(unique, target)
		}
	}

	return unique, nil
}

func runReset(cmd *cobra.Command, args []string) {
	targets, err := resetTargets()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resetDryRun {
		fmt.Println("Would remove:")
		for _, target := range targets {
			fmt.Printf("  %s\n", target)
		}
		return
	}

	if !resetYes {
		fmt.Fprintln(os.Stderr, "Error: reset removes local Vectis config, data, cache, tokens, and generated deployment secrets; pass --yes to confirm")
		fmt.Fprintln(os.Stderr, "Use --dry-run to inspect the directories first.")
		os.Exit(1)
	}

	for _, target := range targets {
		if _, err := os.Stat(target); os.IsNotExist(err) {
			fmt.Printf("Skipped missing path: %s\n", target)
			continue
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "Error: inspect %s: %v\n", target, err)
			os.Exit(1)
		}

		if err := os.RemoveAll(target); err != nil {
			fmt.Fprintf(os.Stderr, "Error: remove %s: %v\n", target, err)
			os.Exit(1)
		}

		fmt.Printf("Removed: %s\n", target)
	}
}

func effectiveToken() string {
	if token := config.CLIAPIToken(); token != "" {
		return token
	}

	return readPersistedToken()
}

func runLogStream(runID string, filterStdout, filterStderr bool) error {
	req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/runs/%s/logs", runID), nil)
	if err != nil {
		return fmt.Errorf("failed to create log stream request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)

	resp, err := doAPIRequest(req)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to connect to API for log stream: %w", err)
	}

	defer func() {
		cancel()
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusServiceUnavailable {
		body, _ := io.ReadAll(resp.Body)
		var apiErr struct {
			Error string `json:"error"`
		}

		if json.Unmarshal(body, &apiErr) == nil && apiErr.Error == "log_service_unavailable" {
			return fmt.Errorf("log service temporarily unavailable; logs will be backfilled when service recovers")
		}

		return fmt.Errorf("log stream request failed: %s", resp.Status)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("log stream request failed: %s", resp.Status)
	}

	fmt.Printf("Connected to logs for run %s\n", runID)
	fmt.Println("Streaming logs... (press Ctrl+C to exit)")

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	done := make(chan struct{})
	readErr := make(chan error, 1)

	go func() {
		defer close(done)

		reader := bufio.NewReader(resp.Body)
		var dataBuf strings.Builder

		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				if err == io.EOF {
					return
				}

				readErr <- err
				return
			}

			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				if dataBuf.Len() == 0 {
					continue
				}

				message := []byte(dataBuf.String())
				dataBuf.Reset()

				var entry LogEntry
				if err := json.Unmarshal(message, &entry); err != nil {
					fmt.Fprintf(os.Stderr, "Error: failed to parse log entry: %v\n", err)
					continue
				}

				if entry.Stream == int(api.Stream_STREAM_CONTROL.Number()) {
					var meta struct {
						Event  string `json:"event"`
						Status string `json:"status,omitempty"`
					}

					if err := json.Unmarshal([]byte(entry.Data), &meta); err != nil {
						continue
					}

					switch meta.Event {
					case "start":
						fmt.Printf("\n=== Run %s started ===\n", runID)
					case "completed":
						status := meta.Status
						switch status {
						case "success":
							fmt.Printf("Run %s finished successfully.\n", runID)
						case "failure":
							fmt.Printf("Run %s failed.\n", runID)
						default:
							fmt.Printf("Run %s finished (status: %s).\n", runID, status)
						}
					}

					if meta.Event == "completed" {
						return
					}

					continue
				}

				if filterStdout && entry.Stream != int(api.Stream_STREAM_STDOUT.Number()) {
					continue
				}

				if filterStderr && entry.Stream != int(api.Stream_STREAM_STDERR.Number()) {
					continue
				}

				streamPrefix := ""
				if entry.Stream == int(api.Stream_STREAM_STDERR.Number()) {
					streamPrefix = "[stderr] "
				}

				fmt.Printf("%s%s\n", streamPrefix, entry.Data)
				continue
			}

			if after, ok := strings.CutPrefix(line, "data:"); ok {
				data := strings.TrimSpace(after)
				dataBuf.WriteString(data)
			}
		}
	}()

	select {
	case <-done:
		select {
		case err := <-readErr:
			if err != nil {
				return fmt.Errorf("log stream read error: %w", err)
			}
		default:
		}
		return nil
	case <-interrupt:
		fmt.Println("\nDisconnecting...")
		cancel()
		<-done
		return fmt.Errorf("interrupted")
	}
}

func triggerJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	jobID := args[0]
	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/jobs/trigger/%s", jobID), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create trigger request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to trigger job: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result struct {
			JobID    string `json:"job_id"`
			RunID    string `json:"run_id"`
			RunIndex int    `json:"run_index"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to parse response: %v\n", err)
			os.Exit(1)
		}

		if result.RunID == "" {
			fmt.Fprintln(os.Stderr, "Error: response missing run_id")
			os.Exit(1)
		}

		follow, _ := cmd.Flags().GetBool("follow")
		if follow {
			if err := runLogStream(result.RunID, false, false); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		} else {
			fmt.Println(result.RunID)
		}
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	case http.StatusServiceUnavailable:
		fmt.Fprintf(os.Stderr, "Error: queue service unavailable\n")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

func runContinuousLogs(jobID string, filterStdout, filterStderr bool) error {
	lastIndex := 0
	fmt.Printf("Streaming logs for job %s (Ctrl+C to stop)\n", jobID)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	type runEvent struct {
		RunID    string `json:"run_id"`
		RunIndex int    `json:"run_index"`
	}

outer:
	for {
		attemptCtx, attemptCancel := context.WithCancel(context.Background())

		runChan := make(chan runEvent, 32)
		go func() {
			defer close(runChan)
			req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/sse/jobs/%s/runs", jobID), nil)
			if err != nil {
				return
			}
			req = req.WithContext(attemptCtx)
			req.Header.Set("Accept", "text/event-stream")

			resp, err := doAPIRequest(req)
			if err != nil {
				return
			}

			defer func() {
				attemptCancel()
				_ = resp.Body.Close()
			}()

			if resp.StatusCode != http.StatusOK {
				return
			}

			reader := bufio.NewReader(resp.Body)
			var dataBuf strings.Builder

			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					if attemptCtx.Err() != nil {
						return
					}
					if err == io.EOF {
						return
					}
					fmt.Fprintf(os.Stderr, "SSE error: %v\n", err)
					return
				}

				line = strings.TrimRight(line, "\r\n")
				if line == "" {
					if dataBuf.Len() == 0 {
						continue
					}

					message := []byte(dataBuf.String())
					dataBuf.Reset()

					var ev runEvent
					if err := json.Unmarshal(message, &ev); err != nil || ev.RunID == "" {
						continue
					}

					select {
					case runChan <- ev:
					default:
						fmt.Fprintf(os.Stderr, "Dropping run event (buffer full)\n")
					}
					continue
				}

				if after, ok := strings.CutPrefix(line, "data:"); ok {
					data := strings.TrimSpace(after)
					dataBuf.WriteString(data)
				}
			}
		}()

		req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/jobs/%s/runs?since=%d", jobID, lastIndex), nil)
		if err != nil {
			attemptCancel()
			return fmt.Errorf("creating runs request: %w", err)
		}

		resp, err := doAPIRequest(req)
		if err != nil {
			attemptCancel()
			return fmt.Errorf("fetching runs: %w", err)
		}
		var runs []struct {
			RunID    string `json:"run_id"`
			RunIndex int    `json:"run_index"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&runs); err != nil {
			resp.Body.Close()
			attemptCancel()
			return fmt.Errorf("parsing runs: %w", err)
		}
		resp.Body.Close()
		for _, r := range runs {
			if r.RunIndex > lastIndex {
				lastIndex = r.RunIndex
			}
		}

		for {
			select {
			case <-interrupt:
				attemptCancel()
				fmt.Println("\nStopping.")
				return fmt.Errorf("interrupted")
			case ev, ok := <-runChan:
				if !ok {
					attemptCancel()
					fmt.Fprintf(os.Stderr, "Runs connection closed; reconnecting...\n")
					continue outer
				}

				if ev.RunIndex > lastIndex {
					lastIndex = ev.RunIndex
				}

				if err := runLogStream(ev.RunID, filterStdout, filterStderr); err != nil {
					if err.Error() == "interrupted" {
						attemptCancel()
						return err
					}
					fmt.Fprintf(os.Stderr, "Error streaming run %s: %v\n", ev.RunID, err)
				}
			}
		}
	}
}

func resolveLogIDArg(arg string) (string, error) {
	if arg != "-" {
		return arg, nil
	}

	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", fmt.Errorf("reading from stdin: %w", err)
	}

	id := strings.TrimSpace(line)
	if id == "" {
		return "", fmt.Errorf("empty id from stdin")
	}

	return id, nil
}

func runLogsRun(cmd *cobra.Command, args []string) {
	id, err := resolveLogIDArg(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	filterStdout, _ := cmd.Flags().GetBool("stdout")
	filterStderr, _ := cmd.Flags().GetBool("stderr")
	if err := runLogStream(id, filterStdout, filterStderr); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runLogsJob(cmd *cobra.Command, args []string) {
	jobID, err := resolveLogIDArg(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	filterStdout, _ := cmd.Flags().GetBool("stdout")
	filterStderr, _ := cmd.Flags().GetBool("stderr")
	if err := runContinuousLogs(jobID, filterStdout, filterStderr); err != nil && err.Error() != "interrupted" {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: path or - is required")
		cmd.Usage()
		os.Exit(1)
	}

	source := args[0]
	var body []byte
	var err error
	if source == "-" {
		body, err = io.ReadAll(os.Stdin)
	} else {
		body, err = os.ReadFile(source)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to read job definition: %v\n", err)
		os.Exit(1)
	}

	var job api.Job
	if err := json.Unmarshal(body, &job); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid job JSON: %v\n", err)
		os.Exit(1)
	}

	if job.GetRoot() == nil {
		fmt.Fprintln(os.Stderr, "Error: job must have a root node")
		os.Exit(1)
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to submit job: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result struct {
			ID    string `json:"id"`
			RunID string `json:"run_id"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to parse response: %v\n", err)
			os.Exit(1)
		}

		if result.RunID == "" {
			fmt.Fprintln(os.Stderr, "Error: response missing run_id")
			os.Exit(1)
		}

		follow, _ := cmd.Flags().GetBool("follow")
		if follow {
			if err := runLogStream(result.RunID, false, false); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		} else {
			fmt.Println(result.RunID)
		}
	case http.StatusUnsupportedMediaType:
		fmt.Fprintln(os.Stderr, "Error: content type must be application/json")
		os.Exit(1)
	case http.StatusBadRequest:
		fmt.Fprintln(os.Stderr, "Error: invalid job definition")
		os.Exit(1)
	case http.StatusServiceUnavailable:
		fmt.Fprintln(os.Stderr, "Error: queue service unavailable")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

func fetchJobDefinitionBody(jobID string) ([]byte, int, error) {
	req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/jobs/%s", jobID), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create job definition request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch job definition: %w", err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to read job definition: %w", readErr)
	}

	return body, resp.StatusCode, nil
}

func formatJobDefinitionBody(body []byte, pretty bool) []byte {
	if !pretty {
		out := body
		if len(out) == 0 {
			return out
		}
		if !bytes.HasSuffix(out, []byte("\n")) {
			out = append(out, '\n')
		}
		return out
	}

	var indented bytes.Buffer
	if err := json.Indent(&indented, body, "", "  "); err != nil {
		out := body
		if len(out) == 0 {
			return out
		}

		if !bytes.HasSuffix(out, []byte("\n")) {
			out = append(out, '\n')
		}

		return out
	}

	out := indented.Bytes()
	if len(out) == 0 {
		return out
	}

	if !bytes.HasSuffix(out, []byte("\n")) {
		out = append(out, '\n')
	}

	return out
}

func editJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	jobID := args[0]
	body, statusCode, err := fetchJobDefinitionBody(jobID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	switch statusCode {
	case http.StatusOK:
		// NOTE(garrett): Continue
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status fetching job: %d\n", statusCode)
		os.Exit(1)
	}

	pretty := formatJobDefinitionBody(body, true)
	tempFile, err := os.CreateTemp("", "vectis-job-*.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create temp file: %v\n", err)
		os.Exit(1)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err := tempFile.Write(pretty); err != nil {
		tempFile.Close()
		fmt.Fprintf(os.Stderr, "Error: failed to write job definition to temp file: %v\n", err)
		os.Exit(1)
	}

	if err := tempFile.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to close temp file: %v\n", err)
		os.Exit(1)
	}

	editorEnv := os.Getenv("EDITOR")
	if editorEnv == "" {
		editorEnv = "vi"
	}

	editorParts := strings.Fields(editorEnv)
	if len(editorParts) == 0 {
		fmt.Fprintln(os.Stderr, "Error: EDITOR is empty after parsing")
		os.Exit(1)
	}

	editorName := editorParts[0]
	editorArgs := append(append([]string{}, editorParts[1:]...), tempPath)

	editCmd := exec.Command(editorName, editorArgs...)
	editCmd.Stdin = os.Stdin
	editCmd.Stdout = os.Stdout
	editCmd.Stderr = os.Stderr

	if err := editCmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() != 0 {
				os.Exit(exitErr.ExitCode())
			}
		}

		fmt.Fprintf(os.Stderr, "Error: editor failed: %v\n", err)
		os.Exit(1)
	}

	edited, err := os.ReadFile(tempPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to read edited job definition: %v\n", err)
		os.Exit(1)
	}

	var job api.Job
	if err := json.Unmarshal(edited, &job); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid job JSON after edit: %v\n", err)
		os.Exit(1)
	}

	if job.GetRoot() == nil {
		fmt.Fprintln(os.Stderr, "Error: job must have a root node")
		os.Exit(1)
	}

	if job.Id == nil || *job.Id != jobID {
		fmt.Fprintf(os.Stderr, "Error: job id mismatch (expected %q, got %v)\n", jobID, job.Id)
		os.Exit(1)
	}

	// NOTE(garrett): Always re-indent the stored job before updating.
	pretty, err = json.MarshalIndent(&job, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to normalize job JSON: %v\n", err)
		os.Exit(1)
	}
	pretty = append(pretty, '\n')

	req, err := newAPIRequest(http.MethodPut, fmt.Sprintf("/api/v1/jobs/%s", jobID), bytes.NewReader(pretty))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create update request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	updateResp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to update job: %v\n", err)
		os.Exit(1)
	}
	defer updateResp.Body.Close()

	switch updateResp.StatusCode {
	case http.StatusNoContent:
		fmt.Println("Job updated successfully.")
	case http.StatusBadRequest:
		fmt.Fprintln(os.Stderr, "Error: invalid job definition or id mismatch")
		os.Exit(1)
	case http.StatusUnsupportedMediaType:
		fmt.Fprintln(os.Stderr, "Error: content type must be application/json")
		os.Exit(1)
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status updating job: %s\n", updateResp.Status)
		os.Exit(1)
	}
}

func getJobDefinition(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	jobID := args[0]
	body, statusCode, err := fetchJobDefinitionBody(jobID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	switch statusCode {
	case http.StatusOK:
		// NOTE(garrett): Continue
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status fetching job: %d\n", statusCode)
		os.Exit(1)
	}

	raw, _ := cmd.Flags().GetBool("raw")
	out := formatJobDefinitionBody(body, !raw)
	fmt.Print(string(out))
}

func listJobs(cmd *cobra.Command, args []string) {
	if err := listJobNames(os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func createJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: path or - is required")
		cmd.Usage()
		os.Exit(1)
	}

	source := args[0]
	var body []byte
	var err error
	if source == "-" {
		body, err = io.ReadAll(os.Stdin)
	} else {
		body, err = os.ReadFile(source)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to read job definition: %v\n", err)
		os.Exit(1)
	}

	var job api.Job
	if err := json.Unmarshal(body, &job); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid job JSON: %v\n", err)
		os.Exit(1)
	}

	if job.Id == nil || *job.Id == "" {
		fmt.Fprintln(os.Stderr, "Error: job definition must include an id field")
		os.Exit(1)
	}

	if err := jobvalidation.ValidateJob(&job, jobvalidation.Options{RequireJobID: true}); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid job definition: %v\n", err)
		os.Exit(1)
	}

	namespace, _ := cmd.Flags().GetString("namespace")

	payload, err := json.Marshal(struct {
		Namespace string          `json:"namespace"`
		Job       json.RawMessage `json:"job"`
	}{
		Namespace: namespace,
		Job:       body,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to encode request: %v\n", err)
		os.Exit(1)
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(payload))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		fmt.Printf("Job %q stored.\n", *job.Id)
	case http.StatusConflict:
		fmt.Fprintf(os.Stderr, "Error: job %q already exists\n", *job.Id)
		os.Exit(1)
	case http.StatusBadRequest:
		fmt.Fprintln(os.Stderr, "Error: invalid job definition")
		os.Exit(1)
	case http.StatusUnsupportedMediaType:
		fmt.Fprintln(os.Stderr, "Error: content type must be application/json")
		os.Exit(1)
	case http.StatusServiceUnavailable:
		fmt.Fprintln(os.Stderr, "Error: database unavailable")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

func deleteJob(cmd *cobra.Command, args []string) {
	jobID := args[0]

	force, _ := cmd.Flags().GetBool("yes")
	if !force {
		fmt.Fprintf(os.Stderr, "Delete job %q? This removes the definition and prevents future triggers.\n", jobID)
		fmt.Fprintf(os.Stderr, "Re-run with --yes to confirm.\n")
		os.Exit(1)
	}

	req, err := newAPIRequest(http.MethodDelete, fmt.Sprintf("/api/v1/jobs/%s", jobID), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Printf("Job %q deleted.\n", jobID)
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job %q not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

func listJobNames(w io.Writer) error {
	req, err := newAPIRequest(http.MethodGet, "/api/v1/jobs", nil)
	if err != nil {
		return fmt.Errorf("failed to create list jobs request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to list jobs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status listing jobs: %s", resp.Status)
	}

	type jobListItem struct {
		Name string `json:"name"`
	}

	var jobsResp struct {
		Data []jobListItem `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&jobsResp); err != nil {
		return fmt.Errorf("failed to parse jobs response: %w", err)
	}

	names := make([]string, 0, len(jobsResp.Data))
	for _, j := range jobsResp.Data {
		if j.Name != "" {
			names = append(names, j.Name)
		}
	}

	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintln(w, name)
	}

	return nil
}

var triggerCmd = &cobra.Command{
	Use:   "trigger [job-id]",
	Short: "Trigger a stored job",
	Long: `Trigger a stored job by its job-id. The job must exist in the database.
The API records the run and returns immediately (202 with run_id); enqueue to the queue happens in the background, so a down queue does not block this command.`,
	Args: cobra.ExactArgs(1),
	Run:  triggerJob,
}

var logsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Stream logs for runs",
	Long:  `Stream logs via Server-Sent Events (SSE). Use "logs run" for a single run (until it completes). Use "logs job" to follow a job (runs triggered after you connect). Use "-" as the id to read from stdin.`,
}

var logsRunCmd = &cobra.Command{
	Use:   "run [run-id]",
	Short: "Stream logs for a single run until it completes",
	Long:  `Connect to the log stream for the given run-id and stream output until the run completes (server closes). Argument is a run-id; use "-" to read from stdin.`,
	Args:  cobra.ExactArgs(1),
	Run:   runLogsRun,
}

var logsJobCmd = &cobra.Command{
	Use:   "job [job-id]",
	Short: "Stream logs for a job (next runs only)",
	Long:  `Subscribe to run events for the job and stream logs for each run triggered after you connect. Does not stream historical runs. Re-subscribes if the runs connection closes. Argument is a job-id; use "-" to read from stdin.`,
	Args:  cobra.ExactArgs(1),
	Run:   runLogsJob,
}

var runCmd = &cobra.Command{
	Use:   "run [path|-]",
	Short: "Submit an ephemeral job",
	Long: `Submit a job definition to run once (ephemeral). Path is a JSON file; use "-" to read from stdin.
On success prints the job ID (and run_id with --follow); the API returns after persisting the run, then enqueues in the background.`,
	Args: cobra.ExactArgs(1),
	Run:  runJob,
}

var editCmd = &cobra.Command{
	Use:   "edit [job-id]",
	Short: "Edit a stored job definition using $EDITOR",
	Long:  `Fetch a stored job definition, open it in your $EDITOR, and update the job if you save and exit successfully.`,
	Args:  cobra.ExactArgs(1),
	Run:   editJob,
}

var getCmd = &cobra.Command{
	Use:   "get [job-id]",
	Short: "Get a stored job definition",
	Long:  `Fetch a stored job definition by its job-id and print it as JSON.`,
	Args:  cobra.ExactArgs(1),
	Run:   getJobDefinition,
}

var createCmd = &cobra.Command{
	Use:   "create [path|-]",
	Short: "Store a job definition",
	Long:  `Store a job definition for later trigger, edit, and delete. Path is a JSON file; use "-" to read from stdin.`,
	Args:  cobra.ExactArgs(1),
	Run:   createJob,
}

var deleteCmd = &cobra.Command{
	Use:   "delete [job-id]",
	Short: "Delete a stored job",
	Long:  `Delete a stored job definition. The job must exist. Pass --yes to skip confirmation.`,
	Args:  cobra.ExactArgs(1),
	Run:   deleteJob,
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List stored job ids",
	Long:  `Fetch all stored jobs and print each job id on its own line.`,
	Args:  cobra.NoArgs,
	Run:   listJobs,
}

func runMigrate(cmd *cobra.Command, args []string) {
	dbPath := database.GetDBPath()
	fmt.Printf("Migrating database: %s\n", dbPath)
	if err := database.Migrate(dbPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Migrations applied.")
}

func runGetRun(cmd *cobra.Command, args []string) {
	if err := getRun(args[0], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func getRun(runID string, w io.Writer) error {
	req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/runs/%s", runID), nil)
	if err != nil {
		return err
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var run struct {
			RunID          string  `json:"run_id"`
			RunIndex       int     `json:"run_index"`
			Status         string  `json:"status"`
			OrphanReason   *string `json:"orphan_reason,omitempty"`
			FailureCode    *string `json:"failure_code,omitempty"`
			StartedAt      *string `json:"started_at,omitempty"`
			FinishedAt     *string `json:"finished_at,omitempty"`
			FailureReason  *string `json:"failure_reason,omitempty"`
			DispatchEvents []struct {
				ID        int64   `json:"id"`
				Source    string  `json:"source"`
				EventType string  `json:"event_type"`
				Message   *string `json:"message,omitempty"`
				CreatedAt int64   `json:"created_at"`
			} `json:"dispatch_events,omitempty"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		fmt.Fprintf(w, "run_id=%s\n", run.RunID)
		fmt.Fprintf(w, "run_index=%d\n", run.RunIndex)
		fmt.Fprintf(w, "status=%s\n", run.Status)
		if run.StartedAt != nil {
			fmt.Fprintf(w, "started_at=%s\n", *run.StartedAt)
		}

		if run.FinishedAt != nil {
			fmt.Fprintf(w, "finished_at=%s\n", *run.FinishedAt)
		}

		if run.FailureCode != nil {
			fmt.Fprintf(w, "failure_code=%s\n", *run.FailureCode)
		}

		if run.FailureReason != nil {
			fmt.Fprintf(w, "failure_reason=%s\n", *run.FailureReason)
		}

		if run.OrphanReason != nil {
			fmt.Fprintf(w, "orphan_reason=%s\n", *run.OrphanReason)
		}

		if len(run.DispatchEvents) > 0 {
			fmt.Fprintln(w, "dispatch_events:")
			for _, ev := range run.DispatchEvents {
				ts := time.Unix(ev.CreatedAt, 0).UTC().Format(time.RFC3339)
				if ev.Message != nil {
					fmt.Fprintf(w, "  [%s] %s/%s: %s\n", ts, ev.Source, ev.EventType, *ev.Message)
				} else {
					fmt.Fprintf(w, "  [%s] %s/%s\n", ts, ev.Source, ev.EventType)
				}
			}
		}

		return nil
	case http.StatusNotFound:
		return fmt.Errorf("run %q not found", runID)
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func runCancelRun(cmd *cobra.Command, args []string) {
	if err := cancelRun(args[0], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func cancelRun(runID string, w io.Writer) error {
	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/runs/%s/cancel", runID), nil)
	if err != nil {
		return err
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Fprintf(w, "Run %s cancel requested.\n", runID)
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("run %q not found", runID)
	case http.StatusConflict:
		return fmt.Errorf("run %q is not cancellable", runID)
	case http.StatusBadGateway:
		return fmt.Errorf("failed to send cancel to worker")
	case http.StatusServiceUnavailable:
		return fmt.Errorf("worker resolution not configured")
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func runListRuns(cmd *cobra.Command, args []string) {
	if err := listRuns(runListJobID, runListLimit, runListSince, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func listRuns(jobID string, limit, since int, w io.Writer) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/runs", jobID)
	params := []string{}
	if limit > 0 {
		params = append(params, fmt.Sprintf("limit=%d", limit))
	}

	if since > 0 {
		params = append(params, fmt.Sprintf("since=%d", since))
	}

	if len(params) > 0 {
		path += "?" + strings.Join(params, "&")
	}

	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return err
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}

	var result struct {
		Data []struct {
			RunID         string  `json:"run_id"`
			RunIndex      int     `json:"run_index"`
			Status        string  `json:"status"`
			StartedAt     *string `json:"started_at,omitempty"`
			FinishedAt    *string `json:"finished_at,omitempty"`
			FailureCode   *string `json:"failure_code,omitempty"`
			FailureReason *string `json:"failure_reason,omitempty"`
		} `json:"data"`
		NextCursor *int64 `json:"next_cursor,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if len(result.Data) == 0 {
		fmt.Fprintln(w, "No runs found")
		return nil
	}

	fmt.Fprintf(w, "%-20s %-5s %-12s %-24s %-24s\n",
		"RUN ID", "INDEX", "STATUS", "STARTED", "FINISHED")

	for _, r := range result.Data {
		started := "-"
		if r.StartedAt != nil {
			started = *r.StartedAt
		}

		finished := "-"
		if r.FinishedAt != nil {
			finished = *r.FinishedAt
		}

		fmt.Fprintf(w, "%-20s %-5d %-12s %-24s %-24s\n",
			r.RunID, r.RunIndex, r.Status, started, finished)
	}

	if result.NextCursor != nil {
		fmt.Fprintf(w, "\nMore runs available (cursor: %d)\n", *result.NextCursor)
	}

	return nil
}

func forceFailRun(cmd *cobra.Command, args []string) {
	runID := args[0]
	reason, _ := cmd.Flags().GetString("reason")

	body := []byte("{}")
	if reason != "" {
		payload, err := json.Marshal(map[string]string{"reason": reason})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to encode request body: %v\n", err)
			os.Exit(1)
		}
		body = payload
	}

	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/runs/%s/force-fail", runID), bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Printf("Run %s force-failed.\n", runID)
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: run '%s' not found\n", runID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

func forceRequeueRun(cmd *cobra.Command, args []string) {
	runID := args[0]
	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/runs/%s/force-requeue", runID), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create request: %v\n", err)
		os.Exit(1)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Printf("Run %s force-requeued.\n", runID)
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: run '%s' not found\n", runID)
		os.Exit(1)
	case http.StatusConflict:
		fmt.Fprintf(os.Stderr, "Error: run '%s' is already succeeded and cannot be requeued\n", runID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply database migrations (admin / one-shot)",
	Long: `Run embedded SQL migrations against the database selected by VECTIS_DATABASE_DRIVER and VECTIS_DATABASE_DSN (or defaults).

Runtime services only wait for the schema; they do not migrate. Use this command (or CI/deploy automation) before starting the stack.`,
	Args: cobra.NoArgs,
	Run:  runMigrate,
}

var forceFailCmd = &cobra.Command{
	Use:   "force-fail [run-id]",
	Short: "Manually mark a run as failed",
	Long:  `Force a run into failed status in the API/database. Intended for manual intervention flows.`,
	Args:  cobra.ExactArgs(1),
	Run:   forceFailRun,
}

var forceRequeueCmd = &cobra.Command{
	Use:   "force-requeue [run-id]",
	Short: "Manually requeue a run",
	Long:  `Force a run back to queued status for manual retry/recovery. Intended for manual intervention flows.`,
	Args:  cobra.ExactArgs(1),
	Run:   forceRequeueRun,
}

var deployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Manage first-class Vectis deployment paths",
}

var deployPodmanCmd = &cobra.Command{
	Use:   "podman",
	Short: "Manage the Podman staging/reference deployment",
}

var deployPodmanInitCmd = &cobra.Command{
	Use:   "init",
	Short: "Generate local Podman deployment secrets",
	Args:  cobra.NoArgs,
	Run:   runDeployPodmanInit,
}

var deployPodmanRenderCmd = &cobra.Command{
	Use:   "render",
	Short: "Render the Podman deployment manifest",
	Args:  cobra.NoArgs,
	Run:   runDeployPodmanRender,
}

var deployPodmanUpCmd = &cobra.Command{
	Use:   "up",
	Short: "Start or replace the Podman deployment and apply migrations",
	Args:  cobra.NoArgs,
	Run:   runDeployPodmanUp,
}

var deployPodmanDownCmd = &cobra.Command{
	Use:   "down",
	Short: "Stop the Podman deployment",
	Args:  cobra.NoArgs,
	Run:   runDeployPodmanDown,
}

var deployPodmanStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show Podman deployment status",
	Args:  cobra.NoArgs,
	Run:   runDeployPodmanStatus,
}

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Remove local Vectis config, data, cache, and generated deploy state",
	Long: `Remove local Vectis application support/config, data, cache, CLI tokens, and generated deployment state.

This is a destructive local reset. It does not stop running services or delete remote/container volumes.`,
	Args: cobra.NoArgs,
	Run:  runReset,
}

var runGetCmd = &cobra.Command{
	Use:   "get [run-id]",
	Short: "Get run status and failure details",
	Long:  `Fetch a run by run-id and print operator-facing status and failure fields.`,
	Args:  cobra.ExactArgs(1),
	Run:   runGetRun,
}

var runCancelCmd = &cobra.Command{
	Use:   "cancel [run-id]",
	Short: "Request cancellation for an executing run",
	Long:  `Request cancellation for an executing run through the worker control path.`,
	Args:  cobra.ExactArgs(1),
	Run:   runCancelRun,
}

var runListCmd = &cobra.Command{
	Use:   "list",
	Short: "List runs for a job",
	Long:  `List runs for a stored job, most recent first. Supports pagination via --limit and cursor.`,
	Args:  cobra.NoArgs,
	Run:   runListRuns,
}

func runLogin(cmd *cobra.Command, args []string) {
	username, _ := cmd.Flags().GetString("username")
	var password string

	if username == "" {
		fmt.Fprint(os.Stderr, "Username: ")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			username = scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to read username: %v\n", err)
			os.Exit(1)
		}
	}

	if password == "" {
		fmt.Fprint(os.Stderr, "Password: ")
		b, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			// Fallback to plain scanner if not a terminal
			scanner := bufio.NewScanner(os.Stdin)
			if scanner.Scan() {
				password = scanner.Text()
			}
			if scanErr := scanner.Err(); scanErr != nil {
				fmt.Fprintf(os.Stderr, "Error: failed to read password: %v\n", scanErr)
				os.Exit(1)
			}
		} else {
			password = string(b)
			fmt.Fprintln(os.Stderr)
		}
	}

	if username == "" || password == "" {
		fmt.Fprintln(os.Stderr, "Error: username and password are required")
		cmd.Usage()
		os.Exit(1)
	}

	token, err := doLogin(username, password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if err := writePersistedToken(token); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to save token: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Logged in as %s.\n", username)
}

func doLogin(username, password string) (string, error) {
	body, err := json.Marshal(map[string]string{
		"username": username,
		"password": password,
	})
	if err != nil {
		return "", fmt.Errorf("failed to encode request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, config.PublicAPIBaseURL()+"/api/v1/login", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := apiHTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("login request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result struct {
			Token     string `json:"token"`
			UserID    int64  `json:"user_id"`
			ExpiresAt string `json:"expires_at"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return "", fmt.Errorf("failed to parse response: %w", err)
		}

		return result.Token, nil
	case http.StatusUnauthorized:
		return "", fmt.Errorf("invalid username or password")
	case http.StatusServiceUnavailable:
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("service unavailable: %s", string(body))
	default:
		return "", fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Authenticate with the Vectis API",
	Long:  `Log in to the Vectis API using username and password. The token is persisted to the config directory for subsequent commands.`,
	Run:   runLogin,
}

func runLogout(cmd *cobra.Command, args []string) {
	if err := deletePersistedToken(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to remove token: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Logged out. Token removed.")
}

var logoutCmd = &cobra.Command{
	Use:   "logout",
	Short: "Remove the persisted API token",
	Long:  `Remove the locally persisted API token. This does not invalidate the token on the server.`,
	Run:   runLogout,
}

func runTokenList(cmd *cobra.Command, args []string) {
	if err := tokenList(os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func tokenList(w io.Writer) error {
	req, err := newAPIRequest(http.MethodGet, "/api/v1/tokens", nil)
	if err != nil {
		return err
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}

	var tokens []struct {
		ID         int64   `json:"id"`
		Label      string  `json:"label"`
		ExpiresAt  *string `json:"expires_at,omitempty"`
		CreatedAt  string  `json:"created_at"`
		LastUsedAt *string `json:"last_used_at,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tokens); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	for _, t := range tokens {
		exp := "never"
		if t.ExpiresAt != nil {
			exp = *t.ExpiresAt
		}
		used := "never"
		if t.LastUsedAt != nil {
			used = *t.LastUsedAt
		}
		fmt.Fprintf(w, "%d\t%s\texpires=%s\tcreated=%s\tlast_used=%s\n", t.ID, t.Label, exp, t.CreatedAt, used)
	}
	return nil
}

func runTokenCreate(cmd *cobra.Command, args []string) {
	label, _ := cmd.Flags().GetString("label")
	expiresIn, _ := cmd.Flags().GetString("expires-in")
	userID, _ := cmd.Flags().GetInt64("user-id")

	if label == "" {
		fmt.Fprintln(os.Stderr, "Error: --label is required")
		cmd.Usage()
		os.Exit(1)
	}

	if err := tokenCreate(label, expiresIn, userID, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func tokenCreate(label, expiresIn string, userID int64, w io.Writer) error {
	reqBody := map[string]any{
		"label":      label,
		"expires_in": expiresIn,
	}

	if userID > 0 {
		reqBody["user_id"] = userID
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to encode request: %w", err)
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		var result struct {
			Token     string `json:"token"`
			ID        int64  `json:"id"`
			Label     string `json:"label"`
			ExpiresAt string `json:"expires_at"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		fmt.Fprintf(w, "Token created: %d (%s)\n%s\n", result.ID, result.Label, result.Token)
		if result.ExpiresAt != "" {
			fmt.Fprintf(w, "Expires: %s\n", result.ExpiresAt)
		}
		return nil
	case http.StatusForbidden:
		return fmt.Errorf("permission denied")
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func runTokenDelete(cmd *cobra.Command, args []string) {
	if err := tokenDelete(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func tokenDelete(tokenID string) error {
	req, err := newAPIRequest(http.MethodDelete, fmt.Sprintf("/api/v1/tokens/%s", tokenID), nil)
	if err != nil {
		return err
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Println("Token deleted.")
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("token not found")
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

var tokenCmd = &cobra.Command{
	Use:   "token",
	Short: "Manage API tokens",
	Long:  `List, create, and delete API tokens for the authenticated user.`,
}

var tokenListCmd = &cobra.Command{
	Use:   "list",
	Short: "List API tokens",
	Run:   runTokenList,
}

var tokenCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new API token",
	Run:   runTokenCreate,
}

var tokenDeleteCmd = &cobra.Command{
	Use:   "delete [token-id]",
	Short: "Delete an API token",
	Args:  cobra.ExactArgs(1),
	Run:   runTokenDelete,
}

var rootCmd = &cobra.Command{
	Use:   "vectis-cli",
	Short: "Vectis CLI - Command line interface for Vectis",
	Long:  `Vectis CLI provides commands to interact with the Vectis build system.`,
}

func init() {
	cli.ConfigureVersion(rootCmd)

	logsRunCmd.Flags().Bool("stdout", false, "Only show stdout")
	logsRunCmd.Flags().Bool("stderr", false, "Only show stderr")
	logsJobCmd.Flags().Bool("stdout", false, "Only show stdout")
	logsJobCmd.Flags().Bool("stderr", false, "Only show stderr")

	logsCmd.AddCommand(logsRunCmd)
	logsCmd.AddCommand(logsJobCmd)

	triggerCmd.Flags().BoolP("follow", "f", false, "After triggering, stream logs (same as logs run <run-id>)")
	runCmd.Flags().BoolP("follow", "f", false, "After submitting, stream logs (same as logs run <run-id>)")
	runCmd.AddCommand(runGetCmd)
	runCmd.AddCommand(runCancelCmd)

	runListCmd.Flags().StringVar(&runListJobID, "job", "", "Job ID to list runs for (required)")
	runListCmd.MarkFlagRequired("job")
	runListCmd.Flags().IntVar(&runListLimit, "limit", 0, "Max runs to return (default 50)")
	runListCmd.Flags().IntVar(&runListSince, "since", 0, "Only list runs after this run index")
	runCmd.AddCommand(runListCmd)

	rootCmd.AddCommand(triggerCmd)
	rootCmd.AddCommand(logsCmd)
	rootCmd.AddCommand(runCmd)
	getCmd.Flags().Bool("raw", false, "Print definition JSON without reformatting")
	rootCmd.AddCommand(getCmd)
	createCmd.Flags().String("namespace", "", "Namespace to store the job in (default: /)")
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(editCmd)
	deleteCmd.Flags().Bool("yes", false, "Skip confirmation prompt")
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(migrateCmd)
	forceFailCmd.Flags().String("reason", "", "Failure reason to record")
	rootCmd.AddCommand(forceFailCmd)
	rootCmd.AddCommand(forceRequeueCmd)

	deployPodmanCmd.PersistentFlags().StringVar(&podmanNetwork, "network", "pasta", "Podman network mode for play kube")
	deployPodmanCmd.PersistentFlags().StringVar(&podmanKubeSpec, "kube-spec", defaultPodmanKubeSpec, "Path to the Podman kube spec template")
	deployPodmanCmd.PersistentFlags().StringVar(&podmanGrafanaSpec, "grafana-spec", defaultPodmanGrafanaSpec, "Path to generated Grafana ConfigMaps")
	deployPodmanInitCmd.Flags().Bool("rotate", false, "Regenerate deployment secrets")
	deployPodmanRenderCmd.Flags().Bool("rotate", false, "Regenerate deployment secrets before rendering")
	deployPodmanRenderCmd.Flags().StringVarP(&podmanRenderOut, "output", "o", "-", "Rendered manifest output path, or '-' for stdout")
	deployPodmanCmd.AddCommand(deployPodmanInitCmd)
	deployPodmanCmd.AddCommand(deployPodmanRenderCmd)
	deployPodmanCmd.AddCommand(deployPodmanUpCmd)
	deployPodmanCmd.AddCommand(deployPodmanDownCmd)
	deployPodmanCmd.AddCommand(deployPodmanStatusCmd)
	deployCmd.AddCommand(deployPodmanCmd)
	rootCmd.AddCommand(deployCmd)

	resetCmd.Flags().BoolVar(&resetYes, "yes", false, "Confirm removal of local Vectis directories")
	resetCmd.Flags().BoolVar(&resetDryRun, "dry-run", false, "Print the directories that would be removed")
	rootCmd.AddCommand(resetCmd)

	loginCmd.Flags().StringP("username", "u", "", "Username (optional; prompts if omitted)")
	rootCmd.AddCommand(loginCmd)
	rootCmd.AddCommand(logoutCmd)

	tokenCmd.AddCommand(tokenListCmd)
	tokenCreateCmd.Flags().String("label", "", "Token label (required)")
	tokenCreateCmd.Flags().String("expires-in", "never", "Expiry preset (1w, 1m, 3m, 6m, 1y, never)")
	tokenCreateCmd.Flags().Int64("user-id", 0, "Create token for another user (admin only)")
	tokenCmd.AddCommand(tokenCreateCmd)
	tokenCmd.AddCommand(tokenDeleteCmd)
	rootCmd.AddCommand(tokenCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
