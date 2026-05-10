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
	"vectis/internal/retention"
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

func setIdempotencyHeader(req *http.Request, key string) {
	if key = strings.TrimSpace(key); key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
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
	retentionYes      bool
	retentionDryRun   bool
	retentionRunAge   time.Duration
	retentionDefAge   time.Duration
	retentionIdemAge  time.Duration
	retentionAuditAge time.Duration
	retentionLogDir   string
	runListJobID      string
	runListLimit      int
	runListSince      int
	triggerIdemKey    string
	runIdemKey        string
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
	setIdempotencyHeader(req, triggerIdemKey)

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
	setIdempotencyHeader(req, runIdemKey)

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

func runRetentionCleanup(cmd *cobra.Command, args []string) {
	policy := retention.Policy{
		TerminalRuns:    retentionRunAge,
		JobDefinitions:  retentionDefAge,
		IdempotencyKeys: retentionIdemAge,
		AuditLog:        retentionAuditAge,
	}

	if err := retentionCleanup(cmd.Context(), os.Stdout, policy, retentionDryRun, retentionYes, retentionLogDir); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func retentionCleanup(ctx context.Context, w io.Writer, policy retention.Policy, dryRun, yes bool, logStorageDir string) error {
	if !dryRun && !yes {
		return fmt.Errorf("retention cleanup deletes durable records; pass --dry-run to inspect or --yes to apply")
	}

	dbPath := database.GetDBPath()
	db, err := database.OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := database.WaitForMigrations(db, nil); err != nil {
		return err
	}

	cleaner := retention.NewSQLCleaner(db)
	now := time.Now().UTC()

	var fileReport retention.FileReport
	if logStorageDir != "" {
		runIDs, err := cleaner.TerminalRunIDs(ctx, policy.TerminalRuns, now)
		if err != nil {
			return fmt.Errorf("list terminal run logs: %w", err)
		}

		logCleaner := retention.LocalRunLogCleaner{Dir: logStorageDir}
		if dryRun {
			fileReport, err = logCleaner.Preview(runIDs)
		} else {
			fileReport, err = logCleaner.Delete(runIDs)
		}
		if err != nil {
			return err
		}
	}

	var report retention.Report
	if dryRun {
		report, err = cleaner.Preview(ctx, policy, now)
	} else {
		report, err = cleaner.Apply(ctx, policy, now)
	}

	if err != nil {
		return err
	}

	printRetentionReport(w, report, fileReport)
	if dryRun {
		fmt.Fprintln(w, "Cleanup not applied.")
		return nil
	}

	fmt.Fprintln(w, "Cleanup applied.")
	return nil
}

func printRetentionReport(w io.Writer, report retention.Report, fileReport retention.FileReport) {
	prefix := "deleted"
	if report.DryRun {
		prefix = "would_delete"
	}

	fmt.Fprintf(w, "dry_run=%t\n", report.DryRun)
	fmt.Fprintf(w, "cutoff.terminal_runs=%s\n", retentionCutoff(report.Cutoffs.TerminalRuns))
	fmt.Fprintf(w, "cutoff.job_definitions=%s\n", retentionCutoff(report.Cutoffs.JobDefinitions))
	fmt.Fprintf(w, "cutoff.idempotency_keys=%s\n", retentionCutoff(report.Cutoffs.IdempotencyKeys))
	fmt.Fprintf(w, "cutoff.audit_log=%s\n", retentionCutoff(report.Cutoffs.AuditLog))
	fmt.Fprintf(w, "%s.terminal_runs=%d\n", prefix, report.Counts.TerminalRuns)
	fmt.Fprintf(w, "%s.run_dispatch_events=%d\n", prefix, report.Counts.RunDispatchEvents)
	fmt.Fprintf(w, "%s.job_definitions=%d\n", prefix, report.Counts.JobDefinitions)
	fmt.Fprintf(w, "%s.idempotency_keys=%d\n", prefix, report.Counts.IdempotencyKeys)
	fmt.Fprintf(w, "%s.audit_log=%d\n", prefix, report.Counts.AuditLog)
	fmt.Fprintf(w, "%s.run_log_files=%d\n", prefix, fileReport.RunLogFiles)
	fmt.Fprintf(w, "%s.run_log_bytes=%d\n", prefix, fileReport.RunLogBytes)
	fmt.Fprintf(w, "audit_event_inserted=%t\n", report.AuditEventInserted)
}

func retentionCutoff(t *time.Time) string {
	if t == nil {
		return "disabled"
	}

	return t.UTC().Format(time.RFC3339)
}

type doctorStatus string

const (
	doctorOK   doctorStatus = "pass"
	doctorWarn doctorStatus = "warn"
	doctorFail doctorStatus = "fail"
)

type doctorSeverity string

const (
	severityCritical doctorSeverity = "critical"
	severityWarning  doctorSeverity = "warning"
)

type doctorCheck struct {
	ID              string         `json:"id"`
	Title           string         `json:"title"`
	Status          doctorStatus   `json:"status"`
	Severity        doctorSeverity `json:"severity"`
	Summary         string         `json:"summary"`
	Evidence        string         `json:"evidence,omitempty"`
	SuggestedAction string         `json:"action,omitempty"`
	DocLink         string         `json:"doc,omitempty"`
}

var doctorJSON bool
var doctorStrict bool

func runDoctor(cmd *cobra.Command, args []string) {
	doctorJSON, _ = cmd.Flags().GetBool("json")
	doctorStrict, _ = cmd.Flags().GetBool("strict")

	if err := doctor(os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func doctor(w io.Writer) error {
	checks := []doctorCheck{
		doctorHTTPStatus("api.live", http.MethodGet, "/health/live", http.StatusOK, "API liveness probe passed", severityCritical, "API liveness", "Check API server process", "docs/RUNBOOKS.md"),
		doctorHTTPStatus("api.ready", http.MethodGet, "/health/ready", http.StatusOK, "API readiness probe passed", severityCritical, "API readiness", "Check API server and dependencies (DB, queue)", "docs/RUNBOOKS.md"),
		doctorSetupStatus(),
		doctorCLIToken(),
		doctorSchemaCurrent(),
		doctorReconcilerActive(),
		doctorAuditDrops(),
		doctorDBPool(),
		doctorQueueBacklog(),
		doctorStuckRuns(),
		doctorLogReachable(),
		doctorAuditFlushFailures(),
	}

	if doctorJSON {
		return writeDoctorJSON(w, checks)
	}

	return writeDoctorText(w, checks)
}

func writeDoctorText(w io.Writer, checks []doctorCheck) error {
	for _, check := range checks {
		fmt.Fprintf(w, "%s\t%s\t%s\n", check.Status, check.ID, check.Summary)
	}

	return evaluateDoctorChecks(checks)
}

func evaluateDoctorChecks(checks []doctorCheck) error {
	failed := false
	warned := false

	for _, check := range checks {
		if check.Status == doctorFail {
			failed = true
		}
		if check.Status == doctorWarn {
			warned = true
		}
	}

	if failed {
		return fmt.Errorf("one or more doctor checks failed")
	}

	if doctorStrict && warned {
		return fmt.Errorf("one or more doctor checks reported warnings (--strict)")
	}

	return nil
}

func writeDoctorJSON(w io.Writer, checks []doctorCheck) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(checks); err != nil {
		return err
	}
	return evaluateDoctorChecks(checks)
}

func doctorHTTPStatus(id, method, path string, want int, okMessage string, severity doctorSeverity, title, action, doc string) doctorCheck {
	req, err := newAPIRequest(method, path, nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severity, Summary: err.Error(), SuggestedAction: action, DocLink: doc}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severity, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: action, DocLink: doc}
	}
	defer resp.Body.Close()

	if resp.StatusCode != want {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severity, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: action, DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severity, Summary: okMessage, DocLink: doc}
}

func doctorSetupStatus() doctorCheck {
	const id = "setup.status"
	title := "Setup complete"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/setup/status", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server reachability", DocLink: "docs/RUNBOOKS.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "docs/RUNBOOKS.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "docs/RUNBOOKS.md"}
	}

	var result struct {
		SetupComplete bool `json:"setup_complete"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "docs/RUNBOOKS.md"}
	}

	if result.SetupComplete {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "initial setup is complete", DocLink: "docs/RUNBOOKS.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "initial setup is not complete", SuggestedAction: "Complete setup via the API or CLI", DocLink: "docs/RUNBOOKS.md"}
}

func doctorCLIToken() doctorCheck {
	const id = "cli.token"
	title := "CLI token present"
	if effectiveToken() == "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "no CLI API token configured", SuggestedAction: "Set VECTIS_API_TOKEN or run login", DocLink: "docs/OPERATIONS_02_REPAIR_WORKFLOWS.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "CLI API token is configured"}
}

func doctorSchemaCurrent() doctorCheck {
	const id = "db.schema.current"
	title := "Database schema current"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/schema/status", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	var result struct {
		CurrentVersion int  `json:"current_version"`
		HasSchema      bool `json:"has_schema"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	if !result.HasSchema {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: "no schema found — database may be uninitialized", SuggestedAction: "Run vectis-cli migrate", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityCritical, Summary: fmt.Sprintf("schema at version %d", result.CurrentVersion), Evidence: fmt.Sprintf("%d", result.CurrentVersion), DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
}

func doctorReconcilerActive() doctorCheck {
	const id = "reconciler.active"
	title := "Reconciler heartbeat recent"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/reconciler/heartbeat", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	var result struct {
		Active bool `json:"active"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	if !result.Active {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "no recent reconciler activity detected", SuggestedAction: "Restart reconciler; check reconciler DB connection", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "reconciler has recent activity", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
}

func doctorAuditDrops() doctorCheck {
	const id = "audit.drops.recent"
	title := "No recent audit drops"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/audit/drops", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	var result struct {
		Dropped int64 `json:"dropped"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	if result.Dropped > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d audit events dropped", result.Dropped), Evidence: fmt.Sprintf("%d", result.Dropped), SuggestedAction: "Check audit buffer configuration; check DB write capacity", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no audit events dropped", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
}

func doctorDBPool() doctorCheck {
	const id = "db.connection.pool"
	title := "DB connection pool healthy"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/db/pool-stats", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	var result struct {
		OpenConnections int   `json:"open_connections"`
		InUse           int   `json:"in_use"`
		WaitCount       int64 `json:"wait_count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	if result.OpenConnections > 0 && result.InUse == result.OpenConnections && result.WaitCount > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("pool exhausted: %d in-use / %d open, %d waits", result.InUse, result.OpenConnections, result.WaitCount), Evidence: fmt.Sprintf("in_use=%d open=%d wait=%d", result.InUse, result.OpenConnections, result.WaitCount), SuggestedAction: "Increase max connections or check slow queries", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("pool healthy: %d open, %d in-use", result.OpenConnections, result.InUse), Evidence: fmt.Sprintf("open=%d in_use=%d", result.OpenConnections, result.InUse), DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
}

func doctorQueueBacklog() doctorCheck {
	const id = "queue.backlog.ratio"
	title := "Queue backlog within threshold"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/queue/backlog", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	var result struct {
		Queued int64 `json:"queued"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	if result.Queued > 100 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("backlog high: %d queued", result.Queued), Evidence: fmt.Sprintf("%d", result.Queued), SuggestedAction: "Check queue service health and worker count", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("backlog ok: %d queued", result.Queued), Evidence: fmt.Sprintf("%d", result.Queued), DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
}

func doctorStuckRuns() doctorCheck {
	const id = "reconciler.stuck.runs"
	title := "No stuck runs beyond threshold"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/reconciler/stuck-runs", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	var result struct {
		Stuck int64 `json:"stuck"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	if result.Stuck > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d stuck runs detected", result.Stuck), Evidence: fmt.Sprintf("%d", result.Stuck), SuggestedAction: "Check reconciler; check dispatch path", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no stuck runs", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
}

func doctorLogReachable() doctorCheck {
	const id = "log.reachable"
	title := "Log service reachable"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/log/reachable", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	var result struct {
		Reachable bool `json:"reachable"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	if !result.Reachable {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "log service is not reachable", SuggestedAction: "Check log service connectivity; check log DB", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "log service is reachable", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
}

func doctorAuditFlushFailures() doctorCheck {
	const id = "audit.flush.failures"
	title := "No recent audit flush failures"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/audit/flush-failures", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	var result struct {
		FlushFailures int64 `json:"flush_failures"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	if result.FlushFailures > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d audit flush failures", result.FlushFailures), Evidence: fmt.Sprintf("%d", result.FlushFailures), SuggestedAction: "Check audit persistence; check DB write capacity", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no audit flush failures", DocLink: "docs/DOCTOR_CHECK_CATALOG.md"}
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

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run operational diagnostics",
	Long: `Run a stable, versioned set of operational checks against the configured Vectis API.

Check IDs are frozen between releases (see docs/DOCTOR_CHECK_CATALOG.md for the catalog).

Output is tab-separated: status, check ID, and summary message.
  --json   emits the full check model as a JSON array.
  --strict treats warnings as exit-nonzero (for CI).

Failed checks always exit non-zero.`,
	Args: cobra.NoArgs,
	Run:  runDoctor,
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

var retentionCmd = &cobra.Command{
	Use:   "retention",
	Short: "Manage durable data retention",
}

var retentionCleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Prune old terminal runs and related durable records",
	Long: `Prune old terminal run rows, dispatch events, orphaned ephemeral job definitions,
idempotency keys, audit log rows, and optionally local durable run log files.

The command is destructive. Use --dry-run first, then pass --yes to apply.`,
	Args: cobra.NoArgs,
	Run:  runRetentionCleanup,
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
	triggerCmd.Flags().StringVar(&triggerIdemKey, "idempotency-key", "", "Optional Idempotency-Key header for safe trigger retries")
	runCmd.Flags().BoolP("follow", "f", false, "After submitting, stream logs (same as logs run <run-id>)")
	runCmd.Flags().StringVar(&runIdemKey, "idempotency-key", "", "Optional Idempotency-Key header for safe ephemeral run retries")
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
	doctorCmd.Flags().Bool("json", false, "Emit output as a JSON array")
	doctorCmd.Flags().Bool("strict", false, "Exit non-zero on warnings")
	rootCmd.AddCommand(doctorCmd)
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

	defaultRetention := retention.DefaultPolicy()
	retentionCleanupCmd.Flags().BoolVar(&retentionYes, "yes", false, "Confirm deletion of retention-eligible records")
	retentionCleanupCmd.Flags().BoolVar(&retentionDryRun, "dry-run", false, "Print the records that would be deleted")
	retentionCleanupCmd.Flags().DurationVar(&retentionRunAge, "terminal-run-age", defaultRetention.TerminalRuns, "Delete succeeded/failed runs older than this duration (0 disables)")
	retentionCleanupCmd.Flags().DurationVar(&retentionDefAge, "job-definition-age", defaultRetention.JobDefinitions, "Delete unreferenced ephemeral job definitions older than this duration (0 disables)")
	retentionCleanupCmd.Flags().DurationVar(&retentionIdemAge, "idempotency-age", defaultRetention.IdempotencyKeys, "Delete idempotency keys older than this duration (0 disables)")
	retentionCleanupCmd.Flags().DurationVar(&retentionAuditAge, "audit-age", defaultRetention.AuditLog, "Delete audit log rows older than this duration (0 disables)")
	retentionCleanupCmd.Flags().StringVar(&retentionLogDir, "log-storage-dir", "", "Optional durable run log directory to prune for deleted terminal runs")
	retentionCmd.AddCommand(retentionCleanupCmd)
	rootCmd.AddCommand(retentionCmd)

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

	namespaceCreateCmd.Flags().Int64("parent-id", 0, "Parent namespace ID (default: root)")
	namespaceCmd.AddCommand(namespaceListCmd, namespaceGetCmd, namespaceCreateCmd, namespaceDeleteCmd)
	rootCmd.AddCommand(namespaceCmd)

	userCreateCmd.Flags().String("password", "", "Initial password (default: generated by API)")
	userUpdateCmd.Flags().Bool("enabled", true, "Set whether the user is enabled")
	userChangePasswordCmd.Flags().Int64("user-id", 0, "Target user ID (default: current user)")
	userChangePasswordCmd.Flags().String("current-password", "", "Current password for self-service password changes")
	userChangePasswordCmd.Flags().String("new-password", "", "New password")
	userCmd.AddCommand(userListCmd, userGetCmd, userCreateCmd, userUpdateCmd, userDeleteCmd, userChangePasswordCmd)
	rootCmd.AddCommand(userCmd)

	roleBindingCmd.AddCommand(roleBindingListCmd, roleBindingCreateCmd, roleBindingDeleteCmd)
	rootCmd.AddCommand(roleBindingCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
