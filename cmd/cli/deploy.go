package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	podmanassets "vectis/deploy/podman"
	"vectis/internal/database"
)

const (
	defaultPodmanKubeSpec    = "deploy/podman/kube-spec.yaml"
	defaultPodmanGrafanaSpec = "deploy/podman/grafana-configmaps.gen.yaml"
	podmanSecretName         = "vectis-podman-secrets"
	envDeployConfigDir       = "VECTIS_DEPLOY_CONFIG_DIR"
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

	// NOTE(garrett): playArgs are fixed podman subcommands plus manifestPath from our rendered output.
	podman := exec.CommandContext(cmd.Context(), "podman", playArgs...) //#nosec G204
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
	podman := exec.CommandContext(cmd.Context(), "podman", playArgs...) //#nosec G204
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

var deployCmd = &cobra.Command{
	Use:     "deploy",
	Short:   "Manage first-class Vectis deployment paths",
	GroupID: cliGroupOperations,
	Run:     showCommandHelp,
}

var deployPodmanCmd = &cobra.Command{
	Use:   "podman",
	Short: "Manage the Podman staging/reference deployment",
	Run:   showCommandHelp,
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
