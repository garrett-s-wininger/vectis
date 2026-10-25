package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/localpki"
	"vectis/internal/platform"

	"github.com/spf13/viper"
	"golang.org/x/term"
)

const (
	localSetupAuto            = "auto"
	localSetupOff             = "off"
	localAdminPasswordFile    = "local-admin-password"
	localAdminUsernameDefault = "admin"
	localAdminPasswordMinLen  = 8
	localSetupPollTimeout     = 30 * time.Second
	localSetupPollInterval    = 200 * time.Millisecond
)

type localSetupResult struct {
	Skipped       bool
	OperatorToken bool
	Created       bool
	AlreadyExists bool
	Username      string
	TokenPath     string
	TokenExisted  bool
	PasswordPath  string
	Err           error
}

type localAdminCredentials struct {
	Username  string
	Password  string
	Generated bool
}

func localSetupMode() string {
	mode := strings.TrimSpace(viper.GetString("setup"))
	if mode == "" {
		return localSetupAuto
	}

	return mode
}

func validLocalSetupMode(mode string) bool {
	switch mode {
	case localSetupAuto, localSetupOff:
		return true
	default:
		return false
	}
}

func localBootstrapFromOperator(source string) bool {
	return strings.HasPrefix(source, "from VECTIS_API_AUTH_BOOTSTRAP_TOKEN")
}

func localSetupSkipsAuto(source string) bool {
	if !viper.GetBool("auth_enabled") || localSetupMode() == localSetupOff {
		return true
	}

	return localBootstrapFromOperator(source)
}

func localSetupAPIHost() string {
	switch localHost() {
	case "0.0.0.0", "::", "[::]":
		return "127.0.0.1"
	default:
		return localHost()
	}
}

func localSetupBaseURL(scheme string) string {
	return fmt.Sprintf("%s://%s:%d", scheme, localSetupAPIHost(), config.APIEffectiveListenPort())
}

func localSetupHTTPClient(https bool, material *localpki.Material) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if https {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}

		if material != nil && material.CAFile != "" {
			pemBytes, err := os.ReadFile(material.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read local CA: %w", err)
			}

			if !pool.AppendCertsFromPEM(pemBytes) {
				return nil, fmt.Errorf("local CA at %s contained no certificates", material.CAFile)
			}
		}

		transport.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    pool,
			ServerName: "localhost",
		}
	}

	return &http.Client{Timeout: 10 * time.Second, Transport: transport}, nil
}

func finishLocalSetup(
	ctx context.Context,
	logger interfaces.Logger,
	baseURL string,
	material *localpki.Material,
	bootstrapToken, bootstrapSource string,
	in *os.File,
	out io.Writer,
) localSetupResult {
	if localSetupSkipsAuto(bootstrapSource) {
		return localSetupResult{Skipped: true, OperatorToken: localBootstrapFromOperator(bootstrapSource)}
	}

	client, err := localSetupHTTPClient(strings.HasPrefix(baseURL, "https://"), material)
	if err != nil {
		return localSetupResult{Err: err}
	}

	status, err := waitForSetupStatus(ctx, client, baseURL)
	if err != nil {
		return localSetupResult{Err: err}
	}

	if !status.AuthEnabled || status.SetupComplete {
		return existingLocalSetupResult(status.SetupComplete)
	}

	creds, passwordPath, err := localAdminCredentialsForBoot(in, out)
	if err != nil {
		return localSetupResult{Err: err}
	}

	tokenPath, err := localCLITokenPath()
	if err != nil {
		return localSetupResult{Err: err}
	}

	result, err := runLocalSetup(ctx, client, baseURL, bootstrapToken, creds, tokenPath, passwordPath)
	if err != nil {
		return localSetupResult{Err: err}
	}

	return result
}

type setupStatusBody struct {
	SetupComplete bool `json:"setup_complete"`
	AuthEnabled   bool `json:"auth_enabled"`
}

func waitForSetupStatus(ctx context.Context, client *http.Client, baseURL string) (setupStatusBody, error) {
	deadline := time.NewTimer(localSetupPollTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(localSetupPollInterval)
	defer ticker.Stop()

	var lastErr error
	for {
		if localAPIExited() {
			return setupStatusBody{}, fmt.Errorf("vectis-api exited before it accepted setup")
		}

		status, err := fetchSetupStatus(ctx, client, baseURL)
		if err == nil {
			return status, nil
		}

		lastErr = err
		select {
		case <-ctx.Done():
			return setupStatusBody{}, ctx.Err()
		case <-deadline.C:
			return setupStatusBody{}, fmt.Errorf("local API setup status: %w", lastErr)
		case <-ticker.C:
		}
	}
}

func fetchSetupStatus(ctx context.Context, client *http.Client, baseURL string) (setupStatusBody, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/api/v1/setup/status", nil)
	if err != nil {
		return setupStatusBody{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return setupStatusBody{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return setupStatusBody{}, fmt.Errorf("setup status returned %s", resp.Status)
	}

	var status setupStatusBody
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return setupStatusBody{}, fmt.Errorf("decode setup status: %w", err)
	}

	return status, nil
}

func existingLocalSetupResult(setupComplete bool) localSetupResult {
	result := localSetupResult{AlreadyExists: setupComplete}
	if !setupComplete {
		return result
	}

	path, err := localCLITokenPath()
	if err != nil {
		return result
	}

	existing, err := os.ReadFile(path)
	if err != nil || strings.TrimSpace(string(existing)) == "" {
		return result
	}

	result.TokenPath = path
	result.TokenExisted = true
	return result
}

func localAdminCredentialsForBoot(in *os.File, out io.Writer) (localAdminCredentials, string, error) {
	if in != nil && term.IsTerminal(int(in.Fd())) {
		username, password, err := promptLocalAdmin(in, out)
		if err != nil {
			return localAdminCredentials{}, "", err
		}

		return localAdminCredentials{Username: username, Password: password}, "", nil
	}

	password, err := randomHex(16)
	if err != nil {
		return localAdminCredentials{}, "", err
	}

	return localAdminCredentials{
		Username:  localAdminUsernameDefault,
		Password:  password,
		Generated: true,
	}, localAdminPasswordPath(), nil
}

func promptLocalAdmin(in *os.File, out io.Writer) (string, string, error) {
	fmt.Fprint(out, "Create the local admin.\nUsername [admin]: ")
	scanner := bufio.NewScanner(in)
	username := localAdminUsernameDefault
	if scanner.Scan() {
		if text := strings.TrimSpace(scanner.Text()); text != "" {
			username = text
		}
	}

	if err := scanner.Err(); err != nil {
		return "", "", fmt.Errorf("read admin username: %w", err)
	}

	for range 3 {
		fmt.Fprint(out, "Password: ")
		password, err := term.ReadPassword(int(in.Fd()))
		fmt.Fprintln(out)
		if err != nil {
			return "", "", fmt.Errorf("read admin password: %w", err)
		}

		if len(password) >= localAdminPasswordMinLen {
			return username, string(password), nil
		}

		fmt.Fprintf(out, "Password must be at least %d characters.\n", localAdminPasswordMinLen)
	}

	return "", "", fmt.Errorf("password must be at least %d characters", localAdminPasswordMinLen)
}

func localAdminPasswordPath() string {
	return filepath.Join(platform.DataHome(), "vectis", localAdminPasswordFile)
}

func localCLITokenPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "vectis", "token"), nil
}

func runLocalSetup(
	ctx context.Context,
	client *http.Client,
	baseURL, bootstrapToken string,
	creds localAdminCredentials,
	tokenPath, passwordPath string,
) (localSetupResult, error) {
	body, err := json.Marshal(map[string]string{
		"bootstrap_token": bootstrapToken,
		"admin_username":  creds.Username,
		"admin_password":  creds.Password,
	})

	if err != nil {
		return localSetupResult{}, err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		baseURL+"/api/v1/setup/complete",
		bytes.NewReader(body),
	)
	if err != nil {
		return localSetupResult{}, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return localSetupResult{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusConflict:
		return localSetupResult{AlreadyExists: true, Username: creds.Username}, nil
	case http.StatusOK:
	default:
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return localSetupResult{}, fmt.Errorf(
			"setup complete returned %s: %s",
			resp.Status,
			strings.TrimSpace(string(payload)),
		)
	}

	var created struct {
		APIToken string `json:"api_token"`
		Username string `json:"username"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return localSetupResult{}, fmt.Errorf("decode setup response: %w", err)
	}

	if strings.TrimSpace(created.APIToken) == "" {
		return localSetupResult{}, fmt.Errorf("setup response did not include an API token")
	}

	result := localSetupResult{Created: true, Username: created.Username}
	if result.Username == "" {
		result.Username = creds.Username
	}

	if creds.Generated {
		if err := writePrivateFile(passwordPath, []byte(creds.Password+"\n")); err != nil {
			return localSetupResult{}, fmt.Errorf("write local admin password: %w", err)
		}

		result.PasswordPath = passwordPath
	}

	existed, err := writeCLITokenIfAbsent(tokenPath, created.APIToken)
	if err != nil {
		return localSetupResult{}, fmt.Errorf("write CLI token: %w", err)
	}

	result.TokenPath = tokenPath
	result.TokenExisted = existed
	return result, nil
}

func writeCLITokenIfAbsent(path, token string) (bool, error) {
	if existing, err := os.ReadFile(path); err == nil && strings.TrimSpace(string(existing)) != "" {
		return true, nil
	} else if err != nil && !os.IsNotExist(err) {
		return false, err
	}

	if err := writePrivateFile(path, []byte(token)); err != nil {
		return false, err
	}

	return false, nil
}

func writePrivateFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	if err := os.WriteFile(path, data, 0o600); err != nil {
		return err
	}

	return os.Chmod(path, 0o600)
}

func printLocalReady(logger interfaces.Logger, scheme string, services []serviceStage, setup localSetupResult) {
	logger.Info("Ready.")
	logger.Info("API %s", localSetupBaseURL(scheme))
	if hasService(services, "vectis-ui") {
		logger.Info("UI http://%s:%d", localSetupAPIHost(), viper.GetInt("ui_port"))
	}

	if hasService(services, "vectis-docs") {
		logger.Info("Docs %s://%s:%d", scheme, localSetupAPIHost(), viper.GetInt("docs_port"))
	}

	switch {
	case setup.Created:
		logger.Info("Local admin %q is ready.", setup.Username)
		if setup.PasswordPath != "" {
			logger.Info("Password written to %s", setup.PasswordPath)
		}

		printCLITokenLine(logger, setup)
		if hasService(services, "vectis-ui") {
			logger.Info("Sign in at the UI as %s.", setup.Username)
		}
	case setup.AlreadyExists:
		logger.Info("Local admin already exists.")
		if setup.TokenPath == "" {
			logger.Info("Run ./bin/vectis-cli auth login to save a CLI token.")
		} else {
			printCLITokenLine(logger, setup)
		}
	case setup.OperatorToken:
		logger.Info("Setup left for the supplied bootstrap token.")
	}

	if len(localSourceRepositorySpecs()) == 0 && !viper.GetBool("config_as_code") {
		logger.Info("Source repository vectis-local is ready for local job authoring on main.")
	}

	logger.Info("Next: ./bin/vectis-cli jobs run examples/sequenced.json --follow")
}

func printCLITokenLine(logger interfaces.Logger, setup localSetupResult) {
	if setup.TokenPath == "" {
		return
	}

	if setup.TokenExisted {
		logger.Info("CLI token already present at %s", setup.TokenPath)
		return
	}

	logger.Info("CLI token saved to %s", setup.TokenPath)
}
