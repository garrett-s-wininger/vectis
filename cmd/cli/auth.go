package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"golang.org/x/term"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"vectis/internal/config"
)

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

func effectiveToken() string {
	if token := config.CLIAPIToken(); token != "" {
		return token
	}

	return readPersistedToken()
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

var authCmd = &cobra.Command{
	Use:     "auth",
	Short:   "Log in, log out, and manage API tokens",
	GroupID: cliGroupAccess,
	Run:     showCommandHelp,
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
	Use:   "tokens",
	Short: "List, create, and delete API tokens",
	Long:  `List, create, and delete API tokens for the authenticated user.`,
	Run:   showCommandHelp,
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

func configureLoginFlags(cmd *cobra.Command) {
	cmd.Flags().StringP("username", "u", "", "Username (optional; prompts if omitted)")
}

func configureTokenCreateFlags(cmd *cobra.Command) {
	cmd.Flags().String("label", "", "Token label (required)")
	cmd.Flags().String("expires-in", "never", "Expiry preset (1w, 1m, 3m, 6m, 1y, never)")
	cmd.Flags().Int64("user-id", 0, "Create token for another user (admin only)")
}
