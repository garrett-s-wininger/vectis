package ldap

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	ldapauth "vectis/extensions/auth/ldap"
	"vectis/internal/api"
	"vectis/internal/api/ratelimit"
	"vectis/internal/cache"
	"vectis/internal/interfaces"
	"vectis/internal/migrations"
	sdkauth "vectis/sdk/auth"

	_ "vectis/internal/dbdrivers"
)

const (
	DefaultAPISmokeBootstrapToken = "sixteenchars----"
	DefaultAPISmokeAdminUsername  = "root"
	DefaultAPISmokeAdminPassword  = "longenough"
)

type APISmokeOptions struct {
	LDAP                 ldapauth.SmokeOptions
	BootstrapToken       string
	AdminUsername        string
	AdminPassword        string
	Timeout              time.Duration
	Stdout               io.Writer
	loginProvider        sdkauth.LoginProvider
	openDB               func() (*sql.DB, error)
	authenticatedAPIPath string
}

type APISmokeResult struct {
	Status                    string `json:"status"`
	LDAPURL                   string `json:"ldap_url"`
	APIURL                    string `json:"api_url"`
	Username                  string `json:"username"`
	UserID                    int64  `json:"user_id"`
	TokenReturned             bool   `json:"token_returned"`
	AuthenticatedRequestOK    bool   `json:"authenticated_request_ok"`
	WrongPasswordDenied       bool   `json:"wrong_password_denied"`
	AutoProvisionedLocalUser  bool   `json:"auto_provisioned_local_user"`
	AuthenticatedAPIProbePath string `json:"authenticated_api_probe_path"`
}

func RunAPISmoke(ctx context.Context, opts APISmokeOptions) (APISmokeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	opts = normalizeAPISmokeOptions(opts)
	if err := validateAPISmokeOptions(opts); err != nil {
		return APISmokeResult{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	var lastErr error
	for {
		result, err := runAPISmokeOnce(ctx, opts)
		if err == nil {
			return result, nil
		}

		lastErr = err
		select {
		case <-ctx.Done():
			return APISmokeResult{}, fmt.Errorf("ldap API smoke did not succeed within %s: %w", opts.Timeout, lastErr)
		case <-time.After(time.Second):
			fmt.Fprintf(opts.Stdout, "Waiting for LDAP-backed API login path: %v\n", err)
		}
	}
}

func normalizeAPISmokeOptions(opts APISmokeOptions) APISmokeOptions {
	opts.LDAP.URL = strings.TrimSpace(opts.LDAP.URL)
	if opts.LDAP.URL == "" {
		opts.LDAP.URL = ldapauth.DefaultSmokeURL
	}

	opts.LDAP.BindDN = strings.TrimSpace(opts.LDAP.BindDN)
	opts.LDAP.BindPassword = strings.TrimSpace(opts.LDAP.BindPassword)
	opts.LDAP.BindPasswordFile = strings.TrimSpace(opts.LDAP.BindPasswordFile)
	opts.LDAP.BaseDN = strings.TrimSpace(opts.LDAP.BaseDN)
	if opts.LDAP.BaseDN == "" {
		opts.LDAP.BaseDN = ldapauth.DefaultSmokeBaseDN
	}

	opts.LDAP.UserFilter = strings.TrimSpace(opts.LDAP.UserFilter)
	opts.LDAP.UsernameAttribute = strings.TrimSpace(opts.LDAP.UsernameAttribute)
	opts.LDAP.DisplayNameAttribute = strings.TrimSpace(opts.LDAP.DisplayNameAttribute)

	opts.LDAP.Username = strings.TrimSpace(opts.LDAP.Username)
	if opts.LDAP.Username == "" {
		opts.LDAP.Username = ldapauth.DefaultSmokeUsername
	}

	opts.LDAP.Password = strings.TrimSpace(opts.LDAP.Password)
	if opts.LDAP.Password == "" {
		opts.LDAP.Password = ldapauth.DefaultSmokePassword
	}

	opts.LDAP.WrongPassword = strings.TrimSpace(opts.LDAP.WrongPassword)
	if opts.LDAP.WrongPassword == "" {
		opts.LDAP.WrongPassword = ldapauth.DefaultSmokeWrongPassword
	}

	opts.BootstrapToken = strings.TrimSpace(opts.BootstrapToken)
	if opts.BootstrapToken == "" {
		opts.BootstrapToken = DefaultAPISmokeBootstrapToken
	}

	opts.AdminUsername = strings.TrimSpace(opts.AdminUsername)
	if opts.AdminUsername == "" {
		opts.AdminUsername = DefaultAPISmokeAdminUsername
	}

	if opts.AdminPassword == "" {
		opts.AdminPassword = DefaultAPISmokeAdminPassword
	}

	if opts.Timeout == 0 {
		opts.Timeout = ldapauth.DefaultSmokeTimeout
	}

	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}

	if opts.openDB == nil {
		opts.openDB = openSmokeDB
	}

	opts.authenticatedAPIPath = strings.TrimSpace(opts.authenticatedAPIPath)
	if opts.authenticatedAPIPath == "" {
		opts.authenticatedAPIPath = "/api/v1/version"
	}

	return opts
}

func validateAPISmokeOptions(opts APISmokeOptions) error {
	if opts.loginProvider == nil && opts.LDAP.URL == "" {
		return fmt.Errorf("ldap API smoke url is required")
	}

	if opts.loginProvider == nil && opts.LDAP.BaseDN == "" {
		return fmt.Errorf("ldap API smoke base dn is required")
	}

	if opts.LDAP.Username == "" {
		return fmt.Errorf("ldap API smoke username is required")
	}

	if opts.LDAP.Password == "" {
		return fmt.Errorf("ldap API smoke password is required")
	}

	if opts.Timeout <= 0 {
		return fmt.Errorf("ldap API smoke timeout must be > 0")
	}

	return nil
}

func runAPISmokeOnce(ctx context.Context, opts APISmokeOptions) (APISmokeResult, error) {
	db, err := opts.openDB()
	if err != nil {
		return APISmokeResult{}, err
	}
	defer db.Close()

	provider, err := apiSmokeLoginProvider(opts)
	if err != nil {
		return APISmokeResult{}, err
	}

	restoreEnv := setEnvForAPISmoke(map[string]string{
		"VECTIS_API_AUTH_ENABLED":         "true",
		"VECTIS_API_AUTH_BOOTSTRAP_TOKEN": opts.BootstrapToken,
		"VECTIS_API_AUTHZ_ENGINE":         "authenticated_full",
	})
	defer restoreEnv()

	cacheService := cache.NewMemoryService()
	apiServer := api.NewAPIServer(interfaces.NewLogger("ldap-api-smoke").WithOutput(io.Discard), db)
	apiServer.SetCacheService(cacheService)
	apiServer.SetRateLimiter(ratelimit.NewCacheRateLimiter(cacheService))
	apiServer.SetLoginProviderRegistrations([]api.LoginProviderRegistration{{
		ID:       ldapauth.DefaultProviderID,
		Kind:     ldapauth.ProviderKind,
		Provider: provider,
	}})

	apiServer.SetExternalLoginAutoProvision(true)
	httpServer := httptest.NewServer(apiServer.Handler())
	defer httpServer.Close()

	client := httpServer.Client()
	if err := postAPIJSON(ctx, client, httpServer.URL+"/api/v1/setup/complete", map[string]any{
		"bootstrap_token": opts.BootstrapToken,
		"admin_username":  opts.AdminUsername,
		"admin_password":  opts.AdminPassword,
	}, http.StatusOK, nil); err != nil {
		return APISmokeResult{}, err
	}

	var login loginResponse
	if err := postAPIJSON(ctx, client, httpServer.URL+"/api/v1/login", map[string]any{
		"username":     opts.LDAP.Username,
		"password":     opts.LDAP.Password,
		"return_token": true,
	}, http.StatusOK, &login); err != nil {
		return APISmokeResult{}, err
	}

	if login.UserID == 0 || strings.TrimSpace(login.Token) == "" {
		return APISmokeResult{}, fmt.Errorf("ldap API smoke login returned user_id=%d token_present=%t", login.UserID, strings.TrimSpace(login.Token) != "")
	}

	if err := getAuthenticatedAPI(ctx, client, httpServer.URL+opts.authenticatedAPIPath, login.Token); err != nil {
		return APISmokeResult{}, err
	}

	wrongPasswordDenied := false
	if opts.LDAP.WrongPassword != "" {
		err := postAPIJSON(ctx, client, httpServer.URL+"/api/v1/login", map[string]any{
			"username": opts.LDAP.Username,
			"password": opts.LDAP.WrongPassword,
		}, http.StatusUnauthorized, nil)

		if err != nil {
			return APISmokeResult{}, err
		}

		wrongPasswordDenied = true
	}

	autoProvisioned, err := localUserExists(ctx, db, opts.LDAP.Username)
	if err != nil {
		return APISmokeResult{}, err
	}

	if !autoProvisioned {
		return APISmokeResult{}, fmt.Errorf("ldap API smoke did not auto-provision local user %q", opts.LDAP.Username)
	}

	return APISmokeResult{
		Status:                    "ok",
		LDAPURL:                   opts.LDAP.URL,
		APIURL:                    httpServer.URL,
		Username:                  opts.LDAP.Username,
		UserID:                    login.UserID,
		TokenReturned:             true,
		AuthenticatedRequestOK:    true,
		WrongPasswordDenied:       wrongPasswordDenied,
		AutoProvisionedLocalUser:  true,
		AuthenticatedAPIProbePath: opts.authenticatedAPIPath,
	}, nil
}

func apiSmokeLoginProvider(opts APISmokeOptions) (sdkauth.LoginProvider, error) {
	if opts.loginProvider != nil {
		return opts.loginProvider, nil
	}

	bindPassword, err := ldapBindPassword(opts.LDAP)
	if err != nil {
		return nil, err
	}

	return ldapauth.NewProvider(ldapauth.ProviderOptions{
		URL:                  opts.LDAP.URL,
		BindDN:               opts.LDAP.BindDN,
		BindPassword:         bindPassword,
		BaseDN:               opts.LDAP.BaseDN,
		UserFilter:           opts.LDAP.UserFilter,
		UsernameAttribute:    opts.LDAP.UsernameAttribute,
		DisplayNameAttribute: opts.LDAP.DisplayNameAttribute,
		StartTLS:             opts.LDAP.StartTLS,
		Timeout:              opts.Timeout,
	})
}

func ldapBindPassword(opts ldapauth.SmokeOptions) (string, error) {
	if opts.BindPasswordFile == "" {
		return opts.BindPassword, nil
	}

	data, err := os.ReadFile(opts.BindPasswordFile)
	if err != nil {
		return "", fmt.Errorf("read ldap bind password file: %w", err)
	}

	return strings.TrimSpace(string(data)), nil
}

func openSmokeDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open smoke sqlite db: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := migrations.Run(db, "sqlite3"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("run smoke sqlite migrations: %w", err)
	}

	return db, nil
}

type loginResponse struct {
	Token  string `json:"token"`
	UserID int64  `json:"user_id"`
}

func postAPIJSON(ctx context.Context, client *http.Client, url string, body map[string]any, wantStatus int, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != wantStatus {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return fmt.Errorf("POST %s status=%d want %d body=%s", url, resp.StatusCode, wantStatus, string(respBody))
	}

	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode POST %s response: %w", url, err)
	}

	return nil
}

func getAuthenticatedAPI(ctx context.Context, client *http.Client, url, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return fmt.Errorf("GET %s status=%d want 200 body=%s", url, resp.StatusCode, string(respBody))
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func localUserExists(ctx context.Context, db *sql.DB, username string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM local_users WHERE username = ?`, username).Scan(&count); err != nil {
		return false, err
	}

	return count == 1, nil
}

func setEnvForAPISmoke(values map[string]string) func() {
	type oldValue struct {
		value string
		ok    bool
	}

	old := make(map[string]oldValue, len(values))
	for key, value := range values {
		current, ok := os.LookupEnv(key)
		old[key] = oldValue{value: current, ok: ok}
		_ = os.Setenv(key, value)
	}

	return func() {
		for key, value := range old {
			if value.ok {
				_ = os.Setenv(key, value.value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	}
}
