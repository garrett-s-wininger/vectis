package ldap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	sdkauth "vectis/sdk/auth"
)

const (
	DefaultSmokeURL                 = "ldap://127.0.0.1:1389"
	DefaultSmokeBaseDN              = "ou=people,dc=example,dc=org"
	DefaultSmokeBindDN              = "cn=vectis,dc=example,dc=org"
	DefaultSmokeUsername            = "alice"
	DefaultSmokePassword            = "alice-secret"
	DefaultSmokeWrongPassword       = "wrong-secret"
	DefaultSmokeExpectedSubject     = "uid=alice,ou=people,dc=example,dc=org"
	DefaultSmokeExpectedDisplayName = "Alice Example"
	DefaultSmokeTimeout             = 30 * time.Second
)

type SmokeOptions struct {
	URL                  string
	BindDN               string
	BindPassword         string
	BindPasswordFile     string
	BaseDN               string
	UserFilter           string
	SubjectAttribute     string
	UsernameAttribute    string
	DisplayNameAttribute string
	StartTLS             bool
	Username             string
	Password             string
	WrongPassword        string
	ExpectedSubject      string
	ExpectedUsername     string
	ExpectedDisplayName  string
	Timeout              time.Duration
	Stdout               io.Writer

	dial func(context.Context) (conn, error)
}

type SmokeResult struct {
	Status              string              `json:"status"`
	URL                 string              `json:"url"`
	BaseDN              string              `json:"base_dn"`
	Username            string              `json:"username"`
	Subject             string              `json:"subject"`
	DisplayName         string              `json:"display_name,omitempty"`
	Attributes          map[string][]string `json:"attributes,omitempty"`
	WrongPasswordDenied bool                `json:"wrong_password_denied"`
}

func RunSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	opts = normalizeSmokeOptions(opts)
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	var lastErr error
	for {
		result, err := runSmokeOnce(ctx, opts)
		if err == nil {
			return result, nil
		}

		lastErr = err
		select {
		case <-ctx.Done():
			return SmokeResult{}, fmt.Errorf("ldap smoke did not succeed within %s: %w", opts.Timeout, lastErr)
		case <-time.After(time.Second):
			fmt.Fprintf(opts.Stdout, "Waiting for LDAP endpoint %s: %v\n", opts.URL, err)
		}
	}
}

func normalizeSmokeOptions(opts SmokeOptions) SmokeOptions {
	opts.URL = strings.TrimSpace(opts.URL)
	opts.BindDN = strings.TrimSpace(opts.BindDN)
	opts.BindPassword = strings.TrimSpace(opts.BindPassword)
	opts.BindPasswordFile = strings.TrimSpace(opts.BindPasswordFile)
	opts.BaseDN = strings.TrimSpace(opts.BaseDN)
	if opts.BaseDN == "" {
		opts.BaseDN = DefaultSmokeBaseDN
	}

	opts.UserFilter = strings.TrimSpace(opts.UserFilter)
	if opts.UserFilter == "" {
		opts.UserFilter = defaultUserFilter
	}

	opts.SubjectAttribute = strings.TrimSpace(opts.SubjectAttribute)

	opts.UsernameAttribute = strings.TrimSpace(opts.UsernameAttribute)
	if opts.UsernameAttribute == "" {
		opts.UsernameAttribute = defaultUsernameAttribute
	}

	opts.DisplayNameAttribute = strings.TrimSpace(opts.DisplayNameAttribute)
	if opts.DisplayNameAttribute == "" {
		opts.DisplayNameAttribute = defaultDisplayNameAttribute
	}

	opts.Username = strings.TrimSpace(opts.Username)
	if opts.Username == "" {
		opts.Username = DefaultSmokeUsername
	}

	opts.Password = strings.TrimSpace(opts.Password)
	if opts.Password == "" {
		opts.Password = DefaultSmokePassword
	}

	opts.WrongPassword = strings.TrimSpace(opts.WrongPassword)
	if opts.WrongPassword == "" {
		opts.WrongPassword = DefaultSmokeWrongPassword
	}

	opts.ExpectedSubject = strings.TrimSpace(opts.ExpectedSubject)
	opts.ExpectedUsername = strings.TrimSpace(opts.ExpectedUsername)
	if opts.ExpectedUsername == "" {
		opts.ExpectedUsername = opts.Username
	}

	opts.ExpectedDisplayName = strings.TrimSpace(opts.ExpectedDisplayName)

	if opts.Timeout == 0 {
		opts.Timeout = DefaultSmokeTimeout
	}

	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}

	return opts
}

func validateSmokeOptions(opts SmokeOptions) error {
	if opts.URL == "" {
		return fmt.Errorf("ldap smoke url is required")
	}

	if opts.BaseDN == "" {
		return fmt.Errorf("ldap smoke base dn is required")
	}

	if opts.Username == "" {
		return fmt.Errorf("ldap smoke username is required")
	}

	if opts.Password == "" {
		return fmt.Errorf("ldap smoke password is required")
	}

	if opts.Timeout <= 0 {
		return fmt.Errorf("ldap smoke timeout must be > 0")
	}

	return nil
}

func runSmokeOnce(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	bindPassword, err := smokeBindPassword(opts)
	if err != nil {
		return SmokeResult{}, err
	}

	provider, err := NewProvider(ProviderOptions{
		URL:                  opts.URL,
		BindDN:               opts.BindDN,
		BindPassword:         bindPassword,
		BaseDN:               opts.BaseDN,
		UserFilter:           opts.UserFilter,
		SubjectAttribute:     opts.SubjectAttribute,
		UsernameAttribute:    opts.UsernameAttribute,
		DisplayNameAttribute: opts.DisplayNameAttribute,
		StartTLS:             opts.StartTLS,
		Timeout:              opts.Timeout,
		Dial:                 opts.dial,
	})

	if err != nil {
		return SmokeResult{}, err
	}

	identity, err := provider.Authenticate(ctx, opts.Username, opts.Password)
	if err != nil {
		return SmokeResult{}, err
	}

	if err := validateSmokeIdentity(opts, identity); err != nil {
		return SmokeResult{}, err
	}

	wrongPasswordDenied := false
	if opts.WrongPassword != "" {
		_, err := provider.Authenticate(ctx, opts.Username, opts.WrongPassword)
		if !errors.Is(err, sdkauth.ErrInvalidCredentials) {
			return SmokeResult{}, fmt.Errorf("ldap smoke wrong password error = %v, want invalid credentials", err)
		}

		wrongPasswordDenied = true
	}

	return SmokeResult{
		Status:              "ok",
		URL:                 opts.URL,
		BaseDN:              opts.BaseDN,
		Username:            identity.Username,
		Subject:             identity.Subject,
		DisplayName:         identity.DisplayName,
		Attributes:          cloneAttributes(identity.Attributes),
		WrongPasswordDenied: wrongPasswordDenied,
	}, nil
}

func smokeBindPassword(opts SmokeOptions) (string, error) {
	if opts.BindPasswordFile == "" {
		return opts.BindPassword, nil
	}

	data, err := os.ReadFile(opts.BindPasswordFile)
	if err != nil {
		return "", fmt.Errorf("read ldap smoke bind password file: %w", err)
	}

	return strings.TrimSpace(string(data)), nil
}

func validateSmokeIdentity(opts SmokeOptions, identity sdkauth.Identity) error {
	if identity.Provider != DefaultProviderID {
		return fmt.Errorf("ldap smoke provider = %q, want %q", identity.Provider, DefaultProviderID)
	}

	if opts.ExpectedSubject != "" && identity.Subject != opts.ExpectedSubject {
		return fmt.Errorf("ldap smoke subject = %q, want %q", identity.Subject, opts.ExpectedSubject)
	}

	if opts.ExpectedUsername != "" && identity.Username != opts.ExpectedUsername {
		return fmt.Errorf("ldap smoke username = %q, want %q", identity.Username, opts.ExpectedUsername)
	}

	if opts.ExpectedDisplayName != "" && identity.DisplayName != opts.ExpectedDisplayName {
		return fmt.Errorf("ldap smoke display name = %q, want %q", identity.DisplayName, opts.ExpectedDisplayName)
	}

	if opts.ExpectedUsername != "" {
		values := identity.Attributes[opts.UsernameAttribute]
		for _, value := range values {
			if value == opts.ExpectedUsername {
				return nil
			}
		}

		encoded, _ := json.Marshal(values)
		return fmt.Errorf("ldap smoke %s attribute = %s, want %q", opts.UsernameAttribute, encoded, opts.ExpectedUsername)
	}

	return nil
}

func cloneAttributes(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string][]string, len(in))
	for key, values := range in {
		out[key] = append([]string(nil), values...)
	}

	return out
}
