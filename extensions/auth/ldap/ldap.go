package ldap

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	goldap "github.com/go-ldap/ldap/v3"

	sdkauth "vectis/sdk/auth"
)

const providerName = "ldap"

type ProviderOptions struct {
	URL                  string
	BindDN               string
	BindPassword         string
	BaseDN               string
	UserFilter           string
	UsernameAttribute    string
	DisplayNameAttribute string
	StartTLS             bool
	Timeout              time.Duration
	TLSConfig            *tls.Config
	Dial                 func(context.Context) (conn, error)
}

type Provider struct {
	url                  string
	bindDN               string
	bindPassword         string
	baseDN               string
	userFilter           string
	usernameAttribute    string
	displayNameAttribute string
	startTLS             bool
	timeout              time.Duration
	tlsConfig            *tls.Config
	dial                 func(context.Context) (conn, error)
}

type conn interface {
	Bind(username, password string) error
	Search(*goldap.SearchRequest) (*goldap.SearchResult, error)
	StartTLS(*tls.Config) error
	Close() error
}

func NewProvider(opts ProviderOptions) (*Provider, error) {
	rawURL := strings.TrimSpace(opts.URL)
	if rawURL == "" {
		return nil, fmt.Errorf("ldap url is required")
	}

	if _, err := url.Parse(rawURL); err != nil {
		return nil, fmt.Errorf("parse ldap url: %w", err)
	}

	baseDN := strings.TrimSpace(opts.BaseDN)
	if baseDN == "" {
		return nil, fmt.Errorf("ldap base dn is required")
	}

	userFilter := strings.TrimSpace(opts.UserFilter)
	if userFilter == "" {
		userFilter = defaultUserFilter
	}

	if !strings.Contains(userFilter, "{username}") {
		return nil, fmt.Errorf("ldap user filter must contain {username}")
	}

	usernameAttribute := strings.TrimSpace(opts.UsernameAttribute)
	if usernameAttribute == "" {
		usernameAttribute = defaultUsernameAttribute
	}

	displayNameAttribute := strings.TrimSpace(opts.DisplayNameAttribute)
	if displayNameAttribute == "" {
		displayNameAttribute = defaultDisplayNameAttribute
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	p := &Provider{
		url:                  rawURL,
		bindDN:               strings.TrimSpace(opts.BindDN),
		bindPassword:         opts.BindPassword,
		baseDN:               baseDN,
		userFilter:           userFilter,
		usernameAttribute:    usernameAttribute,
		displayNameAttribute: displayNameAttribute,
		startTLS:             opts.StartTLS,
		timeout:              timeout,
		tlsConfig:            opts.TLSConfig,
		dial:                 opts.Dial,
	}

	if p.bindDN == "" && strings.TrimSpace(p.bindPassword) != "" {
		return nil, fmt.Errorf("ldap bind password requires ldap bind dn")
	}

	if p.dial == nil {
		p.dial = p.dialLDAP
	}

	return p, nil
}

func (p *Provider) Authenticate(ctx context.Context, username, password string) (sdkauth.Identity, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	username = strings.TrimSpace(username)
	if username == "" || password == "" {
		return sdkauth.Identity{}, sdkauth.ErrInvalidCredentials
	}

	authCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	c, err := p.dial(authCtx)
	if err != nil {
		return sdkauth.Identity{}, fmt.Errorf("%w: dial ldap: %v", sdkauth.ErrUnavailable, err)
	}
	defer c.Close()

	if p.startTLS {
		if err := c.StartTLS(p.effectiveTLSConfig()); err != nil {
			return sdkauth.Identity{}, fmt.Errorf("%w: ldap starttls: %v", sdkauth.ErrUnavailable, err)
		}
	}

	if p.bindDN != "" {
		if err := c.Bind(p.bindDN, p.bindPassword); err != nil {
			return sdkauth.Identity{}, fmt.Errorf("%w: ldap service bind: %v", sdkauth.ErrUnavailable, err)
		}
	}

	entry, err := p.searchUser(authCtx, c, username)
	if err != nil {
		return sdkauth.Identity{}, err
	}

	if err := c.Bind(entry.DN, password); err != nil {
		if goldap.IsErrorWithCode(err, goldap.LDAPResultInvalidCredentials) {
			return sdkauth.Identity{}, sdkauth.ErrInvalidCredentials
		}

		return sdkauth.Identity{}, fmt.Errorf("%w: ldap user bind: %v", sdkauth.ErrUnavailable, err)
	}

	mappedUsername := strings.TrimSpace(entry.GetAttributeValue(p.usernameAttribute))
	if mappedUsername == "" {
		mappedUsername = username
	}

	return sdkauth.Identity{
		Provider:    providerName,
		Subject:     entry.DN,
		Username:    mappedUsername,
		DisplayName: strings.TrimSpace(entry.GetAttributeValue(p.displayNameAttribute)),
		Attributes: map[string][]string{
			p.usernameAttribute: append([]string(nil), entry.GetAttributeValues(p.usernameAttribute)...),
		},
	}, nil
}

func (p *Provider) searchUser(ctx context.Context, c conn, username string) (*goldap.Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	filter := strings.ReplaceAll(p.userFilter, "{username}", goldap.EscapeFilter(username))
	attributes := []string{p.usernameAttribute}
	if p.displayNameAttribute != "" && p.displayNameAttribute != p.usernameAttribute {
		attributes = append(attributes, p.displayNameAttribute)
	}

	result, err := c.Search(goldap.NewSearchRequest(
		p.baseDN,
		goldap.ScopeWholeSubtree,
		goldap.NeverDerefAliases,
		2,
		int(p.timeout/time.Second),
		false,
		filter,
		attributes,
		nil,
	))

	if err != nil {
		return nil, fmt.Errorf("%w: ldap user search: %v", sdkauth.ErrUnavailable, err)
	}

	switch len(result.Entries) {
	case 0:
		return nil, sdkauth.ErrInvalidCredentials
	case 1:
		return result.Entries[0], nil
	default:
		return nil, fmt.Errorf("%w: ldap user search returned multiple entries", sdkauth.ErrIdentityNotAllowed)
	}
}

func (p *Provider) dialLDAP(ctx context.Context) (conn, error) {
	dialer := &net.Dialer{Timeout: p.timeout}
	if deadline, ok := ctx.Deadline(); ok {
		dialer.Deadline = deadline
	}

	opts := []goldap.DialOpt{
		goldap.DialWithDialer(dialer),
		goldap.DialWithTLSConfig(p.effectiveTLSConfig()),
	}

	return goldap.DialURL(p.url, opts...)
}

func (p *Provider) effectiveTLSConfig() *tls.Config {
	if p.tlsConfig != nil {
		return p.tlsConfig
	}

	return &tls.Config{ServerName: ldapServerName(p.url), MinVersion: tls.VersionTLS12}
}

func ldapServerName(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}

	host := u.Hostname()
	if ip := net.ParseIP(host); ip != nil {
		return ""
	}

	return host
}
