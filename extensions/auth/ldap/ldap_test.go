package ldap

import (
	"context"
	"crypto/tls"
	"errors"
	"reflect"
	"testing"
	"time"

	goldap "github.com/go-ldap/ldap/v3"

	sdkauth "vectis/sdk/auth"
)

func TestProviderAuthenticate(t *testing.T) {
	fake := &fakeConn{
		searchResult: &goldap.SearchResult{Entries: []*goldap.Entry{
			goldap.NewEntry("uid=alice,ou=people,dc=example,dc=org", map[string][]string{
				"uid": {"alice"},
				"cn":  {"Alice Example"},
			}),
		}},
	}

	provider := newTestProvider(t, fake)
	identity, err := provider.Authenticate(context.Background(), "alice", "secret")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}

	if identity.Provider != providerName ||
		identity.Subject != "uid=alice,ou=people,dc=example,dc=org" ||
		identity.Username != "alice" ||
		identity.DisplayName != "Alice Example" {
		t.Fatalf("unexpected identity: %+v", identity)
	}

	if !reflect.DeepEqual(fake.binds, [][2]string{
		{"cn=svc,dc=example,dc=org", "svc-secret"},
		{"uid=alice,ou=people,dc=example,dc=org", "secret"},
	}) {
		t.Fatalf("binds = %+v", fake.binds)
	}

	if fake.searchRequest == nil {
		t.Fatal("missing search request")
	}

	if fake.searchRequest.BaseDN != "ou=people,dc=example,dc=org" {
		t.Fatalf("base dn = %q", fake.searchRequest.BaseDN)
	}

	if fake.searchRequest.Filter != "(uid=alice)" {
		t.Fatalf("filter = %q", fake.searchRequest.Filter)
	}
}

func TestProviderAuthenticateEscapesUsernameInFilter(t *testing.T) {
	fake := &fakeConn{searchResult: &goldap.SearchResult{Entries: []*goldap.Entry{
		goldap.NewEntry("uid=a,ou=people,dc=example,dc=org", map[string][]string{"uid": {"a"}}),
	}}}

	provider := newTestProvider(t, fake)

	if _, err := provider.Authenticate(context.Background(), `a*)(uid=*`, "secret"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}

	if fake.searchRequest.Filter != `(uid=a\2a\29\28uid=\2a)` {
		t.Fatalf("filter = %q", fake.searchRequest.Filter)
	}
}

func TestProviderAuthenticateInvalidCredentials(t *testing.T) {
	fake := &fakeConn{searchResult: &goldap.SearchResult{Entries: []*goldap.Entry{
		goldap.NewEntry("uid=alice,ou=people,dc=example,dc=org", map[string][]string{"uid": {"alice"}}),
	}}}

	fake.userBindErr = goldap.NewError(goldap.LDAPResultInvalidCredentials, errors.New("invalid"))
	provider := newTestProvider(t, fake)

	_, err := provider.Authenticate(context.Background(), "alice", "wrong")
	if !errors.Is(err, sdkauth.ErrInvalidCredentials) {
		t.Fatalf("Authenticate error = %v, want invalid credentials", err)
	}
}

func TestProviderAuthenticateSearchResultCounts(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		provider := newTestProvider(t, &fakeConn{searchResult: &goldap.SearchResult{}})
		_, err := provider.Authenticate(context.Background(), "alice", "secret")
		if !errors.Is(err, sdkauth.ErrInvalidCredentials) {
			t.Fatalf("Authenticate error = %v, want invalid credentials", err)
		}
	})

	t.Run("multiple", func(t *testing.T) {
		provider := newTestProvider(t, &fakeConn{searchResult: &goldap.SearchResult{Entries: []*goldap.Entry{
			goldap.NewEntry("uid=alice,ou=people,dc=example,dc=org", nil),
			goldap.NewEntry("uid=alice,ou=contractors,dc=example,dc=org", nil),
		}}})

		_, err := provider.Authenticate(context.Background(), "alice", "secret")
		if !errors.Is(err, sdkauth.ErrIdentityNotAllowed) {
			t.Fatalf("Authenticate error = %v, want identity not allowed", err)
		}
	})
}

func TestNewProviderValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts ProviderOptions
	}{
		{name: "url", opts: ProviderOptions{BaseDN: "dc=example,dc=org"}},
		{name: "base", opts: ProviderOptions{URL: "ldap://ldap.example.org"}},
		{name: "filter placeholder", opts: ProviderOptions{URL: "ldap://ldap.example.org", BaseDN: "dc=example,dc=org", UserFilter: "(uid=*)"}},
		{name: "bind password without dn", opts: ProviderOptions{URL: "ldap://ldap.example.org", BaseDN: "dc=example,dc=org", BindPassword: "secret"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewProvider(tc.opts); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func newTestProvider(t *testing.T, c *fakeConn) *Provider {
	t.Helper()

	provider, err := NewProvider(ProviderOptions{
		URL:               "ldap://ldap.example.org:389",
		BindDN:            "cn=svc,dc=example,dc=org",
		BindPassword:      "svc-secret",
		BaseDN:            "ou=people,dc=example,dc=org",
		UserFilter:        "(uid={username})",
		UsernameAttribute: "uid",
		Timeout:           time.Second,
		Dial: func(context.Context) (conn, error) {
			return c, nil
		},
	})

	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	return provider
}

type fakeConn struct {
	binds             [][2]string
	searchRequest     *goldap.SearchRequest
	searchResult      *goldap.SearchResult
	userBindErr       error
	validUserPassword string
}

func (f *fakeConn) Bind(username, password string) error {
	f.binds = append(f.binds, [2]string{username, password})
	if username != "cn=svc,dc=example,dc=org" {
		if f.validUserPassword != "" && password != f.validUserPassword {
			return goldap.NewError(goldap.LDAPResultInvalidCredentials, errors.New("invalid"))
		}

		return f.userBindErr
	}

	return nil
}

func (f *fakeConn) Search(req *goldap.SearchRequest) (*goldap.SearchResult, error) {
	f.searchRequest = req
	return f.searchResult, nil
}

func (f *fakeConn) StartTLS(*tls.Config) error {
	return nil
}

func (f *fakeConn) Close() error {
	return nil
}
