package ldap

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	goldap "github.com/go-ldap/ldap/v3"
)

func TestRunSmokeAuthenticatesAndRejectsWrongPassword(t *testing.T) {
	fake := &fakeConn{
		searchResult: &goldap.SearchResult{Entries: []*goldap.Entry{
			goldap.NewEntry("uid=alice,ou=people,dc=example,dc=org", map[string][]string{
				"uid":       {"alice"},
				"cn":        {"Alice Example"},
				"entryUUID": {"uuid-123"},
			}),
		}},
		validUserPassword: "alice-secret",
	}

	var out bytes.Buffer
	result, err := RunSmoke(context.Background(), SmokeOptions{
		URL:                 "ldap://ldap.example.org:389",
		BindDN:              "cn=svc,dc=example,dc=org",
		BindPassword:        "svc-secret",
		BaseDN:              "ou=people,dc=example,dc=org",
		SubjectAttribute:    "entryUUID",
		Username:            "alice",
		Password:            "alice-secret",
		WrongPassword:       "wrong-secret",
		ExpectedSubject:     "uuid-123",
		ExpectedUsername:    "alice",
		ExpectedDisplayName: "Alice Example",
		Stdout:              &out,
		dial: func(context.Context) (conn, error) {
			return fake, nil
		},
	})

	if err != nil {
		t.Fatalf("RunSmoke: %v", err)
	}

	if result.Status != "ok" ||
		result.URL != "ldap://ldap.example.org:389" ||
		result.Username != "alice" ||
		result.Subject != "uuid-123" ||
		result.DisplayName != "Alice Example" ||
		!result.WrongPasswordDenied {
		t.Fatalf("unexpected result: %+v", result)
	}

	if len(fake.binds) != 4 {
		t.Fatalf("binds = %+v, want service+user binds for success and wrong-password checks", fake.binds)
	}
}

func TestRunSmokeRequiresURL(t *testing.T) {
	_, err := RunSmoke(context.Background(), SmokeOptions{URL: " ", BaseDN: "dc=example,dc=org", Username: "alice", Password: "secret"})
	if err == nil || !strings.Contains(err.Error(), "url is required") {
		t.Fatalf("RunSmoke error = %v, want url required", err)
	}
}

func TestLDAPSmokeMakefileRecreatesLocalFixture(t *testing.T) {
	b, err := os.ReadFile("../../../Makefile")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	section := ldapTextBetween(t, text, ".PHONY: ldap-smoke-up", ".PHONY: s3-smoke-public-up")
	for _, want := range []string{
		"LDAP_SMOKE_IMAGE ?= docker.io/osixia/openldap:1.5.0",
		"$(CONTAINER_CMD) stop \"$(LDAP_SMOKE_CONTAINER)\"",
		"$(CONTAINER_CMD) rm \"$(LDAP_SMOKE_CONTAINER)\"",
		"-v \"$(LDAP_SMOKE_BOOTSTRAP_DIR)\":/container/service/slapd/assets/config/bootstrap/ldif/custom:ro",
		"ldap-smoke: ldap-smoke-up ldap-smoke-check ldap-api-smoke-check",
	} {
		if !strings.Contains(section, want) && !strings.Contains(text, want) {
			t.Fatalf("LDAP smoke Makefile contract missing %q", want)
		}
	}

	if strings.Contains(section, "$(CONTAINER_CMD) start \"$(LDAP_SMOKE_CONTAINER)\"") {
		t.Fatal("ldap-smoke-up must recreate the local fixture instead of restarting stale directory state")
	}
}

func ldapTextBetween(t *testing.T, text, start, end string) string {
	t.Helper()

	startIndex := strings.Index(text, start)
	if startIndex < 0 {
		t.Fatalf("missing start marker %q", start)
	}

	endIndex := strings.Index(text[startIndex:], end)
	if endIndex < 0 {
		t.Fatalf("missing end marker %q", end)
	}

	return text[startIndex : startIndex+endIndex]
}
