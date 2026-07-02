package ldap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	ldapauth "vectis/extensions/auth/ldap"
	sdkauth "vectis/sdk/auth"
)

func TestRunAPISmokeWithInjectedProvider(t *testing.T) {
	result, err := RunAPISmoke(context.Background(), APISmokeOptions{
		LDAP: ldapauth.SmokeOptions{
			Username:      "alice",
			Password:      "alice-secret",
			WrongPassword: "wrong-secret",
		},
		loginProvider: fakeAPILoginProvider{
			username: "alice",
			password: "alice-secret",
		},
	})

	if err != nil {
		t.Fatalf("RunAPISmoke: %v", err)
	}

	if result.Status != "ok" ||
		result.Username != "alice" ||
		result.UserID == 0 ||
		!result.TokenReturned ||
		!result.AuthenticatedRequestOK ||
		!result.WrongPasswordDenied ||
		!result.SetupExternalIdentityLinked ||
		!result.PasswordLoginDenied ||
		!result.ExternalLoginMatchedSetup {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestRunAPISmokeAgainstDeployedAPI(t *testing.T) {
	setupComplete := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/setup/status":
			_ = json.NewEncoder(w).Encode(setupStatusResponse{SetupComplete: setupComplete, AuthEnabled: true})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/setup/complete":
			setupComplete = true
			_ = json.NewEncoder(w).Encode(setupCompleteResponse{
				APIToken:            "setup-token",
				Username:            DefaultAPISmokeAdminUsername,
				PasswordAuthEnabled: false,
				ExternalIdentity: &externalIdentityResponse{
					LocalUserID: 42,
					ProviderID:  ldapauth.DefaultProviderID,
					Subject:     "uid=alice,ou=people,dc=example,dc=org",
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/login":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode login: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			switch {
			case body["username"] == DefaultAPISmokeAdminUsername:
				w.WriteHeader(http.StatusUnauthorized)
			case body["username"] == "alice" && body["password"] == "alice-secret":
				_ = json.NewEncoder(w).Encode(loginResponse{Token: "login-token", UserID: 42})
			default:
				w.WriteHeader(http.StatusUnauthorized)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/version":
			if r.Header.Get("Authorization") != "Bearer login-token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			_, _ = w.Write([]byte(`{"version":"test"}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	result, err := RunAPISmoke(context.Background(), APISmokeOptions{
		APIURL: server.URL,
		LDAP: ldapauth.SmokeOptions{
			Username:      "alice",
			Password:      "alice-secret",
			WrongPassword: "wrong-secret",
		},
		loginProvider: fakeAPILoginProvider{
			username: "alice",
			password: "alice-secret",
		},
	})

	if err != nil {
		t.Fatalf("RunAPISmoke: %v", err)
	}

	if result.APIURL != server.URL ||
		!result.SetupPerformed ||
		result.SetupAlreadyComplete ||
		!result.SetupExternalIdentityLinked ||
		!result.PasswordLoginDenied ||
		!result.ExternalLoginMatchedSetup ||
		!result.WrongPasswordDenied ||
		!result.AuthenticatedRequestOK {
		t.Fatalf("unexpected result: %+v", result)
	}
}

type fakeAPILoginProvider struct {
	username string
	password string
}

func (p fakeAPILoginProvider) Authenticate(_ context.Context, username, password string) (sdkauth.Identity, error) {
	if username != p.username || password != p.password {
		return sdkauth.Identity{}, sdkauth.ErrInvalidCredentials
	}

	return sdkauth.Identity{
		Provider: "ldap",
		Subject:  "uid=alice,ou=people,dc=example,dc=org",
		Username: "alice",
	}, nil
}
