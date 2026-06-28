package ldap

import (
	"context"
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
