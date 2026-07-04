package api

import (
	"os"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestMain(m *testing.M) {
	_ = os.Setenv("VECTIS_API_ALLOWED_HOSTS", "example.com,localhost,127.0.0.1,::1,example.test,vectis.example")
	_ = os.Setenv("VECTIS_API_AUTH_ENABLED", "false")
	passwordHashCost = bcrypt.MinCost
	if hash, err := bcrypt.GenerateFromPassword([]byte("dummy"), bcrypt.MinCost); err == nil {
		dummyBcryptHash = string(hash)
	}

	os.Exit(m.Run())
}
