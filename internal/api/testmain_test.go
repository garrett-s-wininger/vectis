package api

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	_ = os.Setenv("VECTIS_API_ALLOWED_HOSTS", "example.com,localhost,127.0.0.1,::1,example.test,vectis.example")
	os.Exit(m.Run())
}
