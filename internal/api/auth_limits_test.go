package api

import "testing"

func TestAuthLimits_constants(t *testing.T) {
	if maxSetupCompleteBodyBytes <= 0 || maxBearerTokenBytes <= 0 {
		t.Fatal("limits must be positive")
	}

	if adminUsernameMaxLen < adminUsernameMinLen {
		t.Fatal("username max must be >= min length")
	}

	if adminPasswordMaxLen < adminPasswordMinLen {
		t.Fatal("password max must be >= min length")
	}
}
