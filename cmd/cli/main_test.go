package main

import (
	"net/http"
	"testing"
)

func TestNewAPIRequest_NoToken(t *testing.T) {
	t.Setenv("VECTIS_API_TOKEN", "")

	req, err := newAPIRequest(http.MethodGet, "/api/v1/jobs", nil)
	if err != nil {
		t.Fatal(err)
	}

	if got := req.Header.Get("Authorization"); got != "" {
		t.Fatalf("Authorization=%q", got)
	}
}

func TestNewAPIRequest_WithToken(t *testing.T) {
	t.Setenv("VECTIS_API_TOKEN", "secret-token")

	req, err := newAPIRequest(http.MethodGet, "/api/v1/jobs", nil)
	if err != nil {
		t.Fatal(err)
	}

	if got := req.Header.Get("Authorization"); got != "Bearer secret-token" {
		t.Fatalf("Authorization=%q", got)
	}
}

func TestNewAPIRequest_PreservesOtherHeaders(t *testing.T) {
	t.Setenv("VECTIS_API_TOKEN", "secret-token")

	req, err := newAPIRequest(http.MethodPost, "/api/v1/jobs", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	if got := req.Header.Get("Authorization"); got != "Bearer secret-token" {
		t.Fatalf("Authorization=%q", got)
	}
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type=%q", got)
	}
}
