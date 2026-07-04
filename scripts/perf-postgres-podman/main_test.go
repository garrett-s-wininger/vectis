package main

import (
	"net/url"
	"testing"
)

func TestPostgresDSNWithExtraParamsUnset(t *testing.T) {
	t.Setenv(envPostgresDSNParams, "")

	const dsn = "postgres://vectis:vectis@127.0.0.1:5432/vectis?sslmode=disable"
	got, err := postgresDSNWithExtraParams(dsn)
	if err != nil {
		t.Fatalf("postgresDSNWithExtraParams: %v", err)
	}
	if got != dsn {
		t.Fatalf("postgresDSNWithExtraParams = %q, want %q", got, dsn)
	}
}

func TestPostgresDSNWithExtraParamsMergesAndOverrides(t *testing.T) {
	t.Setenv(envPostgresDSNParams, "?sslmode=require&plan_cache_mode=force_generic_plan")

	got, err := postgresDSNWithExtraParams("postgres://vectis:vectis@127.0.0.1:5432/vectis?sslmode=disable")
	if err != nil {
		t.Fatalf("postgresDSNWithExtraParams: %v", err)
	}

	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse resulting dsn: %v", err)
	}
	query := parsed.Query()
	if query.Get("sslmode") != "require" {
		t.Fatalf("sslmode = %q, want require", query.Get("sslmode"))
	}
	if query.Get("plan_cache_mode") != "force_generic_plan" {
		t.Fatalf("plan_cache_mode = %q, want force_generic_plan", query.Get("plan_cache_mode"))
	}
}

func TestPostgresDSNWithExtraParamsRejectsMalformedParams(t *testing.T) {
	t.Setenv(envPostgresDSNParams, "plan_cache_mode=%zz")

	if _, err := postgresDSNWithExtraParams("postgres://vectis:vectis@127.0.0.1:5432/vectis?sslmode=disable"); err == nil {
		t.Fatal("postgresDSNWithExtraParams succeeded with malformed params")
	}
}
