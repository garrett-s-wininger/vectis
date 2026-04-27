package api

import (
	"net/http"
	"testing"
)

func TestParsePageParams_defaults(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	p := parsePageParams(req)
	if p.Cursor != 0 {
		t.Fatalf("cursor=%d, want 0", p.Cursor)
	}

	if p.Limit != defaultPageLimit {
		t.Fatalf("limit=%d, want %d", p.Limit, defaultPageLimit)
	}
}

func TestParsePageParams_custom(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/jobs?cursor=10&limit=25", nil)
	p := parsePageParams(req)
	if p.Cursor != 10 {
		t.Fatalf("cursor=%d, want 10", p.Cursor)
	}

	if p.Limit != 25 {
		t.Fatalf("limit=%d, want 25", p.Limit)
	}
}

func TestParsePageParams_maxLimit(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/jobs?limit=9999", nil)
	p := parsePageParams(req)
	if p.Limit != maxPageLimit {
		t.Fatalf("limit=%d, want %d", p.Limit, maxPageLimit)
	}
}

func TestParsePageParams_invalid(t *testing.T) {
	cases := []string{
		"cursor=-1&limit=-5",
		"cursor=abc&limit=xyz",
		"cursor=0&limit=0",
	}

	for _, q := range cases {
		req, _ := http.NewRequest(http.MethodGet, "/api/v1/jobs?"+q, nil)
		p := parsePageParams(req)
		if p.Cursor != 0 {
			t.Fatalf("cursor=%d, want 0 for %s", p.Cursor, q)
		}

		if p.Limit != defaultPageLimit {
			t.Fatalf("limit=%d, want %d for %s", p.Limit, defaultPageLimit, q)
		}
	}
}

func TestBuildPaginatedResponse_withNextCursor(t *testing.T) {
	resp := buildPaginatedResponse([]string{"a"}, 42)
	if resp.NextCursor == nil || *resp.NextCursor != 42 {
		t.Fatalf("next_cursor=%v, want 42", resp.NextCursor)
	}
}

func TestBuildPaginatedResponse_withoutNextCursor(t *testing.T) {
	resp := buildPaginatedResponse([]string{"a"}, 0)
	if resp.NextCursor != nil {
		t.Fatalf("next_cursor=%v, want nil", resp.NextCursor)
	}
}
