package httpsecurity

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestHasBody(t *testing.T) {
	tests := []struct {
		name string
		req  func(t *testing.T) *http.Request
		want bool
	}{
		{
			name: "no body",
			req: func(t *testing.T) *http.Request {
				t.Helper()
				return httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			},
			want: false,
		},
		{
			name: "known body length",
			req: func(t *testing.T) *http.Request {
				t.Helper()
				return httptest.NewRequest(http.MethodGet, "/", strings.NewReader("body"))
			},
			want: true,
		},
		{
			name: "chunked transfer encoding",
			req: func(t *testing.T) *http.Request {
				t.Helper()
				req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
				req.TransferEncoding = []string{"chunked"}
				return req
			},
			want: true,
		},
		{
			name: "unknown body length",
			req: func(t *testing.T) *http.Request {
				t.Helper()
				req := httptest.NewRequest(http.MethodGet, "/", strings.NewReader("body"))
				req.ContentLength = -1
				return req
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RequestHasBody(tt.req(t)); got != tt.want {
				t.Fatalf("RequestHasBody()=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestRequestContentTypeIsJSON(t *testing.T) {
	tests := []struct {
		contentType string
		want        bool
	}{
		{contentType: "application/json", want: true},
		{contentType: "application/json; charset=utf-8", want: true},
		{contentType: "Application/JSON", want: true},
		{contentType: "text/plain", want: false},
		{contentType: "", want: false},
		{contentType: "not a media type", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.contentType, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
			req.Header.Set("Content-Type", tt.contentType)
			if got := RequestContentTypeIsJSON(req); got != tt.want {
				t.Fatalf("RequestContentTypeIsJSON()=%v, want %v", got, tt.want)
			}
		})
	}
}
