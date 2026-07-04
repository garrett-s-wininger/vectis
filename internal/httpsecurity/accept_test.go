package httpsecurity

import "testing"

func TestAcceptsAny(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		allowed []string
		want    bool
	}{
		{name: "empty accepts any", header: "", allowed: []string{MediaTypeJSON}, want: true},
		{name: "exact media type", header: "application/json", allowed: []string{MediaTypeJSON}, want: true},
		{name: "parameters ignored", header: "application/json; charset=utf-8", allowed: []string{MediaTypeJSON}, want: true},
		{name: "any wildcard", header: "*/*", allowed: []string{MediaTypeJSON}, want: true},
		{name: "type wildcard", header: "application/*", allowed: []string{MediaTypeJSON}, want: true},
		{name: "weighted list", header: "text/html;q=0.5, application/json;q=0.9", allowed: []string{MediaTypeJSON}, want: true},
		{name: "q zero rejects", header: "application/json;q=0", allowed: []string{MediaTypeJSON}, want: false},
		{name: "specific q zero overrides wildcard", header: "application/json;q=0, */*;q=0.1", allowed: []string{MediaTypeJSON}, want: false},
		{name: "case insensitive", header: "Application/JSON", allowed: []string{MediaTypeJSON}, want: true},
		{name: "incompatible", header: "text/html, application/xhtml+xml", allowed: []string{MediaTypeJSON}, want: false},
		{name: "invalid", header: "not a media type", allowed: []string{MediaTypeJSON}, want: false},
		{name: "quoted comma in parameter", header: "text/plain; note=\"a,b\", application/json", allowed: []string{MediaTypeJSON}, want: true},
		{name: "invalid allowed type", header: "application/json", allowed: []string{"bad media type"}, want: false},
		{name: "openmetrics accepted", header: "application/openmetrics-text; version=1.0.0", allowed: []string{MediaTypePlainText, MediaTypeOpenMetricsText}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AcceptsAny(tt.header, tt.allowed...); got != tt.want {
				t.Fatalf("AcceptsAny(%q, %v) = %v, want %v", tt.header, tt.allowed, got, tt.want)
			}
		})
	}
}
