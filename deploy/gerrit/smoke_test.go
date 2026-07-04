package gerrit

import (
	"strings"
	"testing"
)

func TestNormalizeSmokeOptionsGeneratesProject(t *testing.T) {
	opts := normalizeSmokeOptions(SmokeOptions{
		URL:           "http://gerrit.example/",
		ProjectPrefix: " vectis smoke ",
	})

	if opts.URL != "http://gerrit.example" {
		t.Fatalf("URL = %q", opts.URL)
	}

	if !strings.HasPrefix(opts.Project, "vectis-smoke-") {
		t.Fatalf("Project = %q, want generated prefix", opts.Project)
	}
}
