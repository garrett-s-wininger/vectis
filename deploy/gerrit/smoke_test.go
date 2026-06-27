package gerrit

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestDecodeGerritJSONStripsXSSIPrefix(t *testing.T) {
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(")]}'\n{\"current_revision\":\"abc\",\"revisions\":{\"abc\":{\"ref\":\"refs/changes/01/1/1\"}}}")),
	}

	var info gerritChangeInfo
	if err := decodeGerritJSON(resp, &info); err != nil {
		t.Fatalf("decodeGerritJSON: %v", err)
	}

	revision, ref, err := info.currentRevision()
	if err != nil {
		t.Fatalf("currentRevision: %v", err)
	}

	if revision != "abc" || ref != "refs/changes/01/1/1" {
		t.Fatalf("revision/ref = %q/%q", revision, ref)
	}
}

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
