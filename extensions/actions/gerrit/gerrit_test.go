package gerrit

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	sdkscm "vectis/sdk/scm"
)

func TestPostReviewPostsBasicAuthPayloadAndEscapedPath(t *testing.T) {
	var gotPath string
	var gotAuthUser string
	var gotAuthPassword string
	var gotPayload map[string]any

	client := Client{
		BaseURL:  "http://gerrit.example.com/r/",
		Username: "ci-bot",
		Password: "secret-pass",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotPath = r.URL.EscapedPath()
			gotAuthUser, gotAuthPassword, _ = r.BasicAuth()

			if r.Method != http.MethodPost {
				t.Errorf("method = %s, want POST", r.Method)
			}

			if r.Header.Get("Content-Type") != "application/json; charset=UTF-8" {
				t.Errorf("content-type = %q", r.Header.Get("Content-Type"))
			}

			if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
				t.Errorf("decode body: %v", err)
			}

			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(")]}'\n{\"labels\":{\"Verified\":1}}")),
				Request:    r,
			}, nil
		})},
	}

	err := client.PostReview(context.Background(), ReviewRequest{
		Change:    "project~feature/x~Iabc123",
		Revision:  DefaultReviewRevision,
		Message:   "Vectis run succeeded",
		Label:     "Verified",
		LabelVote: 1,
	})

	if err != nil {
		t.Fatalf("PostReview: %v", err)
	}

	if gotPath != "/r/a/changes/project~feature%2Fx~Iabc123/revisions/current/review" {
		t.Fatalf("path = %q", gotPath)
	}

	if gotAuthUser != "ci-bot" || gotAuthPassword != "secret-pass" {
		t.Fatalf("basic auth = %q/%q", gotAuthUser, gotAuthPassword)
	}

	if gotPayload["message"] != "Vectis run succeeded" {
		t.Fatalf("message payload = %#v", gotPayload["message"])
	}

	if gotPayload["tag"] != DefaultReviewTag {
		t.Fatalf("tag payload = %#v", gotPayload["tag"])
	}

	labels, ok := gotPayload["labels"].(map[string]any)
	if !ok {
		t.Fatalf("labels payload = %#v", gotPayload["labels"])
	}

	if labels["Verified"] != float64(1) {
		t.Fatalf("Verified label = %#v", labels["Verified"])
	}
}

func TestPostReviewFailsWhenLabelIsNotConfirmed(t *testing.T) {
	client := Client{
		BaseURL: "http://gerrit.example.com",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(strings.NewReader(")]}'\n{\"labels\":{}}")),
				Request:    r,
			}, nil
		})},
	}

	err := client.PostReview(context.Background(), ReviewRequest{
		Change:    "1",
		Message:   "review",
		Label:     "Verified",
		LabelVote: 1,
	})

	if err == nil || !strings.Contains(err.Error(), `did not confirm label "Verified"`) {
		t.Fatalf("PostReview error = %v, want missing label confirmation", err)
	}
}

func TestPostReviewReportsHTTPFailureWithoutXSSIPrefix(t *testing.T) {
	client := Client{
		BaseURL: "http://gerrit.example.com",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Status:     "403 Forbidden",
				Body:       io.NopCloser(strings.NewReader(")]}'\npermission denied")),
				Request:    r,
			}, nil
		})},
	}

	err := client.PostReview(context.Background(), ReviewRequest{
		Change:  "1",
		Message: "review",
	})

	if err == nil || !strings.Contains(err.Error(), "permission denied") || strings.Contains(err.Error(), ")]}'") {
		t.Fatalf("PostReview error = %v, want sanitized Gerrit error", err)
	}
}

func TestQueryChangesUsesGerritQueryAndDecodesCurrentRevision(t *testing.T) {
	var gotPath string
	var gotAuthUser string
	var gotAuthPassword string
	var gotQuery url.Values

	client := Client{
		BaseURL:  "http://gerrit.example.com/gerrit/",
		Username: "admin",
		Password: "secret-pass",
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotPath = r.URL.EscapedPath()
			gotQuery = r.URL.Query()
			gotAuthUser, gotAuthPassword, _ = r.BasicAuth()

			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(strings.NewReader(")]}'\n[{\"id\":\"project~master~Iabc\",\"project\":\"project\",\"branch\":\"master\",\"status\":\"NEW\",\"change_id\":\"Iabc\",\"current_revision\":\"abc\",\"revisions\":{\"abc\":{\"ref\":\"refs/changes/01/1/1\"}}}]")),
				Request:    r,
			}, nil
		})},
	}

	changes, err := client.QueryChanges(context.Background(), sdkscm.Query{
		Project:  "project",
		Branch:   "master",
		Status:   "open",
		ChangeID: "Iabc",
	})

	if err != nil {
		t.Fatalf("QueryChanges: %v", err)
	}

	if gotPath != "/gerrit/a/changes/" {
		t.Fatalf("path = %q", gotPath)
	}

	if gotQuery.Get("q") != "project:project branch:master status:open change:Iabc" {
		t.Fatalf("q = %q", gotQuery.Get("q"))
	}

	if gotQuery.Get("o") != "CURRENT_REVISION" {
		t.Fatalf("o = %q", gotQuery.Get("o"))
	}

	if gotAuthUser != "admin" || gotAuthPassword != "secret-pass" {
		t.Fatalf("basic auth = %q/%q", gotAuthUser, gotAuthPassword)
	}

	info, ok := sdkscm.FindChange(changes, "Iabc")
	if !ok {
		t.Fatalf("FindChange returned false for %+v", changes)
	}

	revision, ref, err := info.CurrentRevisionRef()
	if err != nil {
		t.Fatalf("CurrentRevisionRef: %v", err)
	}

	if revision != "abc" || ref != "refs/changes/01/1/1" {
		t.Fatalf("revision/ref = %q/%q", revision, ref)
	}
}

func TestDecodeJSONStripsXSSIPrefix(t *testing.T) {
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(")]}'\n{\"current_revision\":\"abc\",\"revisions\":{\"abc\":{\"ref\":\"refs/changes/01/1/1\"}}}")),
	}

	var info sdkscm.Change
	if err := DecodeJSON(resp, &info); err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}

	revision, ref, err := info.CurrentRevisionRef()
	if err != nil {
		t.Fatalf("CurrentRevisionRef: %v", err)
	}

	if revision != "abc" || ref != "refs/changes/01/1/1" {
		t.Fatalf("revision/ref = %q/%q", revision, ref)
	}
}

func TestParseLabelValue(t *testing.T) {
	got, err := ParseLabelValue("+1")
	if err != nil {
		t.Fatalf("ParseLabelValue: %v", err)
	}

	if got != 1 {
		t.Fatalf("value = %d, want 1", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
