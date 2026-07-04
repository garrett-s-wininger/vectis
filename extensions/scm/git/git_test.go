package git

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"vectis/sdk/scm"
)

func TestProviderPollBootstrapsCursorWithoutEvents(t *testing.T) {
	runner := &fakeRunner{
		refs: []remoteRef{{Name: "refs/heads/main", SHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
	}

	provider := newProviderWithRunner(runner)
	result, err := provider.Poll(context.Background(), scm.PollSpec{
		Provider: "git",
		BaseURL:  "https://git.example.com",
		Project:  "team/repo.git",
		Branch:   "main",
	})

	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if len(result.Events) != 0 {
		t.Fatalf("events = %+v, want none during cursor bootstrap", result.Events)
	}

	assertCursorRefs(t, result.Cursor, map[string]string{
		"refs/heads/main": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})

	if runner.remote != "https://git.example.com/team/repo.git" {
		t.Fatalf("remote = %q", runner.remote)
	}

	if !reflect.DeepEqual(runner.patterns, []string{"refs/heads/main"}) {
		t.Fatalf("patterns = %+v", runner.patterns)
	}
}

func TestProviderPollEmitsChangedAndNewRefs(t *testing.T) {
	cursor, err := encodeCursor(map[string]string{
		"refs/heads/main": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})

	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	runner := &fakeRunner{
		refs: []remoteRef{
			{Name: "refs/heads/feature", SHA: "cccccccccccccccccccccccccccccccccccccccc"},
			{Name: "refs/heads/main", SHA: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		},
	}

	provider := newProviderWithRunner(runner)
	result, err := provider.Poll(context.Background(), scm.PollSpec{
		Provider: "git",
		Project:  "file:///tmp/repo.git",
		Cursor:   cursor,
	})

	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if len(result.Events) != 2 {
		t.Fatalf("events = %+v, want changed main and new feature", result.Events)
	}

	if !strings.Contains(result.Events[0].Key, ":refs/heads/feature:cccc") ||
		!strings.Contains(result.Events[1].Key, ":refs/heads/main:bbbb") {
		t.Fatalf("event order/keys = %+v", result.Events)
	}

	var payload eventPayload
	if err := json.Unmarshal([]byte(result.Events[1].PayloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if payload.Provider != "git" || payload.Ref != "refs/heads/main" || payload.Branch != "main" ||
		payload.SHA != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" ||
		payload.PreviousSHA != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("payload = %+v", payload)
	}

	if payload.Project != "" {
		t.Fatalf("payload project = %q, want empty when project is the remote", payload.Project)
	}

	assertCursorRefs(t, result.Cursor, map[string]string{
		"refs/heads/feature": "cccccccccccccccccccccccccccccccccccccccc",
		"refs/heads/main":    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	})
}

func TestProviderPollSkipsUnchangedRefs(t *testing.T) {
	cursor, err := encodeCursor(map[string]string{
		"refs/heads/main": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})

	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	provider := newProviderWithRunner(&fakeRunner{
		refs: []remoteRef{{Name: "refs/heads/main", SHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
	})

	result, err := provider.Poll(context.Background(), scm.PollSpec{
		Project: "file:///tmp/repo.git",
		Cursor:  cursor,
	})

	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if len(result.Events) != 0 {
		t.Fatalf("events = %+v, want unchanged ref skipped", result.Events)
	}
}

func TestProviderPollNormalizesQueryPatterns(t *testing.T) {
	runner := &fakeRunner{}
	provider := newProviderWithRunner(runner)

	if _, err := provider.Poll(context.Background(), scm.PollSpec{
		Project: "file:///tmp/repo.git",
		Branch:  "main",
		Query:   "refs/tags/v* heads/release main",
	}); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	want := []string{"refs/heads/main", "refs/tags/v*", "refs/heads/release"}
	if !reflect.DeepEqual(runner.patterns, want) {
		t.Fatalf("patterns = %+v, want %+v", runner.patterns, want)
	}
}

func TestProviderPollRejectsInvalidCursor(t *testing.T) {
	provider := newProviderWithRunner(&fakeRunner{})
	_, err := provider.Poll(context.Background(), scm.PollSpec{
		Project: "file:///tmp/repo.git",
		Cursor:  "not-json",
	})

	if err == nil || !strings.Contains(err.Error(), "decode git cursor") {
		t.Fatalf("Poll error = %v, want decode git cursor", err)
	}
}

func TestProviderPollsLocalGitRemote(t *testing.T) {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git binary is not available")
	}

	root := t.TempDir()
	remote := filepath.Join(root, "remote.git")
	work := filepath.Join(root, "work")

	runGit(t, gitPath, "", "init", "--bare", remote)
	runGit(t, gitPath, "", "-c", "init.defaultBranch=main", "init", work)
	runGit(t, gitPath, work, "config", "user.email", "vectis@example.invalid")
	runGit(t, gitPath, work, "config", "user.name", "Vectis Test")
	runGit(t, gitPath, work, "config", "commit.gpgsign", "false")

	if err := os.WriteFile(filepath.Join(work, "README.md"), []byte("one\n"), 0o600); err != nil {
		t.Fatalf("write README: %v", err)
	}

	runGit(t, gitPath, work, "add", "README.md")
	runGit(t, gitPath, work, "commit", "-m", "initial")
	runGit(t, gitPath, work, "remote", "add", "origin", remote)
	runGit(t, gitPath, work, "push", "-u", "origin", "main")

	provider := NewProvider()
	first, err := provider.Poll(context.Background(), scm.PollSpec{
		Project: remote,
		Branch:  "main",
	})

	if err != nil {
		t.Fatalf("first Poll: %v", err)
	}

	if len(first.Events) != 0 {
		t.Fatalf("first poll events = %+v, want bootstrap only", first.Events)
	}

	if err := os.WriteFile(filepath.Join(work, "README.md"), []byte("two\n"), 0o600); err != nil {
		t.Fatalf("update README: %v", err)
	}

	runGit(t, gitPath, work, "add", "README.md")
	runGit(t, gitPath, work, "commit", "-m", "update")
	runGit(t, gitPath, work, "push", "origin", "main")

	second, err := provider.Poll(context.Background(), scm.PollSpec{
		Project: remote,
		Branch:  "main",
		Cursor:  first.Cursor,
	})

	if err != nil {
		t.Fatalf("second Poll: %v", err)
	}

	if len(second.Events) != 1 || !strings.Contains(second.Events[0].Key, ":refs/heads/main:") {
		t.Fatalf("second poll events = %+v, want one main update", second.Events)
	}
}

func TestParseRemoteRefsRejectsMalformedOutput(t *testing.T) {
	_, err := parseRemoteRefs("abc only extra\n")
	if err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("parseRemoteRefs error = %v, want line error", err)
	}
}

func assertCursorRefs(t *testing.T, raw string, want map[string]string) {
	t.Helper()

	var cur cursor
	if err := json.Unmarshal([]byte(raw), &cur); err != nil {
		t.Fatalf("decode cursor %q: %v", raw, err)
	}

	if cur.Version != 1 {
		t.Fatalf("cursor version = %d, want 1", cur.Version)
	}

	if !reflect.DeepEqual(cur.Refs, want) {
		t.Fatalf("cursor refs = %+v, want %+v", cur.Refs, want)
	}
}

type fakeRunner struct {
	remote   string
	patterns []string
	refs     []remoteRef
	err      error
}

func (r *fakeRunner) ListRemote(_ context.Context, remote string, patterns []string) ([]remoteRef, error) {
	r.remote = remote
	r.patterns = append([]string(nil), patterns...)
	return append([]remoteRef(nil), r.refs...), r.err
}

func runGit(t *testing.T, gitPath, dir string, args ...string) {
	t.Helper()

	cmd := exec.Command(gitPath, args...)
	if dir != "" {
		cmd.Dir = dir
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, string(out))
	}
}
