package refspec

import "testing"

func TestNormalizeFetchRefspecs(t *testing.T) {
	got, err := NormalizeFetchRefspecs([]string{
		" +refs/heads/*:refs/heads/* ",
		"+refs/heads/*:refs/heads/*",
		"refs/notes/*:refs/notes/*",
	})

	if err != nil {
		t.Fatalf("NormalizeFetchRefspecs: %v", err)
	}

	want := []string{"+refs/heads/*:refs/heads/*", "refs/notes/*:refs/notes/*"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d: got=%v", len(got), len(want), got)
	}

	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("refspec[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestNormalizeFetchRefspecRejectsUnsafeRefs(t *testing.T) {
	for _, raw := range []string{
		"-c core.fsmonitor=true",
		"refs/heads/*",
		"refs/heads/*:refs/cache/heads/main",
		"refs/heads/main:../main",
		"refs/heads/main:refs/heads/main.lock",
		"refs/heads/main:refs/heads/main\nrefs/tags/v1:refs/tags/v1",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := NormalizeFetchRefspec(raw); err == nil {
				t.Fatalf("NormalizeFetchRefspec(%q) succeeded, want error", raw)
			}
		})
	}
}
