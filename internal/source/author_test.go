package source

import (
	"context"
	"errors"
	"testing"

	"vectis/internal/dal"
)

func TestNewDefinitionAuthorFromRecordModes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		rec  dal.SourceRepositoryRecord
		err  error
	}{
		{
			name: "default read only",
			rec:  dal.SourceRepositoryRecord{},
			err:  ErrAuthoringUnavailable,
		},
		{
			name: "explicit read only",
			rec:  dal.SourceRepositoryRecord{AuthoringMode: dal.SourceAuthoringModeReadOnly},
			err:  ErrAuthoringUnavailable,
		},
		{
			name: "external change request not configured",
			rec:  dal.SourceRepositoryRecord{AuthoringMode: dal.SourceAuthoringModeExternalChangeRequest},
			err:  ErrAuthoringUnavailable,
		},
		{
			name: "local commit requires managed checkout",
			rec: dal.SourceRepositoryRecord{
				AuthoringMode: dal.SourceAuthoringModeLocalCommit,
				CheckoutMode:  dal.SourceCheckoutModeExternal,
			},
			err: ErrAuthoringUnavailable,
		},
		{
			name: "unsupported mode",
			rec:  dal.SourceRepositoryRecord{AuthoringMode: "magic"},
			err:  ErrInvalidReference,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewDefinitionAuthorFromRecord(tc.rec)
			if !errors.Is(err, tc.err) {
				t.Fatalf("expected %v, got %v", tc.err, err)
			}
		})
	}

	author, err := NewDefinitionAuthorFromRecord(dal.SourceRepositoryRecord{
		AuthoringMode: dal.SourceAuthoringModeLocalCommit,
		CheckoutMode:  dal.SourceCheckoutModeManaged,
		CheckoutPath:  "/work/repo",
		DefaultRef:    "main",
	})

	if err != nil {
		t.Fatalf("NewDefinitionAuthorFromRecord local commit: %v", err)
	}

	local, ok := author.(LocalCommitDefinitionAuthor)
	if !ok {
		t.Fatalf("expected LocalCommitDefinitionAuthor, got %T", author)
	}

	if local.Checkout == nil || local.DefaultRef != "main" {
		t.Fatalf("local author mismatch: %+v", local)
	}
}

func TestAuthoringCapabilityFromRecord(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		rec  dal.SourceRepositoryRecord
		want AuthoringCapability
	}{
		{
			name: "default read only",
			rec:  dal.SourceRepositoryRecord{Enabled: true},
			want: AuthoringCapability{
				Mode:   dal.SourceAuthoringModeReadOnly,
				Reason: "read_only",
			},
		},
		{
			name: "disabled",
			rec: dal.SourceRepositoryRecord{
				AuthoringMode: dal.SourceAuthoringModeLocalCommit,
				CheckoutMode:  dal.SourceCheckoutModeManaged,
			},
			want: AuthoringCapability{
				Mode:   dal.SourceAuthoringModeLocalCommit,
				Reason: "source_repository_disabled",
			},
		},
		{
			name: "local commit",
			rec: dal.SourceRepositoryRecord{
				AuthoringMode: dal.SourceAuthoringModeLocalCommit,
				CheckoutMode:  dal.SourceCheckoutModeManaged,
				Enabled:       true,
			},
			want: AuthoringCapability{
				Mode:             dal.SourceAuthoringModeLocalCommit,
				WriteDefinitions: true,
				LocalCommits:     true,
			},
		},
		{
			name: "external change request not configured",
			rec: dal.SourceRepositoryRecord{
				AuthoringMode: dal.SourceAuthoringModeExternalChangeRequest,
				CheckoutMode:  dal.SourceCheckoutModeExternal,
				Enabled:       true,
			},
			want: AuthoringCapability{
				Mode:   dal.SourceAuthoringModeExternalChangeRequest,
				Reason: "external_change_request_not_configured",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := AuthoringCapabilityFromRecord(tc.rec); got != tc.want {
				t.Fatalf("AuthoringCapabilityFromRecord() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestLocalCommitDefinitionAuthorUsesDefaultRef(t *testing.T) {
	repo := initGitRepo(t)
	writeAndCommit(t, repo, "README.md", "hello\n", "readme")
	branch := gitOutput(t, repo, "branch", "--show-current")

	author := LocalCommitDefinitionAuthor{
		Checkout:   NewGitCheckout(repo),
		DefaultRef: branch,
	}

	written, err := author.WriteDefinition(context.Background(), WriteDefinitionRequest{
		Path:           ".vectis/jobs/build.json",
		DefinitionJSON: `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`,
		Message:        "add build",
		ExpectedHead:   gitOutput(t, repo, "rev-parse", "HEAD"),
	})

	if err != nil {
		t.Fatalf("WriteDefinition: %v", err)
	}

	if written.RequestedRef != branch || written.Commit == "" || written.Path != ".vectis/jobs/build.json" || written.BlobSHA == "" {
		t.Fatalf("written definition mismatch: %+v", written)
	}

	deleted, err := author.DeleteDefinition(context.Background(), DeleteDefinitionRequest{
		Path:         ".vectis/jobs/build.json",
		Message:      "delete build",
		ExpectedHead: written.Commit,
	})

	if err != nil {
		t.Fatalf("DeleteDefinition: %v", err)
	}

	if deleted.RequestedRef != branch || deleted.Commit == "" || deleted.Commit == written.Commit || deleted.ParentCommit != written.Commit || deleted.Path != ".vectis/jobs/build.json" || deleted.BlobSHA != "" {
		t.Fatalf("deleted definition mismatch: %+v written=%+v", deleted, written)
	}
}
