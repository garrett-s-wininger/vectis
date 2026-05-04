package validation_test

import (
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/job/validation"
)

func strp(s string) *string { return &s }

func validJob() *api.Job {
	return &api.Job{
		Id: strp("job-1"),
		Root: &api.Node{
			Id:   strp("root"),
			Uses: strp("builtins/sequence"),
			Steps: []*api.Node{{
				Id:   strp("shell"),
				Uses: strp("builtins/shell"),
				With: map[string]string{"command": "echo hi"},
			}},
		},
	}
}

func TestValidateJob_Valid(t *testing.T) {
	t.Parallel()

	if err := validation.ValidateJob(validJob(), validation.Options{RequireJobID: true}); err != nil {
		t.Fatalf("expected valid job: %v", err)
	}
}

func TestValidateJob_MissingJobIDWhenRequired(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Id = nil

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil || !strings.Contains(err.Error(), "id: is required") {
		t.Fatalf("expected missing id error, got %v", err)
	}
}

func TestValidateJob_AllowsMissingJobIDWhenNotRequired(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Id = nil

	if err := validation.ValidateJob(job, validation.Options{}); err != nil {
		t.Fatalf("expected generated-id job to validate: %v", err)
	}
}

func TestValidateJob_MissingRoot(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root = nil

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil || !strings.Contains(err.Error(), "root: is required") {
		t.Fatalf("expected missing root error, got %v", err)
	}
}

func TestValidateJob_NodeValidation(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps = append(job.Root.Steps, &api.Node{
		Id:   strp("shell"),
		Uses: strp("builtins/not-real"),
	})

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error")
	}

	msg := err.Error()
	for _, want := range []string{
		`root.steps[1].id: duplicates node id "shell"`,
		`root.steps[1].uses: unknown action "builtins/not-real"`,
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected %q in %q", want, msg)
		}
	}
}

func TestValidateJob_MaxDepth(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps = []*api.Node{{
		Id:    strp("child"),
		Uses:  strp("builtins/sequence"),
		Steps: []*api.Node{{Id: strp("grandchild"), Uses: strp("builtins/shell")}},
	}}

	err := validation.ValidateJob(job, validation.Options{MaxDepth: 2})
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum depth 2") {
		t.Fatalf("expected max depth error, got %v", err)
	}
}
