package actionregistry

import (
	"fmt"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
)

func TestResolveJobActions(t *testing.T) {
	t.Parallel()

	job := actionLockJob()
	resolver := fakeDescriptorResolver{
		descriptors: map[string]Descriptor{
			"builtins/sequence": descriptorForLockTest("builtins/sequence", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			"builtins/script":   descriptorForLockTest("builtins/script", "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		},
	}

	locks, err := ResolveJobActions(job, resolver)
	if err != nil {
		t.Fatalf("ResolveJobActions: %v", err)
	}

	if len(locks) != 2 {
		t.Fatalf("lock count: got %d, want 2: %+v", len(locks), locks)
	}

	if locks[0].NodeID != "root" || locks[0].NodePath != "root" || locks[0].Descriptor.CanonicalName != "builtins/sequence" {
		t.Fatalf("root lock mismatch: %+v", locks[0])
	}

	if locks[1].NodeID != "script" || locks[1].NodePath != "root.steps[0]" || locks[1].Descriptor.CanonicalName != "builtins/script" {
		t.Fatalf("child lock mismatch: %+v", locks[1])
	}
}

func TestResolveJobActionsReportsPath(t *testing.T) {
	t.Parallel()

	job := actionLockJob()
	resolver := fakeDescriptorResolver{
		descriptors: map[string]Descriptor{
			"builtins/sequence": descriptorForLockTest("builtins/sequence", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		},
	}

	_, err := ResolveJobActions(job, resolver)
	if err == nil || !strings.Contains(err.Error(), `root.steps[0].uses`) {
		t.Fatalf("expected child uses path, got %v", err)
	}
}

func TestValidateActionLocks(t *testing.T) {
	t.Parallel()

	lock := ActionLock{
		NodePath: "root",
		Uses:     "builtins/script",
		Descriptor: Descriptor{
			CanonicalName: "builtins/script",
			Version:       "v1",
			Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Source:        SourceBuiltin,
			Runtime:       RuntimeBuiltin,
		},
	}

	if err := ValidateActionLocks([]ActionLock{lock}); err != nil {
		t.Fatalf("ValidateActionLocks: %v", err)
	}

	lock.Descriptor.Digest = ""
	err := ValidateActionLocks([]ActionLock{lock})
	if err == nil || !strings.Contains(err.Error(), "descriptor.digest") {
		t.Fatalf("expected missing digest error, got %v", err)
	}
}

type fakeDescriptorResolver struct {
	descriptors map[string]Descriptor
}

func (r fakeDescriptorResolver) ResolveDescriptor(uses string) (Descriptor, error) {
	descriptor, ok := r.descriptors[uses]
	if !ok {
		return Descriptor{}, fmt.Errorf("unknown action: %s", uses)
	}

	return descriptor, nil
}

func descriptorForLockTest(name, digest string) Descriptor {
	return Descriptor{
		CanonicalName: name,
		Version:       "v1",
		Digest:        digest,
		Source:        SourceBuiltin,
		Runtime:       RuntimeBuiltin,
		InputSchema: InputSchema{
			Fields: []InputField{{Name: "script", Type: action.FieldString, Required: true}},
		},
	}
}

func actionLockJob() *api.Job {
	jobID := "job-lock"
	runID := "run-lock"
	rootID := "root"
	childID := "script"
	sequenceUses := "builtins/sequence"
	scriptUses := "builtins/script"

	return &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &sequenceUses,
			Steps: []*api.Node{{
				Id:   &childID,
				Uses: &scriptUses,
				With: map[string]string{"script": "echo hi"},
			}},
		},
	}
}
