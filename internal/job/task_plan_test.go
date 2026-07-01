package job_test

import (
	"fmt"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action/actionregistry"
	"vectis/internal/dal"
	"vectis/internal/job"
)

func taskPlanStrp(s string) *string { return &s }

func TestPlanTaskExecutionsBuildsStableBoundaryPlan(t *testing.T) {
	t.Parallel()

	plan, err := job.PlanTaskExecutions(taskPlanJob("echo compile"))
	if err != nil {
		t.Fatalf("PlanTaskExecutions: %v", err)
	}

	if len(plan) != 3 {
		t.Fatalf("plan len: got %d, want 3: %+v", len(plan), plan)
	}

	assertPlanEntry(t, plan[0], "build", dal.RootTaskKey, "root.steps[1]", "builtins/parallel", []string{"compile", "test"})
	assertPlanEntry(t, plan[1], "compile", "build", "root.steps[1].steps[0]", "builtins/script", nil)
	assertPlanEntry(t, plan[2], "test", "build", "root.steps[1].steps[1]", "builtins/script", nil)

	again, err := job.PlanTaskExecutions(taskPlanJob("echo compile"))
	if err != nil {
		t.Fatalf("PlanTaskExecutions again: %v", err)
	}

	for i := range plan {
		if plan[i].SpecHash != again[i].SpecHash {
			t.Fatalf("spec hash %d changed: got %q then %q", i, plan[i].SpecHash, again[i].SpecHash)
		}
	}

	changed, err := job.PlanTaskExecutions(taskPlanJob("echo changed"))
	if err != nil {
		t.Fatalf("PlanTaskExecutions changed: %v", err)
	}

	if plan[1].SpecHash == changed[1].SpecHash {
		t.Fatalf("compile spec hash did not change after command changed: %q", plan[1].SpecHash)
	}

	if plan[0].SpecHash != changed[0].SpecHash {
		t.Fatalf("unrelated build spec hash changed: got %q then %q", plan[0].SpecHash, changed[0].SpecHash)
	}
}

func TestPlanTaskExecutionsAllowsRootOnlyJob(t *testing.T) {
	t.Parallel()

	jobID := "job-root-only"
	runID := "run-root-only"
	rootID := "root"
	uses := "builtins/script"
	plan, err := job.PlanTaskExecutions(&api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &uses,
			With: map[string]string{"script": "echo root"},
		},
	})

	if err != nil {
		t.Fatalf("PlanTaskExecutions: %v", err)
	}

	if len(plan) != 0 {
		t.Fatalf("root-only plan: got %+v, want empty", plan)
	}
}

func TestPlanTaskExecutionsIncludesActionDigestsInSpecHash(t *testing.T) {
	t.Parallel()

	first, err := job.PlanTaskExecutionsWithActions(taskPlanJob("echo compile"), taskPlanResolver(
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	))

	if err != nil {
		t.Fatalf("PlanTaskExecutionsWithActions first: %v", err)
	}

	second, err := job.PlanTaskExecutionsWithActions(taskPlanJob("echo compile"), taskPlanResolver(
		"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	))

	if err != nil {
		t.Fatalf("PlanTaskExecutionsWithActions second: %v", err)
	}

	if len(first) == 0 || len(first) != len(second) {
		t.Fatalf("unexpected plan lengths: first=%d second=%d", len(first), len(second))
	}

	for i := range first {
		if first[i].SpecHash == second[i].SpecHash {
			t.Fatalf("task %q spec hash did not change after resolved script digest changed: %q", first[i].TaskKey, first[i].SpecHash)
		}
	}
}

func TestPlanTaskExecutionsRejectsInvalidTaskKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(*api.Job)
		want string
	}{
		{
			name: "reserved root child",
			edit: func(j *api.Job) {
				j.Root.Steps[0].Id = taskPlanStrp(dal.RootTaskKey)
			},
			want: `root.steps[0].id "root" is reserved`,
		},
		{
			name: "duplicate child",
			edit: func(j *api.Job) {
				j.Root.Steps[1].Steps[1].Id = taskPlanStrp("compile")
			},
			want: `root.steps[1].steps[1].id duplicates task key "compile"`,
		},
		{
			name: "duplicates root node",
			edit: func(j *api.Job) {
				j.Root.Id = taskPlanStrp("setup")
			},
			want: `root.steps[0].id duplicates task key "setup" first used at root.id`,
		},
		{
			name: "nil child",
			edit: func(j *api.Job) {
				j.Root.Steps[1].Steps[0] = nil
			},
			want: "root.steps[1].steps[0] is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			j := taskPlanJob("echo compile")
			tt.edit(j)

			_, err := job.PlanTaskExecutions(j)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q, got %v", tt.want, err)
			}
		})
	}
}

func assertPlanEntry(t *testing.T, got job.TaskPlanEntry, key, parent, path, uses string, children []string) {
	t.Helper()

	if got.TaskKey != key || got.Name != key || got.NodeID != key || got.ParentTaskKey != parent || got.NodePath != path || got.Uses != uses {
		t.Fatalf("plan entry mismatch: got %+v", got)
	}

	if !strings.HasPrefix(got.SpecHash, "sha256:") || len(got.SpecHash) != len("sha256:")+64 {
		t.Fatalf("spec hash: got %q", got.SpecHash)
	}

	if len(got.ChildTaskKeys) != len(children) {
		t.Fatalf("children for %s: got %+v, want %+v", key, got.ChildTaskKeys, children)
	}

	for i := range children {
		if got.ChildTaskKeys[i] != children[i] {
			t.Fatalf("children for %s: got %+v, want %+v", key, got.ChildTaskKeys, children)
		}
	}
}

func taskPlanJob(compileCommand string) *api.Job {
	jobID := "job-task-plan"
	runID := "run-task-plan"
	rootID := "root"
	rootUses := "builtins/sequence"
	setupID := "setup"
	buildID := "build"
	compileID := "compile"
	testID := "test"
	buildUses := "builtins/parallel"
	scriptUses := "builtins/script"

	return &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &rootUses,
			Steps: []*api.Node{
				{
					Id:   &setupID,
					Uses: &scriptUses,
					With: map[string]string{"script": "echo setup"},
				},
				{
					Id:   &buildID,
					Uses: &buildUses,
					Steps: []*api.Node{
						{
							Id:   &compileID,
							Uses: &scriptUses,
							With: map[string]string{"script": compileCommand},
						},
						{
							Id:   &testID,
							Uses: &scriptUses,
							With: map[string]string{"script": "echo test"},
						},
					},
				},
			},
		},
	}
}

type fakeTaskPlanResolver struct {
	descriptors map[string]actionregistry.Descriptor
}

func (r fakeTaskPlanResolver) ResolveDescriptor(uses string) (actionregistry.Descriptor, error) {
	descriptor, ok := r.descriptors[uses]
	if !ok {
		return actionregistry.Descriptor{}, fmt.Errorf("unknown action: %s", uses)
	}

	return descriptor, nil
}

func taskPlanResolver(shellDigest, parallelDigest string) fakeTaskPlanResolver {
	return fakeTaskPlanResolver{
		descriptors: map[string]actionregistry.Descriptor{
			"builtins/script": {
				CanonicalName: "builtins/script",
				Version:       "v1",
				Digest:        shellDigest,
				Source:        actionregistry.SourceBuiltin,
				Runtime:       actionregistry.RuntimeBuiltin,
			},
			"builtins/parallel": {
				CanonicalName: "builtins/parallel",
				Version:       "v1",
				Digest:        parallelDigest,
				Source:        actionregistry.SourceBuiltin,
				Runtime:       actionregistry.RuntimeBuiltin,
			},
			"builtins/sequence": {
				CanonicalName: "builtins/sequence",
				Version:       "v1",
				Digest:        "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
				Source:        actionregistry.SourceBuiltin,
				Runtime:       actionregistry.RuntimeBuiltin,
			},
		},
	}
}
