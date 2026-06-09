package validation_test

import (
	"encoding/json"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/job/validation"
	"vectis/internal/taskgraph"
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

func nodePort(nodes ...*api.Node) *api.NodePort {
	return &api.NodePort{Nodes: nodes}
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

func TestErrorDetailsIncludesStructuredFields(t *testing.T) {
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

	details := validation.ErrorDetails(err)
	if _, ok := details["error"]; ok {
		t.Fatalf("details included deprecated error field: %v", details)
	}

	fields, ok := details["fields"].([]validation.FieldError)
	if !ok {
		t.Fatalf("details fields type = %T, want []validation.FieldError", details["fields"])
	}

	want := map[string]string{
		"root.steps[1].id":   `duplicates node id "shell" first used at root.steps[0].id`,
		"root.steps[1].uses": `unknown action "builtins/not-real"`,
	}

	for _, field := range fields {
		if want[field.Field] == field.Message {
			delete(want, field.Field)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing structured fields: %v", want)
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

func TestValidateJob_ShellMissingCommand(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].With = nil

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for missing command")
	}

	msg := err.Error()
	if !strings.Contains(msg, `root.steps[0].with.command: is required`) {
		t.Fatalf("expected command required error, got %q", msg)
	}
}

func TestValidateJob_ShellEmptyCommand(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].With = map[string]string{"command": "   "}

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for empty command")
	}

	msg := err.Error()
	if !strings.Contains(msg, `root.steps[0].with.command: is required`) {
		t.Fatalf("expected command required error, got %q", msg)
	}
}

func TestValidateJob_ShellValidCommand(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].With = map[string]string{"command": "go build ./..."}

	if err := validation.ValidateJob(job, validation.Options{RequireJobID: true}); err != nil {
		t.Fatalf("expected valid shell job: %v", err)
	}
}

func TestValidateJob_CheckoutMissingURL(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].Uses = strp("builtins/checkout")
	job.Root.Steps[0].With = nil

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for missing url")
	}

	msg := err.Error()
	if !strings.Contains(msg, `root.steps[0].with.url: is required`) {
		t.Fatalf("expected url required error, got %q", msg)
	}
}

func TestValidateJob_CheckoutValidURL(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].Uses = strp("builtins/checkout")
	job.Root.Steps[0].With = map[string]string{"url": "https://github.com/example/repo.git"}

	if err := validation.ValidateJob(job, validation.Options{RequireJobID: true}); err != nil {
		t.Fatalf("expected valid checkout job: %v", err)
	}
}

func TestValidateJob_CheckoutRejectsCredentialedHTTPSURL(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].Uses = strp("builtins/checkout")
	job.Root.Steps[0].With = map[string]string{"url": "https://user:token@github.com/example/repo.git"}

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for credentialed checkout URL")
	}

	msg := err.Error()
	if !strings.Contains(msg, `root.steps[0].with.url: must not include embedded credentials`) {
		t.Fatalf("expected credential error, got %q", msg)
	}
}

func TestValidateJob_SequenceAnyWith(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.With = map[string]string{"some_key": "some_value"}

	if err := validation.ValidateJob(job, validation.Options{RequireJobID: true}); err != nil {
		t.Fatalf("expected valid sequence job: %v", err)
	}
}

func TestValidateJob_ParallelAnyWith(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Uses = strp("builtins/parallel")
	job.Root.With = map[string]string{"some_key": "some_value"}

	if err := validation.ValidateJob(job, validation.Options{RequireJobID: true}); err != nil {
		t.Fatalf("expected valid parallel job: %v", err)
	}
}

func TestValidateJob_ExplicitPorts(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps = nil
	job.Root.Ports = map[string]*api.NodePort{
		taskgraph.StepsPort: nodePort(&api.Node{
			Id:   strp("shell"),
			Uses: strp("builtins/shell"),
			With: map[string]string{"command": "echo hi"},
		}),
	}

	if err := validation.ValidateJob(job, validation.Options{RequireJobID: true}); err != nil {
		t.Fatalf("expected explicit sequence port job to validate: %v", err)
	}
}

func TestValidateJob_ExplicitPortsJSON(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"id": "job-ports-json",
		"root": {
			"id": "root",
			"uses": "builtins/sequence",
			"ports": {
				"steps": {
					"nodes": [
						{"id": "shell", "uses": "builtins/shell", "with": {"command": "echo hi"}}
					]
				}
			}
		}
	}`)

	var job api.Job
	if err := json.Unmarshal(raw, &job); err != nil {
		t.Fatalf("unmarshal job: %v", err)
	}

	if err := validation.ValidateJob(&job, validation.Options{RequireJobID: true}); err != nil {
		t.Fatalf("expected explicit ports JSON to validate: %v", err)
	}
}

func TestValidateJob_ParallelBranchesPort(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Uses = strp("builtins/parallel")
	job.Root.Steps = nil
	job.Root.Ports = map[string]*api.NodePort{
		taskgraph.BranchesPort: nodePort(&api.Node{
			Id:   strp("shell"),
			Uses: strp("builtins/shell"),
			With: map[string]string{"command": "echo hi"},
		}),
	}

	if err := validation.ValidateJob(job, validation.Options{RequireJobID: true}); err != nil {
		t.Fatalf("expected explicit parallel branches job to validate: %v", err)
	}
}

func TestValidateJob_RejectsUnknownPort(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps = nil
	job.Root.Ports = map[string]*api.NodePort{
		"condition": nodePort(&api.Node{
			Id:   strp("shell"),
			Uses: strp("builtins/shell"),
			With: map[string]string{"command": "echo hi"},
		}),
	}

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for unknown port")
	}

	if msg := err.Error(); !strings.Contains(msg, `root.ports.condition: unknown port "condition" for action "builtins/sequence"`) {
		t.Fatalf("expected unknown port error, got %q", msg)
	}
}

func TestValidateJob_RejectsStepsAndPrimaryPortTogether(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Ports = map[string]*api.NodePort{
		taskgraph.StepsPort: nodePort(&api.Node{
			Id:   strp("second"),
			Uses: strp("builtins/shell"),
			With: map[string]string{"command": "echo second"},
		}),
	}

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for mixed steps and primary port")
	}

	if msg := err.Error(); !strings.Contains(msg, `root.steps: cannot be used together with ports.steps`) {
		t.Fatalf("expected mixed steps/ports error, got %q", msg)
	}
}

func TestValidateJob_RejectsLeafChildPorts(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].Ports = map[string]*api.NodePort{
		taskgraph.StepsPort: nodePort(&api.Node{
			Id:   strp("grandchild"),
			Uses: strp("builtins/shell"),
			With: map[string]string{"command": "echo grandchild"},
		}),
	}

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for leaf child port")
	}

	if msg := err.Error(); !strings.Contains(msg, `root.steps[0].ports.steps: unknown port "steps" for action "builtins/shell"`) {
		t.Fatalf("expected leaf port error, got %q", msg)
	}
}

func TestValidateJob_ControlExecutionModeMustBeKnown(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.With = map[string]string{"execution": "remote"}

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for invalid execution mode")
	}

	if msg := err.Error(); !strings.Contains(msg, `root.with.execution: must be "local" or "distributed", got "remote"`) {
		t.Fatalf("expected execution mode error, got %q", msg)
	}
}

func TestValidateJob_ExecutionModeIsGlobalWithMetadata(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].With = map[string]string{
		"command":   "echo hi",
		"execution": "distributed",
	}

	if err := validation.ValidateJob(job, validation.Options{RequireJobID: true}); err != nil {
		t.Fatalf("expected shell execution metadata to validate: %v", err)
	}
}

func TestValidateJob_LeafExecutionModeMustBeKnown(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].With = map[string]string{
		"command":   "echo hi",
		"execution": "remote",
	}

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for invalid execution mode")
	}

	if msg := err.Error(); !strings.Contains(msg, `root.steps[0].with.execution: must be "local" or "distributed", got "remote"`) {
		t.Fatalf("expected execution mode error, got %q", msg)
	}
}

func TestValidateJob_RejectsReservedRootChildID(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].Id = strp("root")

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for reserved child id")
	}

	if msg := err.Error(); !strings.Contains(msg, `root.steps[0].id: "root" is reserved for the root task`) {
		t.Fatalf("expected reserved root task error, got %q", msg)
	}
}

func TestValidateJob_UnknownActionStillReports(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].Uses = strp("builtins/not-real")

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error")
	}

	msg := err.Error()
	if !strings.Contains(msg, `unknown action "builtins/not-real"`) {
		t.Fatalf("expected unknown action error, got %q", msg)
	}

	if strings.Contains(msg, "with.") {
		t.Fatalf("did not expect 'with' validation errors for unknown action, got %q", msg)
	}
}

func TestValidateJob_ShellUnknownKey(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].With = map[string]string{"command": "echo hi", "unknown_key": "val"}

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for unknown key")
	}

	msg := err.Error()
	if !strings.Contains(msg, `root.steps[0].with.unknown_key: unknown field "unknown_key"`) {
		t.Fatalf("expected unknown key error, got %q", msg)
	}
}

func TestValidateJob_CheckoutInvalidURL(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].Uses = strp("builtins/checkout")
	job.Root.Steps[0].With = map[string]string{"url": "not-a-valid-url"}

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for invalid URL")
	}

	msg := err.Error()
	if !strings.Contains(msg, `root.steps[0].with.url: must be a valid URL`) {
		t.Fatalf("expected invalid URL error, got %q", msg)
	}
}

func TestValidateJob_CheckoutSCPURL(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].Uses = strp("builtins/checkout")
	job.Root.Steps[0].With = map[string]string{"url": "git@github.com:user/repo.git"}

	if err := validation.ValidateJob(job, validation.Options{RequireJobID: true}); err != nil {
		t.Fatalf("expected valid checkout with SCP URL: %v", err)
	}
}

func TestValidateJob_CheckoutUnknownKey(t *testing.T) {
	t.Parallel()

	job := validJob()
	job.Root.Steps[0].Uses = strp("builtins/checkout")
	job.Root.Steps[0].With = map[string]string{"url": "https://github.com/example/repo.git", "ref": "main"}

	err := validation.ValidateJob(job, validation.Options{RequireJobID: true})
	if err == nil {
		t.Fatal("expected validation error for unknown key")
	}

	msg := err.Error()
	if !strings.Contains(msg, `unknown field "ref"`) {
		t.Fatalf("expected unknown key error, got %q", msg)
	}
}
