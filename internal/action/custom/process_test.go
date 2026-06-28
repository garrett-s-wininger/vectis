package custom

import (
	"context"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/interfaces/mocks"
)

func TestProcessActionExecutesDescriptorCommand(t *testing.T) {
	executor := mocks.NewMockExecExecutor()
	process := mocks.NewMockProcess()
	executor.SetProcess(process)

	act := NewProcessAction(actionregistry.Descriptor{
		CanonicalName: "acme/deploy",
		Version:       "v1",
		Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Source:        actionregistry.SourceLocalFilesystem,
		SourcePath:    "/opt/vectis/actions/acme/deploy",
		Runtime:       actionregistry.RuntimeProcess,
		RuntimeConfig: map[string]string{
			"command": "./deploy.sh",
		},
	}, executor)

	state := &action.ExecutionState{
		Workspace:  "/work/run-1",
		ProcessEnv: action.SanitizedProcessEnv("/work/run-1", []string{"PATH=/bin"}),
		Logger:     mocks.NewMockLogger(),
	}

	result := act.Execute(context.Background(), state, map[string]any{
		"environment": "staging",
		"dry-run":     true,
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("Execute status = %s err=%v, want success", result.Status, result.Error)
	}

	if got := executor.GetPaths(); len(got) != 1 || got[0] != "sh" {
		t.Fatalf("executor paths = %v, want [sh]", got)
	}

	args := executor.GetArgs()
	if len(args) != 1 || len(args[0]) != 2 || args[0][0] != "-c" || args[0][1] != "./deploy.sh" {
		t.Fatalf("executor args = %v, want [-c ./deploy.sh]", args)
	}

	if got := executor.GetWorkDirs(); len(got) != 1 || got[0] != "/opt/vectis/actions/acme/deploy" {
		t.Fatalf("executor workdirs = %v, want action source path", got)
	}

	env := strings.Join(executor.GetEnvs()[0], "\n")
	for _, want := range []string{
		"VECTIS_ACTION_NAME=acme/deploy",
		"VECTIS_ACTION_VERSION=v1",
		"VECTIS_ACTION_DIGEST=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"VECTIS_WORKSPACE=/work/run-1",
		"VECTIS_INPUT_ENVIRONMENT=staging",
		"VECTIS_INPUT_DRY_RUN=true",
	} {
		if !strings.Contains(env, want) {
			t.Fatalf("env missing %q in:\n%s", want, env)
		}
	}
}

func TestProcessActionRejectsMissingCommand(t *testing.T) {
	act := NewProcessAction(actionregistry.Descriptor{
		CanonicalName: "acme/deploy",
		Version:       "v1",
		Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Runtime:       actionregistry.RuntimeProcess,
	}, nil)

	result := act.Execute(context.Background(), &action.ExecutionState{Logger: mocks.NewMockLogger()}, nil, nil)
	if result.Status != action.StatusFailure || result.Error == nil || !strings.Contains(result.Error.Error(), "runtime_config.command") {
		t.Fatalf("Execute result = %+v, want missing command failure", result)
	}
}

func TestProcessActionRejectsChildrenWhenUnsupported(t *testing.T) {
	act := NewProcessAction(actionregistry.Descriptor{
		CanonicalName: "acme/deploy",
		Version:       "v1",
		Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Runtime:       actionregistry.RuntimeProcess,
		RuntimeConfig: map[string]string{"command": "true"},
	}, nil)

	result := act.Execute(context.Background(), &action.ExecutionState{Logger: mocks.NewMockLogger()}, nil, action.Ports{"steps": []*api.Node{{}}})
	if result.Status != action.StatusFailure || result.Error == nil || !strings.Contains(result.Error.Error(), "does not support child ports") {
		t.Fatalf("Execute result = %+v, want child-port failure", result)
	}
}

func TestProcessActionRejectsMissingWorkingDirectoryBase(t *testing.T) {
	act := NewProcessAction(actionregistry.Descriptor{
		CanonicalName: "acme/deploy",
		Version:       "v1",
		Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Runtime:       actionregistry.RuntimeProcess,
		RuntimeConfig: map[string]string{"command": "true"},
	}, mocks.NewMockExecExecutor())

	result := act.Execute(context.Background(), &action.ExecutionState{Logger: mocks.NewMockLogger()}, nil, nil)
	if result.Status != action.StatusFailure || result.Error == nil || !strings.Contains(result.Error.Error(), "base directory is required") {
		t.Fatalf("Execute result = %+v, want working directory base failure", result)
	}
}

func TestProcessActionRejectsAbsoluteWorkingDirectory(t *testing.T) {
	act := NewProcessAction(actionregistry.Descriptor{
		CanonicalName: "acme/deploy",
		Version:       "v1",
		Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Runtime:       actionregistry.RuntimeProcess,
		RuntimeConfig: map[string]string{
			"command":           "true",
			"working_directory": "/tmp",
		},
	}, mocks.NewMockExecExecutor())

	state := &action.ExecutionState{
		Workspace: "/work/run-1",
		Logger:    mocks.NewMockLogger(),
	}

	result := act.Execute(context.Background(), state, nil, nil)
	if result.Status != action.StatusFailure || result.Error == nil || !strings.Contains(result.Error.Error(), "must be relative") {
		t.Fatalf("Execute result = %+v, want relative working directory failure", result)
	}
}

func TestProcessActionRejectsEscapingWorkingDirectory(t *testing.T) {
	act := NewProcessAction(actionregistry.Descriptor{
		CanonicalName: "acme/deploy",
		Version:       "v1",
		Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Runtime:       actionregistry.RuntimeProcess,
		RuntimeConfig: map[string]string{
			"command":           "true",
			"working_directory": "../outside",
		},
	}, mocks.NewMockExecExecutor())

	state := &action.ExecutionState{
		Workspace: "/work/run-1",
		Logger:    mocks.NewMockLogger(),
	}

	result := act.Execute(context.Background(), state, nil, nil)
	if result.Status != action.StatusFailure || result.Error == nil || !strings.Contains(result.Error.Error(), "must stay within the action base directory") {
		t.Fatalf("Execute result = %+v, want working directory containment failure", result)
	}
}
