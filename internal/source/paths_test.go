package source

import (
	"errors"
	"testing"
)

func TestResolveDefinitionTargetDefaultsAndNormalizes(t *testing.T) {
	target, err := ResolveDefinitionTarget(DefinitionTargetRequest{
		JobID:      "team.build",
		DefaultRef: " main ",
	})
	if err != nil {
		t.Fatalf("ResolveDefinitionTarget: %v", err)
	}

	if target.Ref != "main" || target.Path != ".vectis/jobs/team/build.json" {
		t.Fatalf("target mismatch: %+v", target)
	}

	target, err = ResolveDefinitionTarget(DefinitionTargetRequest{
		JobID:      "team.build",
		Ref:        "feature/build",
		DefaultRef: "main",
		Path:       " .vectis/jobs/custom.json ",
	})
	if err != nil {
		t.Fatalf("ResolveDefinitionTarget override: %v", err)
	}

	if target.Ref != "feature/build" || target.Path != ".vectis/jobs/custom.json" {
		t.Fatalf("override target mismatch: %+v", target)
	}
}

func TestResolveDefinitionTargetDefaultsToHEAD(t *testing.T) {
	target, err := ResolveDefinitionTarget(DefinitionTargetRequest{
		JobID: "build",
	})
	if err != nil {
		t.Fatalf("ResolveDefinitionTarget: %v", err)
	}

	if target.Ref != "HEAD" || target.Path != ".vectis/jobs/build.json" {
		t.Fatalf("target mismatch: %+v", target)
	}
}

func TestResolveDefinitionTargetRejectsUnsafeInput(t *testing.T) {
	for _, tc := range []DefinitionTargetRequest{
		{JobID: "build", Ref: "HEAD~1"},
		{JobID: "build", Path: "../build.json"},
		{JobID: "team/build"},
	} {
		t.Run(tc.JobID+tc.Ref+tc.Path, func(t *testing.T) {
			if _, err := ResolveDefinitionTarget(tc); !errors.Is(err, ErrInvalidReference) {
				t.Fatalf("expected ErrInvalidReference, got %v", err)
			}
		})
	}
}
