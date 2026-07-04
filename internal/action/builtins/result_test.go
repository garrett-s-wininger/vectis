package builtins

import (
	"context"
	"strings"
	"testing"

	"vectis/internal/action"
)

func TestResultAction_Type(t *testing.T) {
	resultAction := &ResultAction{}
	if resultAction.Type() != "builtins/result" {
		t.Errorf("expected 'builtins/result', got %q", resultAction.Type())
	}
}

func TestResultAction_Execute_Success(t *testing.T) {
	resultAction := &ResultAction{}
	result := resultAction.Execute(context.Background(), createTestState(nil), map[string]any{
		"success": "true",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %v with error %v", result.Status, result.Error)
	}

	if got := result.Outputs["success"]; got != true {
		t.Fatalf("expected success output true, got %#v", got)
	}
}

func TestResultAction_Execute_Failure(t *testing.T) {
	resultAction := &ResultAction{}
	result := resultAction.Execute(context.Background(), createTestState(nil), map[string]any{
		"success": "false",
	}, nil)

	if result.Status != action.StatusFailure {
		t.Fatalf("expected failure, got %v", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), "returned false") {
		t.Fatalf("expected returned false error, got %v", result.Error)
	}

	if got := result.Outputs["success"]; got != false {
		t.Fatalf("expected success output false, got %#v", got)
	}
}

func TestResultAction_ValidateWith(t *testing.T) {
	resultAction := &ResultAction{}

	cases := []struct {
		name string
		with map[string]string
		want string
	}{
		{name: "valid true", with: map[string]string{"success": "true"}},
		{name: "valid false", with: map[string]string{"success": "false"}},
		{name: "missing", with: map[string]string{}, want: "is required"},
		{name: "invalid", with: map[string]string{"success": "maybe"}, want: "must be true or false"},
		{name: "unknown", with: map[string]string{"success": "true", "extra": "x"}, want: `unknown field "extra"`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errs := resultAction.ValidateWith(tc.with)
			if tc.want == "" {
				if len(errs) != 0 {
					t.Fatalf("expected no validation errors, got %+v", errs)
				}

				return
			}

			if len(errs) == 0 {
				t.Fatalf("expected validation error containing %q", tc.want)
			}

			if !strings.Contains(errs[0].Message, tc.want) {
				t.Fatalf("expected validation error containing %q, got %+v", tc.want, errs)
			}
		})
	}
}

func TestResultAction_Registry(t *testing.T) {
	registry := NewRegistry()

	node, err := registry.Resolve("builtins/result")
	if err != nil {
		t.Fatalf("resolve builtins/result: %v", err)
	}

	if node.Type() != "builtins/result" {
		t.Fatalf("resolved type: got %q", node.Type())
	}

	node, err = registry.Resolve("result")
	if err != nil {
		t.Fatalf("resolve result shorthand: %v", err)
	}

	if node.Type() != "builtins/result" {
		t.Fatalf("resolved shorthand type: got %q", node.Type())
	}
}
