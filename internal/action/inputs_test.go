package action

import (
	"strings"
	"testing"

	api "vectis/api/gen/go"
)

func TestResolveNodeInputsCoercesNumberBinding(t *testing.T) {
	state := &ExecutionState{}
	state.RecordOutputs("policy", map[string]any{"attempts": float64(2)})

	inputs, err := ResolveNodeInputs(state, boundInputNode("attempts", "policy", "attempts"), []FieldSpec{
		{Name: "attempts", Type: FieldNumber},
	})

	if err != nil {
		t.Fatalf("resolve inputs: %v", err)
	}

	if got := inputs["attempts"]; got != "2" {
		t.Fatalf("attempts: got %v (%T), want string 2", got, got)
	}
}

func TestResolveNodeInputsRejectsStringBindingTypeMismatch(t *testing.T) {
	state := &ExecutionState{}
	state.RecordOutputs("producer", map[string]any{"command": true})

	_, err := ResolveNodeInputs(state, boundInputNode("command", "producer", "command"), []FieldSpec{
		{Name: "command", Type: FieldString},
	})

	if err == nil {
		t.Fatal("expected type mismatch")
	}

	if !strings.Contains(err.Error(), `input "command" must resolve to a string`) {
		t.Fatalf("error: got %v, want string mismatch", err)
	}
}

func boundInputNode(inputName, fromNode, fromOutput string) *api.Node {
	return &api.Node{
		Inputs: map[string]*api.NodeInput{
			inputName: {
				From: &api.NodeOutputRef{
					Node:   stringPtr(fromNode),
					Output: stringPtr(fromOutput),
				},
			},
		},
	}
}

func stringPtr(value string) *string {
	return &value
}
