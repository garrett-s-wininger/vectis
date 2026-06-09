package builtins

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces"
	"vectis/internal/taskgraph"
)

func ifTestStrp(s string) *string { return &s }

func TestIfNodeExecuteThenBranch(t *testing.T) {
	condition := &countedNode{count: &atomic.Int32{}, result: action.NewSuccessResult(map[string]any{TestResultOutput: true})}
	thenCount := &atomic.Int32{}
	elseCount := &atomic.Int32{}
	thenNode := &countedNode{count: thenCount, result: action.NewSuccessResult(nil)}
	elseNode := &countedNode{count: elseCount, result: action.NewSuccessResult(nil)}
	state := ifTestState(testResolver{
		"test/condition": condition,
		"test/then":      thenNode,
		"test/else":      elseNode,
	})

	result := (&IfNode{}).Execute(context.Background(), state, nil, action.Ports{
		taskgraph.ConditionPort: {ifTestNode("condition", "test/condition")},
		taskgraph.ThenPort:      {ifTestNode("then", "test/then")},
		taskgraph.ElsePort:      {ifTestNode("else", "test/else")},
	})

	if result.Status != action.StatusSuccess {
		t.Fatalf("status: got %s err=%v, want success", result.Status, result.Error)
	}

	if got := thenCount.Load(); got != 1 {
		t.Fatalf("then executions: got %d, want 1", got)
	}

	if got := elseCount.Load(); got != 0 {
		t.Fatalf("else executions: got %d, want 0", got)
	}
}

func TestIfNodeExecuteElseBranch(t *testing.T) {
	condition := &countedNode{count: &atomic.Int32{}, result: action.NewSuccessResult(map[string]any{TestResultOutput: false})}
	thenCount := &atomic.Int32{}
	elseCount := &atomic.Int32{}
	thenNode := &countedNode{count: thenCount, result: action.NewSuccessResult(nil)}
	elseNode := &countedNode{count: elseCount, result: action.NewSuccessResult(nil)}
	state := ifTestState(testResolver{
		"test/condition": condition,
		"test/then":      thenNode,
		"test/else":      elseNode,
	})

	result := (&IfNode{}).Execute(context.Background(), state, nil, action.Ports{
		taskgraph.ConditionPort: {ifTestNode("condition", "test/condition")},
		taskgraph.ThenPort:      {ifTestNode("then", "test/then")},
		taskgraph.ElsePort:      {ifTestNode("else", "test/else")},
	})

	if result.Status != action.StatusSuccess {
		t.Fatalf("status: got %s err=%v, want success", result.Status, result.Error)
	}

	if got := thenCount.Load(); got != 0 {
		t.Fatalf("then executions: got %d, want 0", got)
	}

	if got := elseCount.Load(); got != 1 {
		t.Fatalf("else executions: got %d, want 1", got)
	}
}

func TestIfNodeExecuteConditionFailure(t *testing.T) {
	wantErr := errors.New("condition exploded")
	condition := &countedNode{count: &atomic.Int32{}, result: action.NewFailureResult(wantErr)}
	thenCount := &atomic.Int32{}
	state := ifTestState(testResolver{
		"test/condition": condition,
		"test/then":      &countedNode{count: thenCount, result: action.NewSuccessResult(nil)},
	})

	result := (&IfNode{}).Execute(context.Background(), state, nil, action.Ports{
		taskgraph.ConditionPort: {ifTestNode("condition", "test/condition")},
		taskgraph.ThenPort:      {ifTestNode("then", "test/then")},
	})

	if result.Status != action.StatusFailure {
		t.Fatalf("status: got %s, want failure", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), wantErr.Error()) {
		t.Fatalf("error: got %v, want %v", result.Error, wantErr)
	}

	if got := thenCount.Load(); got != 0 {
		t.Fatalf("then executions: got %d, want 0", got)
	}
}

func TestIfNodeExecuteRequiresBooleanResultOutput(t *testing.T) {
	condition := &countedNode{count: &atomic.Int32{}, result: action.NewSuccessResult(nil)}
	state := ifTestState(testResolver{"test/condition": condition})

	result := (&IfNode{}).Execute(context.Background(), state, nil, action.Ports{
		taskgraph.ConditionPort: {ifTestNode("condition", "test/condition")},
	})

	if result.Status != action.StatusFailure {
		t.Fatalf("status: got %s, want failure", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), `"result" is required`) {
		t.Fatalf("error: got %v, want missing result", result.Error)
	}
}

func TestIfNodePortSchema(t *testing.T) {
	schema := (&IfNode{}).PortSchema()
	if len(schema) != 3 {
		t.Fatalf("schema len: got %d, want 3", len(schema))
	}

	if !(&IfNode{}).LocalOnly() {
		t.Fatal("if node should be local-only")
	}
}

func ifTestState(resolver action.Resolver) *action.ExecutionState {
	return &action.ExecutionState{
		JobID:    "if-test",
		Logger:   interfaces.NewLogger("if-test"),
		Resolver: resolver,
	}
}

func ifTestNode(id, uses string) *api.Node {
	return &api.Node{
		Id:   ifTestStrp(id),
		Uses: ifTestStrp(uses),
	}
}
