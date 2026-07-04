package builtins

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/taskgraph"
)

type inputCaptureNode struct {
	mu     sync.Mutex
	inputs map[string]any
	result action.Result
	count  atomic.Int32
}

func (n *inputCaptureNode) Type() string { return "test/input-capture" }

func (n *inputCaptureNode) ValidateWith(map[string]string) []action.FieldError { return nil }

func (n *inputCaptureNode) Execute(_ context.Context, _ *action.ExecutionState, inputs map[string]any, _ action.Ports) action.Result {
	n.count.Add(1)

	n.mu.Lock()
	defer n.mu.Unlock()

	n.inputs = make(map[string]any, len(inputs))
	for key, value := range inputs {
		n.inputs[key] = value
	}

	return n.result
}

func (n *inputCaptureNode) Inputs() map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()

	out := make(map[string]any, len(n.inputs))
	for key, value := range n.inputs {
		out[key] = value
	}

	return out
}

func TestSequenceNodeResolvesBoundInputs(t *testing.T) {
	producer := &countedNode{
		count:  &atomic.Int32{},
		result: action.NewSuccessResult(map[string]any{"command": "echo from output"}),
	}

	consumer := &inputCaptureNode{
		result: action.NewSuccessResult(map[string]any{"final": "ok"}),
	}

	state := policyTestState(testResolver{
		"test/producer": producer,
		"test/consumer": consumer,
	})

	result := (&SequenceNode{}).Execute(context.Background(), state, nil, action.Ports{
		taskgraph.StepsPort: {
			ifTestNode("producer", "test/producer"),
			inputBindingNode("consumer", "test/consumer", "command", "producer", "command"),
		},
	})

	if result.Status != action.StatusSuccess {
		t.Fatalf("status: got %s err=%v, want success", result.Status, result.Error)
	}

	if got := consumer.Inputs()["command"]; got != "echo from output" {
		t.Fatalf("bound input: got %v, want command output", got)
	}

	if got := result.Outputs["final"]; got != "ok" {
		t.Fatalf("sequence outputs: got %+v, want final=ok", result.Outputs)
	}
}

func TestSequenceNodeFailsWhenBoundOutputUnavailable(t *testing.T) {
	producer := &countedNode{
		count:  &atomic.Int32{},
		result: action.NewSuccessResult(map[string]any{"other": "value"}),
	}

	consumer := &inputCaptureNode{result: action.NewSuccessResult(nil)}
	state := policyTestState(testResolver{
		"test/producer": producer,
		"test/consumer": consumer,
	})

	result := (&SequenceNode{}).Execute(context.Background(), state, nil, action.Ports{
		taskgraph.StepsPort: {
			ifTestNode("producer", "test/producer"),
			inputBindingNode("consumer", "test/consumer", "command", "producer", "command"),
		},
	})

	if result.Status != action.StatusFailure {
		t.Fatalf("status: got %s, want failure", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), "references unavailable output producer.command") {
		t.Fatalf("error: got %v, want unavailable output", result.Error)
	}

	if got := consumer.count.Load(); got != 0 {
		t.Fatalf("consumer executions: got %d, want 0", got)
	}
}

func inputBindingNode(id, uses, inputName, fromNode, fromOutput string) *api.Node {
	return &api.Node{
		Id:   ifTestStrp(id),
		Uses: ifTestStrp(uses),
		Inputs: map[string]*api.NodeInput{
			inputName: {
				From: &api.NodeOutputRef{
					Node:   ifTestStrp(fromNode),
					Output: ifTestStrp(fromOutput),
				},
			},
		},
	}
}
