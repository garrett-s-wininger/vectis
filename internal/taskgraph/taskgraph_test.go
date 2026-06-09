package taskgraph

import (
	"reflect"
	"testing"

	api "vectis/api/gen/go"
)

func strp(s string) *string { return &s }

func graphNode(id, uses string, with map[string]string, steps ...*api.Node) *api.Node {
	return &api.Node{
		Id:    strp(id),
		Uses:  strp(uses),
		With:  with,
		Steps: steps,
	}
}

func TestExecutionModeDefaultsAndOverrides(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		node *api.Node
		want string
	}{
		{
			name: "sequence defaults local",
			node: graphNode("seq", "builtins/sequence", nil),
			want: ExecutionLocal,
		},
		{
			name: "parallel defaults distributed",
			node: graphNode("fanout", "builtins/parallel", nil),
			want: ExecutionDistributed,
		},
		{
			name: "parallel can run local",
			node: graphNode("fanout", "builtins/parallel", map[string]string{ExecutionField: ExecutionLocal}),
			want: ExecutionLocal,
		},
		{
			name: "leaf can be distributed",
			node: graphNode("shell", "builtins/shell", map[string]string{ExecutionField: ExecutionDistributed}),
			want: ExecutionDistributed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := ExecutionMode(tt.node); got != tt.want {
				t.Fatalf("ExecutionMode: got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLocalChildrenForTaskSequenceStopsAtDistributedBoundary(t *testing.T) {
	t.Parallel()

	root := graphNode("root-node", "builtins/sequence", nil,
		graphNode("setup", "builtins/shell", nil),
		graphNode("checks", "builtins/parallel", nil,
			graphNode("unit", "builtins/shell", nil),
			graphNode("lint", "builtins/shell", nil),
		),
		graphNode("deploy", "builtins/shell", nil),
	)

	if got := nodeIDs(LocalChildrenForTask(root)); !reflect.DeepEqual(got, []string{"setup"}) {
		t.Fatalf("local children: got %+v, want [setup]", got)
	}
}

func TestPlanTaskBoundariesQueuesDistributedSubtreeAndSequenceTail(t *testing.T) {
	t.Parallel()

	job := &api.Job{
		Root: graphNode("root-node", "builtins/sequence", nil,
			graphNode("setup", "builtins/shell", nil),
			graphNode("checks", "builtins/parallel", nil,
				graphNode("unit", "builtins/shell", nil),
				graphNode("lint", "builtins/shell", nil),
			),
			graphNode("deploy", "builtins/shell", nil),
		),
	}

	plan, err := PlanTaskBoundaries(job, "root")
	if err != nil {
		t.Fatalf("PlanTaskBoundaries: %v", err)
	}

	if got := taskKeys(plan.Entries); !reflect.DeepEqual(got, []string{"checks", "unit", "lint", "deploy"}) {
		t.Fatalf("entries: got %+v, want [checks unit lint deploy]", got)
	}

	if got := plan.ChildrenByKey["root"]; !reflect.DeepEqual(got, []string{"checks", "deploy"}) {
		t.Fatalf("root children: got %+v, want [checks deploy]", got)
	}

	if got := plan.ChildrenByKey["checks"]; !reflect.DeepEqual(got, []string{"unit", "lint"}) {
		t.Fatalf("checks children: got %+v, want [unit lint]", got)
	}
}

func nodeIDs(nodes []*api.Node) []string {
	out := make([]string, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, node.GetId())
	}
	return out
}

func taskKeys(entries []BoundaryEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.TaskKey)
	}
	return out
}
