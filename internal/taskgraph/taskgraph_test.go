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

func portGraphNode(id, uses string, ports map[string][]*api.Node) *api.Node {
	nodePorts := make(map[string]*api.NodePort, len(ports))
	for name, nodes := range ports {
		nodePorts[name] = &api.NodePort{Nodes: nodes}
	}

	return &api.Node{
		Id:    strp(id),
		Uses:  strp(uses),
		Ports: nodePorts,
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

func TestPlanTaskBoundariesSupportsExplicitPorts(t *testing.T) {
	t.Parallel()

	job := &api.Job{
		Root: portGraphNode("root-node", "builtins/sequence", map[string][]*api.Node{
			StepsPort: {
				graphNode("setup", "builtins/shell", nil),
				portGraphNode("checks", "builtins/parallel", map[string][]*api.Node{
					BranchesPort: {
						graphNode("unit", "builtins/shell", nil),
						graphNode("lint", "builtins/shell", nil),
					},
				}),
				graphNode("deploy", "builtins/shell", nil),
			},
		}),
	}

	plan, err := PlanTaskBoundaries(job, "root")
	if err != nil {
		t.Fatalf("PlanTaskBoundaries: %v", err)
	}

	if got := taskKeys(plan.Entries); !reflect.DeepEqual(got, []string{"checks", "unit", "lint", "deploy"}) {
		t.Fatalf("entries: got %+v, want [checks unit lint deploy]", got)
	}

	if got := plan.NodePathByKey["checks"]; got != "root.ports.steps.nodes[1]" {
		t.Fatalf("checks path: got %q", got)
	}

	if got := plan.NodePathByKey["unit"]; got != "root.ports.steps.nodes[1].ports.branches.nodes[0]" {
		t.Fatalf("unit path: got %q", got)
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
