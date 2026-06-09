package taskgraph

import (
	"fmt"
	"strings"

	api "vectis/api/gen/go"
)

const (
	ExecutionField       = "execution"
	ExecutionLocal       = "local"
	ExecutionDistributed = "distributed"
)

type BoundaryEntry struct {
	TaskKey       string
	ParentTaskKey string
	NodePath      string
	Uses          string
	Node          *api.Node
	ChildTaskKeys []string
}

type BoundaryPlan struct {
	Entries        []BoundaryEntry
	UsesByTaskKey  map[string]string
	ParentByKey    map[string]string
	ChildrenByKey  map[string][]string
	NodePathByKey  map[string]string
	BoundaryNodeBy map[string]*api.Node
}

func NormalizeUses(uses string) string {
	uses = strings.TrimSpace(uses)
	if uses == "" {
		return ""
	}

	if !strings.Contains(uses, "/") {
		return "builtins/" + uses
	}

	return uses
}

func ExecutionMode(node *api.Node) string {
	if node == nil {
		return ExecutionLocal
	}

	if raw := strings.TrimSpace(node.GetWith()[ExecutionField]); raw != "" {
		switch strings.ToLower(raw) {
		case ExecutionLocal:
			return ExecutionLocal
		case ExecutionDistributed:
			return ExecutionDistributed
		}
	}

	switch NormalizeUses(node.GetUses()) {
	case "builtins/parallel":
		return ExecutionDistributed
	default:
		return ExecutionLocal
	}
}

func ValidExecutionMode(mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", ExecutionLocal, ExecutionDistributed:
		return true
	default:
		return false
	}
}

func ActionWith(with map[string]string) map[string]string {
	if len(with) == 0 {
		return nil
	}

	out := make(map[string]string, len(with))
	for key, value := range with {
		if key == ExecutionField {
			continue
		}

		out[key] = value
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func ActionInputs(with map[string]string) map[string]any {
	if len(with) == 0 {
		return nil
	}

	out := make(map[string]any, len(with))
	for key, value := range with {
		if key == ExecutionField {
			continue
		}

		out[key] = value
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func IsSequence(node *api.Node) bool {
	return NormalizeUses(node.GetUses()) == "builtins/sequence"
}

func ContainsDistributedBoundary(node *api.Node) bool {
	if node == nil {
		return false
	}

	if ExecutionMode(node) == ExecutionDistributed {
		return true
	}

	for _, child := range node.GetSteps() {
		if ContainsDistributedBoundary(child) {
			return true
		}
	}

	return false
}

func LocalChildrenForTask(node *api.Node) []*api.Node {
	if node == nil || ExecutionMode(node) == ExecutionDistributed {
		return nil
	}

	children := node.GetSteps()
	if len(children) == 0 {
		return nil
	}

	if !IsSequence(node) {
		local := make([]*api.Node, 0, len(children))
		for _, child := range children {
			if ContainsDistributedBoundary(child) {
				continue
			}

			local = append(local, child)
		}

		return local
	}

	local := make([]*api.Node, 0, len(children))
	for _, child := range children {
		if ContainsDistributedBoundary(child) {
			break
		}

		local = append(local, child)
	}

	return local
}

func PlanTaskBoundaries(job *api.Job, rootTaskKey string) (BoundaryPlan, error) {
	if job == nil {
		return BoundaryPlan{}, fmt.Errorf("job is required")
	}

	if job.GetRoot() == nil {
		return BoundaryPlan{}, fmt.Errorf("job root is required")
	}

	seen := map[string]string{}
	if rootID := strings.TrimSpace(job.GetRoot().GetId()); rootID != "" {
		seen[rootID] = "root"
	}

	if err := validateTree(job.GetRoot(), "root", rootTaskKey, seen); err != nil {
		return BoundaryPlan{}, err
	}

	b := boundaryBuilder{
		rootTaskKey: rootTaskKey,
		plan: BoundaryPlan{
			UsesByTaskKey:  map[string]string{rootTaskKey: job.GetRoot().GetUses()},
			ParentByKey:    map[string]string{},
			ChildrenByKey:  map[string][]string{},
			NodePathByKey:  map[string]string{rootTaskKey: "root"},
			BoundaryNodeBy: map[string]*api.Node{rootTaskKey: job.GetRoot()},
		},
	}

	if err := b.collect(job.GetRoot(), rootTaskKey, "root"); err != nil {
		return BoundaryPlan{}, err
	}

	for i := range b.plan.Entries {
		taskKey := b.plan.Entries[i].TaskKey
		b.plan.Entries[i].ChildTaskKeys = append([]string(nil), b.plan.ChildrenByKey[taskKey]...)
	}

	return b.plan, nil
}

type boundaryBuilder struct {
	rootTaskKey string
	plan        BoundaryPlan
}

func (b *boundaryBuilder) collect(parentNode *api.Node, parentTaskKey, parentPath string) error {
	children := parentNode.GetSteps()
	if len(children) == 0 {
		return nil
	}

	if ExecutionMode(parentNode) == ExecutionDistributed {
		for i, child := range children {
			if err := b.addBoundary(child, parentTaskKey, fmt.Sprintf("%s.steps[%d]", parentPath, i)); err != nil {
				return err
			}
		}

		return nil
	}

	if IsSequence(parentNode) {
		blocked := false
		for i, child := range children {
			childPath := fmt.Sprintf("%s.steps[%d]", parentPath, i)
			if err := b.validateNode(child, childPath); err != nil {
				return err
			}

			if blocked || ContainsDistributedBoundary(child) {
				if err := b.addBoundary(child, parentTaskKey, childPath); err != nil {
					return err
				}

				blocked = true
			}
		}

		return nil
	}

	for i, child := range children {
		childPath := fmt.Sprintf("%s.steps[%d]", parentPath, i)
		if err := b.validateNode(child, childPath); err != nil {
			return err
		}

		if ContainsDistributedBoundary(child) {
			if err := b.addBoundary(child, parentTaskKey, childPath); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *boundaryBuilder) addBoundary(node *api.Node, parentTaskKey, path string) error {
	if err := b.validateNode(node, path); err != nil {
		return err
	}

	taskKey := strings.TrimSpace(node.GetId())
	uses := strings.TrimSpace(node.GetUses())
	b.plan.UsesByTaskKey[taskKey] = uses
	b.plan.ParentByKey[taskKey] = parentTaskKey
	b.plan.ChildrenByKey[parentTaskKey] = append(b.plan.ChildrenByKey[parentTaskKey], taskKey)
	b.plan.NodePathByKey[taskKey] = path
	b.plan.BoundaryNodeBy[taskKey] = node
	b.plan.Entries = append(b.plan.Entries, BoundaryEntry{
		TaskKey:       taskKey,
		ParentTaskKey: parentTaskKey,
		NodePath:      path,
		Uses:          uses,
		Node:          node,
	})

	return b.collect(node, taskKey, path)
}

func (b *boundaryBuilder) validateNode(node *api.Node, path string) error {
	if node == nil {
		return fmt.Errorf("%s is required", path)
	}

	taskKey := strings.TrimSpace(node.GetId())
	if taskKey == "" {
		return fmt.Errorf("%s.id is required", path)
	}

	if taskKey == b.rootTaskKey {
		return fmt.Errorf("%s.id %q is reserved for the root task", path, b.rootTaskKey)
	}

	if strings.TrimSpace(node.GetUses()) == "" {
		return fmt.Errorf("%s.uses is required", path)
	}

	for i, child := range node.GetSteps() {
		if child == nil {
			return fmt.Errorf("%s.steps[%d] is required", path, i)
		}

		if strings.TrimSpace(child.GetId()) == "" {
			return fmt.Errorf("%s.steps[%d].id is required", path, i)
		}
	}

	return nil
}

func validateTree(node *api.Node, path, rootTaskKey string, seen map[string]string) error {
	if node == nil {
		return fmt.Errorf("%s is required", path)
	}

	if path != "root" {
		taskKey := strings.TrimSpace(node.GetId())
		if taskKey == "" {
			return fmt.Errorf("%s.id is required", path)
		}

		if taskKey == rootTaskKey {
			return fmt.Errorf("%s.id %q is reserved for the root task", path, rootTaskKey)
		}

		if firstPath, ok := seen[taskKey]; ok {
			return fmt.Errorf("%s.id duplicates task key %q first used at %s.id", path, taskKey, firstPath)
		}

		seen[taskKey] = path
	}

	if strings.TrimSpace(node.GetUses()) == "" {
		return fmt.Errorf("%s.uses is required", path)
	}

	for i, child := range node.GetSteps() {
		if err := validateTree(child, fmt.Sprintf("%s.steps[%d]", path, i), rootTaskKey, seen); err != nil {
			return err
		}
	}

	return nil
}
