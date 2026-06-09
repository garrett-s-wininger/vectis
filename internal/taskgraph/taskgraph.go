package taskgraph

import (
	"fmt"
	"sort"
	"strings"

	api "vectis/api/gen/go"
)

const (
	ExecutionField       = "execution"
	ExecutionLocal       = "local"
	ExecutionDistributed = "distributed"
	StepsPort            = "steps"
	BranchesPort         = "branches"
	ConditionPort        = "condition"
	ThenPort             = "then"
	ElsePort             = "else"
	BodyPort             = "body"
	AlwaysPort           = "always"
	ChoicesPort          = "choices"
)

type ChildRef struct {
	PortName string
	Index    int
	Path     string
	Node     *api.Node
}

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

func PrimaryPortName(node *api.Node) string {
	switch NormalizeUses(node.GetUses()) {
	case "builtins/parallel":
		return BranchesPort
	default:
		return StepsPort
	}
}

func ExplicitPortChildren(node *api.Node, portName string) []*api.Node {
	if node == nil {
		return nil
	}

	port, ok := node.GetPorts()[portName]
	if !ok {
		return nil
	}

	return port.GetNodes()
}

func HasExplicitPort(node *api.Node, portName string) bool {
	if node == nil {
		return false
	}

	_, ok := node.GetPorts()[portName]
	return ok
}

func ChildPorts(node *api.Node) map[string][]*api.Node {
	if node == nil {
		return nil
	}

	out := make(map[string][]*api.Node, len(node.GetPorts())+1)
	if steps := node.GetSteps(); len(steps) > 0 {
		primary := PrimaryPortName(node)
		out[primary] = append(out[primary], steps...)
	}

	for _, portName := range sortedPortNames(node) {
		nodes := ExplicitPortChildren(node, portName)
		if len(nodes) == 0 {
			if _, exists := out[portName]; !exists {
				out[portName] = nil
			}
			continue
		}

		out[portName] = append(out[portName], nodes...)
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func PrimaryChildren(node *api.Node) []*api.Node {
	if node == nil {
		return nil
	}

	return ChildPorts(node)[PrimaryPortName(node)]
}

func AllChildren(node *api.Node) []*api.Node {
	refs := ChildRefs(node, "")
	children := make([]*api.Node, 0, len(refs))
	for _, ref := range refs {
		children = append(children, ref.Node)
	}

	return children
}

func HasChildren(node *api.Node) bool {
	return len(AllChildren(node)) > 0
}

func ChildRefs(node *api.Node, parentPath string) []ChildRef {
	if node == nil {
		return nil
	}

	var refs []ChildRef
	primary := PrimaryPortName(node)
	for i, child := range node.GetSteps() {
		refs = append(refs, ChildRef{
			PortName: primary,
			Index:    i,
			Path:     childPath(parentPath, fmt.Sprintf("steps[%d]", i)),
			Node:     child,
		})
	}

	for _, portName := range sortedPortNames(node) {
		nodes := ExplicitPortChildren(node, portName)
		for i, child := range nodes {
			refs = append(refs, ChildRef{
				PortName: portName,
				Index:    i,
				Path:     childPath(parentPath, fmt.Sprintf("ports.%s.nodes[%d]", portName, i)),
				Node:     child,
			})
		}
	}

	return refs
}

func ValidatePortLayout(node *api.Node, path string) error {
	if node == nil {
		return nil
	}

	primary := PrimaryPortName(node)
	if len(node.GetSteps()) > 0 && HasExplicitPort(node, primary) {
		return fmt.Errorf("%s.steps cannot be used together with %s.ports.%s", path, path, primary)
	}

	return nil
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

func sortedPortNames(node *api.Node) []string {
	if node == nil || len(node.GetPorts()) == 0 {
		return nil
	}

	names := make([]string, 0, len(node.GetPorts()))
	for name := range node.GetPorts() {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func childPath(parentPath, suffix string) string {
	if parentPath == "" {
		return suffix
	}

	return parentPath + "." + suffix
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

	for _, ref := range ChildRefs(node, "") {
		if ContainsDistributedBoundary(ref.Node) {
			return true
		}
	}

	return false
}

func LocalChildrenForTask(node *api.Node) []*api.Node {
	return LocalPortsForTask(node)[PrimaryPortName(node)]
}

func LocalPortsForTask(node *api.Node) map[string][]*api.Node {
	if node == nil || ExecutionMode(node) == ExecutionDistributed {
		return nil
	}

	ports := ChildPorts(node)
	if len(ports) == 0 {
		return nil
	}

	if !IsSequence(node) {
		localPorts := make(map[string][]*api.Node, len(ports))
		for portName, children := range ports {
			for _, child := range children {
				if ContainsDistributedBoundary(child) {
					continue
				}

				localPorts[portName] = append(localPorts[portName], child)
			}

			if len(localPorts[portName]) == 0 {
				delete(localPorts, portName)
			}
		}

		if len(localPorts) == 0 {
			return nil
		}

		return localPorts
	}

	primary := PrimaryPortName(node)
	children := ports[primary]
	localPorts := make(map[string][]*api.Node, 1)
	for _, child := range children {
		if ContainsDistributedBoundary(child) {
			break
		}

		localPorts[primary] = append(localPorts[primary], child)
	}

	if len(localPorts[primary]) == 0 {
		return nil
	}

	return localPorts
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
	refs := ChildRefs(parentNode, parentPath)
	if len(refs) == 0 {
		return nil
	}

	if ExecutionMode(parentNode) == ExecutionDistributed {
		for _, ref := range refs {
			if err := b.addBoundary(ref.Node, parentTaskKey, ref.Path); err != nil {
				return err
			}
		}

		return nil
	}

	if IsSequence(parentNode) {
		blocked := false
		for _, ref := range refs {
			if ref.PortName != PrimaryPortName(parentNode) {
				if err := b.validateNode(ref.Node, ref.Path); err != nil {
					return err
				}

				if ContainsDistributedBoundary(ref.Node) {
					if err := b.addBoundary(ref.Node, parentTaskKey, ref.Path); err != nil {
						return err
					}
				}

				continue
			}

			if err := b.validateNode(ref.Node, ref.Path); err != nil {
				return err
			}

			if blocked || ContainsDistributedBoundary(ref.Node) {
				if err := b.addBoundary(ref.Node, parentTaskKey, ref.Path); err != nil {
					return err
				}

				blocked = true
			}
		}

		return nil
	}

	for _, ref := range refs {
		if err := b.validateNode(ref.Node, ref.Path); err != nil {
			return err
		}

		if ContainsDistributedBoundary(ref.Node) {
			if err := b.addBoundary(ref.Node, parentTaskKey, ref.Path); err != nil {
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
	if err := ValidatePortLayout(node, path); err != nil {
		return err
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

	for _, ref := range ChildRefs(node, path) {
		if ref.Node == nil {
			return fmt.Errorf("%s is required", ref.Path)
		}

		if strings.TrimSpace(ref.Node.GetId()) == "" {
			return fmt.Errorf("%s.id is required", ref.Path)
		}
	}

	return nil
}

func validateTree(node *api.Node, path, rootTaskKey string, seen map[string]string) error {
	if node == nil {
		return fmt.Errorf("%s is required", path)
	}
	if err := ValidatePortLayout(node, path); err != nil {
		return err
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

	for _, ref := range ChildRefs(node, path) {
		if err := validateTree(ref.Node, ref.Path, rootTaskKey, seen); err != nil {
			return err
		}
	}

	return nil
}
