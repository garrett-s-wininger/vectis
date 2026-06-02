package job

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
)

type TaskPlanEntry struct {
	TaskKey       string
	Name          string
	ParentTaskKey string
	NodeID        string
	NodePath      string
	Uses          string
	ChildTaskKeys []string
	SpecHash      string
}

type canonicalTaskSpec struct {
	NodeID        string            `json:"node_id"`
	Uses          string            `json:"uses"`
	With          map[string]string `json:"with,omitempty"`
	ChildTaskKeys []string          `json:"child_task_keys,omitempty"`
}

func PlanTaskExecutions(job *api.Job) ([]TaskPlanEntry, error) {
	if job == nil {
		return nil, fmt.Errorf("job is required")
	}

	if job.GetRoot() == nil {
		return nil, fmt.Errorf("job root is required")
	}

	planner := taskPlanner{
		seen: map[string]string{},
	}

	if rootID := strings.TrimSpace(job.GetRoot().GetId()); rootID != "" {
		planner.seen[rootID] = "root"
	}

	for i, child := range job.GetRoot().GetSteps() {
		if err := planner.walk(child, fmt.Sprintf("root.steps[%d]", i), dal.RootTaskKey); err != nil {
			return nil, err
		}
	}

	return planner.entries, nil
}

type taskPlanner struct {
	seen    map[string]string
	entries []TaskPlanEntry
}

func (p *taskPlanner) walk(node *api.Node, path, parentTaskKey string) error {
	if node == nil {
		return fmt.Errorf("%s is required", path)
	}

	taskKey := strings.TrimSpace(node.GetId())
	if taskKey == "" {
		return fmt.Errorf("%s.id is required", path)
	}

	if taskKey == dal.RootTaskKey {
		return fmt.Errorf("%s.id %q is reserved for the root task", path, dal.RootTaskKey)
	}

	if firstPath, ok := p.seen[taskKey]; ok {
		return fmt.Errorf("%s.id duplicates task key %q first used at %s.id", path, taskKey, firstPath)
	}
	p.seen[taskKey] = path

	uses := strings.TrimSpace(node.GetUses())
	if uses == "" {
		return fmt.Errorf("%s.uses is required", path)
	}

	childKeys := make([]string, 0, len(node.GetSteps()))
	for i, child := range node.GetSteps() {
		childPath := fmt.Sprintf("%s.steps[%d]", path, i)
		if child == nil {
			return fmt.Errorf("%s is required", childPath)
		}

		childKey := strings.TrimSpace(child.GetId())
		if childKey == "" {
			return fmt.Errorf("%s.id is required", childPath)
		}
		childKeys = append(childKeys, childKey)
	}

	specHash, err := taskSpecHash(node, taskKey, uses, childKeys)
	if err != nil {
		return err
	}

	p.entries = append(p.entries, TaskPlanEntry{
		TaskKey:       taskKey,
		Name:          taskKey,
		ParentTaskKey: parentTaskKey,
		NodeID:        taskKey,
		NodePath:      path,
		Uses:          uses,
		ChildTaskKeys: childKeys,
		SpecHash:      specHash,
	})

	for i, child := range node.GetSteps() {
		if err := p.walk(child, fmt.Sprintf("%s.steps[%d]", path, i), taskKey); err != nil {
			return err
		}
	}

	return nil
}

func taskSpecHash(node *api.Node, nodeID, uses string, childTaskKeys []string) (string, error) {
	spec := canonicalTaskSpec{
		NodeID:        nodeID,
		Uses:          uses,
		With:          cloneStringMap(node.GetWith()),
		ChildTaskKeys: append([]string(nil), childTaskKeys...),
	}

	payload, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("marshal task spec: %w", err)
	}

	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}

	return out
}
