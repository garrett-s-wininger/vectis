package job

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/action/actionregistry"
	"vectis/internal/dal"
	"vectis/internal/taskgraph"
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
	ActionDigests map[string]string `json:"action_digests,omitempty"`
}

func PlanTaskExecutions(job *api.Job) ([]TaskPlanEntry, error) {
	return PlanTaskExecutionsWithActions(job, nil)
}

func PlanTaskExecutionsWithActions(job *api.Job, resolver actionregistry.Resolver) ([]TaskPlanEntry, error) {
	if job == nil {
		return nil, fmt.Errorf("job is required")
	}

	if job.GetRoot() == nil {
		return nil, fmt.Errorf("job root is required")
	}

	boundaries, err := taskgraph.PlanTaskBoundaries(job, dal.RootTaskKey)
	if err != nil {
		return nil, err
	}

	actionDigests, err := taskActionDigests(job, resolver)
	if err != nil {
		return nil, err
	}

	entries := make([]TaskPlanEntry, 0, len(boundaries.Entries))
	for _, boundary := range boundaries.Entries {
		specHash, err := taskSpecHash(boundary.Node, boundary.TaskKey, boundary.Uses, boundary.ChildTaskKeys, actionDigests)
		if err != nil {
			return nil, err
		}

		entries = append(entries, TaskPlanEntry{
			TaskKey:       boundary.TaskKey,
			Name:          boundary.TaskKey,
			ParentTaskKey: boundary.ParentTaskKey,
			NodeID:        boundary.TaskKey,
			NodePath:      boundary.NodePath,
			Uses:          boundary.Uses,
			ChildTaskKeys: append([]string(nil), boundary.ChildTaskKeys...),
			SpecHash:      specHash,
		})
	}

	return entries, nil
}

func taskSpecHash(node *api.Node, nodeID, uses string, childTaskKeys []string, actionDigests map[string]string) (string, error) {
	spec := canonicalTaskSpec{
		NodeID:        nodeID,
		Uses:          uses,
		With:          cloneStringMap(node.GetWith()),
		ChildTaskKeys: append([]string(nil), childTaskKeys...),
		ActionDigests: cloneStringMap(actionDigests),
	}

	payload, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("marshal task spec: %w", err)
	}

	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func taskActionDigests(job *api.Job, resolver actionregistry.Resolver) (map[string]string, error) {
	if resolver == nil {
		return map[string]string{}, nil
	}

	locks, err := actionregistry.ResolveJobActions(job, resolver)
	if err != nil {
		return nil, err
	}

	if len(locks) == 0 {
		return map[string]string{}, nil
	}

	out := make(map[string]string, len(locks))
	for _, lock := range locks {
		out[lock.NodePath] = lock.Descriptor.Digest
	}

	return out, nil
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
