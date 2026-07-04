package actionregistry

import (
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/taskgraph"
)

type ActionLock struct {
	NodeID     string     `json:"node_id,omitempty"`
	NodePath   string     `json:"node_path"`
	Uses       string     `json:"uses"`
	Descriptor Descriptor `json:"descriptor"`
}

func CloneActionLocks(in []ActionLock) []ActionLock {
	if len(in) == 0 {
		return nil
	}

	out := make([]ActionLock, len(in))
	for i, lock := range in {
		out[i] = lock
		out[i].Descriptor = cloneDescriptor(lock.Descriptor)
	}

	return out
}

func ResolveJobActions(job *api.Job, resolver Resolver) ([]ActionLock, error) {
	if resolver == nil {
		return nil, fmt.Errorf("action descriptor resolver is required")
	}

	if job == nil {
		return nil, fmt.Errorf("job is required")
	}

	if job.GetRoot() == nil {
		return nil, fmt.Errorf("job root is required")
	}

	locks := []ActionLock{}
	if err := resolveNodeActions(job.GetRoot(), "root", resolver, &locks); err != nil {
		return nil, err
	}

	return locks, nil
}

func ValidateActionLocks(locks []ActionLock) error {
	for i, lock := range locks {
		if strings.TrimSpace(lock.NodePath) == "" {
			return fmt.Errorf("action_locks[%d].node_path is required", i)
		}

		if strings.TrimSpace(lock.Uses) == "" {
			return fmt.Errorf("action_locks[%d].uses is required", i)
		}

		if strings.TrimSpace(lock.Descriptor.CanonicalName) == "" {
			return fmt.Errorf("action_locks[%d].descriptor.canonical_name is required", i)
		}

		if strings.TrimSpace(lock.Descriptor.Version) == "" {
			return fmt.Errorf("action_locks[%d].descriptor.version is required", i)
		}

		if strings.TrimSpace(lock.Descriptor.Digest) == "" {
			return fmt.Errorf("action_locks[%d].descriptor.digest is required", i)
		}

		if lock.Descriptor.Source == "" {
			return fmt.Errorf("action_locks[%d].descriptor.source is required", i)
		}

		if lock.Descriptor.Runtime == "" {
			return fmt.Errorf("action_locks[%d].descriptor.runtime is required", i)
		}
	}

	return nil
}

func resolveNodeActions(node *api.Node, path string, resolver Resolver, locks *[]ActionLock) error {
	if node == nil {
		return fmt.Errorf("%s is required", path)
	}

	uses := strings.TrimSpace(node.GetUses())
	if uses == "" {
		return fmt.Errorf("%s.uses is required", path)
	}

	descriptor, err := resolver.ResolveDescriptor(uses)
	if err != nil {
		return fmt.Errorf("%s.uses: resolve action %q: %w", path, uses, err)
	}

	*locks = append(*locks, ActionLock{
		NodeID:     strings.TrimSpace(node.GetId()),
		NodePath:   path,
		Uses:       uses,
		Descriptor: cloneDescriptor(descriptor),
	})

	for _, ref := range taskgraph.ChildRefs(node, path) {
		if err := resolveNodeActions(ref.Node, ref.Path, resolver, locks); err != nil {
			return err
		}
	}

	return nil
}

func cloneDescriptor(in Descriptor) Descriptor {
	out := in
	out.RuntimeConfig = cloneStringMap(in.RuntimeConfig)
	out.InputSchema.Fields = append([]InputField(nil), in.InputSchema.Fields...)
	out.PortSchema = append([]PortSpec(nil), in.PortSchema...)
	out.Capabilities = append([]Capability(nil), in.Capabilities...)

	return out
}
