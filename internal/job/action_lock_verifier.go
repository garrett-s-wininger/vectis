package job

import (
	"errors"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
)

type actionLockVerifier struct {
	resolver actionregistry.Resolver
	locks    map[string]actionregistry.ActionLock
}

var _ action.ActionVerifier = (*actionLockVerifier)(nil)

func newActionLockVerifier(resolver actionregistry.Resolver, locks []actionregistry.ActionLock) (action.ActionVerifier, error) {
	if len(locks) == 0 {
		return nil, nil
	}

	if resolver == nil {
		return nil, fmt.Errorf("action descriptor resolver is required")
	}

	if err := actionregistry.ValidateActionLocks(locks); err != nil {
		return nil, err
	}

	verifier := &actionLockVerifier{
		resolver: resolver,
		locks:    make(map[string]actionregistry.ActionLock, len(locks)),
	}

	for _, lock := range locks {
		nodePath := strings.TrimSpace(lock.NodePath)
		if nodePath == "" {
			return nil, fmt.Errorf("action lock is missing node_path")
		}

		if _, exists := verifier.locks[nodePath]; exists {
			return nil, fmt.Errorf("duplicate frozen action lock for node path %q", nodePath)
		}

		verifier.locks[nodePath] = lock
	}

	return verifier, nil
}

func (v *actionLockVerifier) VerifyAction(node *api.Node, path string) error {
	if node == nil {
		return fmt.Errorf("nil node")
	}

	nodePath := strings.TrimSpace(path)
	if nodePath == "" {
		return fmt.Errorf("node path is required for frozen action verification")
	}

	lock, ok := v.locks[nodePath]
	if !ok {
		return fmt.Errorf("missing frozen action lock for node path %q", nodePath)
	}

	if lock.NodeID != "" && node.GetId() != "" && lock.NodeID != node.GetId() {
		return fmt.Errorf("node %q id changed: got %q, frozen %q", nodePath, node.GetId(), lock.NodeID)
	}

	uses := strings.TrimSpace(node.GetUses())
	if uses != lock.Uses {
		return fmt.Errorf("node %q action reference changed: got %q, frozen %q", nodePath, uses, lock.Uses)
	}

	descriptor, err := v.resolver.ResolveDescriptor(uses)
	if err != nil {
		var statusErr *actionregistry.DescriptorStatusError
		if !errors.As(err, &statusErr) {
			return nil
		}

		descriptor = statusErr.Descriptor
	}

	frozen := lock.Descriptor
	if descriptor.CanonicalName != frozen.CanonicalName {
		return fmt.Errorf("node %q resolved action name changed: got %q, frozen %q", nodePath, descriptor.CanonicalName, frozen.CanonicalName)
	}

	if descriptor.Source != frozen.Source {
		return fmt.Errorf("node %q resolved action source changed: got %q, frozen %q", nodePath, descriptor.Source, frozen.Source)
	}

	if descriptor.Runtime != frozen.Runtime {
		return fmt.Errorf("node %q resolved action runtime changed: got %q, frozen %q", nodePath, descriptor.Runtime, frozen.Runtime)
	}

	if descriptor.Version != frozen.Version {
		return fmt.Errorf("node %q resolved action version changed: got %q, frozen %q", nodePath, descriptor.Version, frozen.Version)
	}

	if descriptor.Digest != frozen.Digest {
		return fmt.Errorf("node %q resolved action digest changed: got %q, frozen %q", nodePath, descriptor.Digest, frozen.Digest)
	}

	if err := actionregistry.ValidateFrozenDescriptorExecution(uses, descriptor); err != nil {
		return fmt.Errorf("node %q frozen action unavailable: %w", nodePath, err)
	}

	return nil
}
