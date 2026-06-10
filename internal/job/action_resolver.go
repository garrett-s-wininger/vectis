package job

import (
	"fmt"
	"strings"

	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/action/builtins"
	"vectis/internal/action/custom"
	"vectis/internal/interfaces"
)

type executableActionResolver struct {
	builtins        *builtins.Registry
	descriptors     actionregistry.Resolver
	frozenByUses    map[string]actionregistry.Descriptor
	ambiguousFrozen map[string]struct{}
	processExecutor interfaces.ExecExecutor
}

func NewActionResolver(descriptorResolver actionregistry.Resolver, processExecutor interfaces.ExecExecutor) (action.Resolver, error) {
	return newExecutableActionResolver(builtins.NewRegistry(), descriptorResolver, nil, processExecutor)
}

func newExecutableActionResolver(
	builtinRegistry *builtins.Registry,
	descriptorResolver actionregistry.Resolver,
	locks []actionregistry.ActionLock,
	processExecutor interfaces.ExecExecutor,
) (action.Resolver, error) {
	if builtinRegistry == nil {
		builtinRegistry = builtins.NewRegistry()
	}

	if descriptorResolver == nil {
		descriptorResolver = builtinRegistry
	}

	resolver := &executableActionResolver{
		builtins:        builtinRegistry,
		descriptors:     descriptorResolver,
		frozenByUses:    make(map[string]actionregistry.Descriptor),
		ambiguousFrozen: make(map[string]struct{}),
		processExecutor: processExecutor,
	}

	for _, lock := range locks {
		uses := strings.TrimSpace(lock.Uses)
		if uses == "" {
			continue
		}

		descriptor := lock.Descriptor
		if existing, ok := resolver.frozenByUses[uses]; ok {
			if existing.Digest != descriptor.Digest {
				resolver.ambiguousFrozen[uses] = struct{}{}
			}

			continue
		}

		resolver.frozenByUses[uses] = descriptor
	}

	return resolver, nil
}

func (r *executableActionResolver) Resolve(uses string) (action.Node, error) {
	if node, err := r.builtins.Resolve(uses); err == nil {
		return node, nil
	}

	descriptor, frozen, err := r.descriptorForUses(uses)
	if err != nil {
		return nil, err
	}

	if frozen {
		if err := actionregistry.ValidateFrozenDescriptorExecution(uses, descriptor); err != nil {
			return nil, err
		}
	} else if err := actionregistry.ValidateDescriptorUse(uses, descriptor); err != nil {
		return nil, err
	}

	switch descriptor.Runtime {
	case actionregistry.RuntimeProcess:
		return custom.NewProcessAction(descriptor, r.processExecutor), nil
	case actionregistry.RuntimeBuiltin:
		return nil, fmt.Errorf("builtin action %q was not registered", uses)
	default:
		return nil, fmt.Errorf("action %q runtime %q is not executable by this worker", uses, descriptor.Runtime)
	}
}

func (r *executableActionResolver) descriptorForUses(uses string) (actionregistry.Descriptor, bool, error) {
	uses = strings.TrimSpace(uses)
	if _, ok := r.ambiguousFrozen[uses]; ok {
		return actionregistry.Descriptor{}, false, fmt.Errorf("multiple frozen descriptors for action reference %q", uses)
	}

	if frozen, ok := r.frozenByUses[uses]; ok {
		return r.enrichFrozenDescriptor(uses, frozen), true, nil
	}

	if r.descriptors == nil {
		return actionregistry.Descriptor{}, false, fmt.Errorf("no action descriptor resolver configured")
	}

	descriptor, err := r.descriptors.ResolveDescriptor(uses)
	return descriptor, false, err
}

func (r *executableActionResolver) enrichFrozenDescriptor(uses string, frozen actionregistry.Descriptor) actionregistry.Descriptor {
	if r.descriptors == nil {
		return frozen
	}

	current, err := r.descriptors.ResolveDescriptor(uses)
	if err != nil {
		return frozen
	}

	if current.CanonicalName == frozen.CanonicalName &&
		current.Source == frozen.Source &&
		current.Runtime == frozen.Runtime &&
		current.Version == frozen.Version &&
		current.Digest == frozen.Digest {
		frozen.SourcePath = current.SourcePath
	}

	return frozen
}
