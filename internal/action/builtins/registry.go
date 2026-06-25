package builtins

import (
	"fmt"

	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
)

type Registry struct {
	nodes map[string]action.Node
}

type RegistryOption func(*registryOptions)

type registryOptions struct {
	checkoutCache action.CheckoutCache
}

var _ actionregistry.Resolver = (*Registry)(nil)
var _ actionregistry.DescriptorLister = (*Registry)(nil)

func WithCheckoutCache(cache action.CheckoutCache) RegistryOption {
	return func(opts *registryOptions) {
		opts.checkoutCache = cache
	}
}

func NewRegistry(options ...RegistryOption) *Registry {
	var opts registryOptions
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}

	r := &Registry{
		nodes: make(map[string]action.Node),
	}

	r.Register(NewScriptAction(nil))
	r.Register(NewTestAction(nil))
	r.Register(NewCheckoutAction(nil, opts.checkoutCache))
	r.Register(NewGerritReviewAction(nil))
	r.Register(&UploadArtifactAction{})
	r.Register(&SequenceNode{})
	r.Register(&ParallelNode{})
	r.Register(&IfNode{})
	r.Register(&RetryNode{})
	r.Register(&TimeoutNode{})
	r.Register(&FinallyNode{})
	r.Register(&FallbackNode{})
	r.Register(&ResultAction{})

	return r
}

func (r *Registry) Register(n action.Node) {
	r.nodes[n.Type()] = n
}

func (r *Registry) Resolve(uses string) (action.Node, error) {
	ref, err := actionregistry.ParseBuiltinReference(uses)
	if err != nil {
		return nil, err
	}

	n, ok := r.nodes[ref.CanonicalName()]
	if !ok {
		return nil, fmt.Errorf("unknown action: %s", uses)
	}

	if ref.Selector != "" {
		descriptor, err := actionregistry.DescriptorFromNode(n, actionregistry.SourceBuiltin, actionregistry.RuntimeBuiltin)
		if err != nil {
			return nil, err
		}

		if err := descriptor.MatchReference(ref); err != nil {
			return nil, err
		}
	}

	return n, nil
}

func (r *Registry) IsBuiltin(uses string) bool {
	_, err := r.Resolve(uses)
	return err == nil
}

func (r *Registry) ResolveDescriptor(uses string) (actionregistry.Descriptor, error) {
	ref, err := actionregistry.ParseBuiltinReference(uses)
	if err != nil {
		return actionregistry.Descriptor{}, err
	}

	n, ok := r.nodes[ref.CanonicalName()]
	if !ok {
		return actionregistry.Descriptor{}, fmt.Errorf("unknown action: %s", uses)
	}

	descriptor, err := actionregistry.DescriptorFromNode(n, actionregistry.SourceBuiltin, actionregistry.RuntimeBuiltin)
	if err != nil {
		return actionregistry.Descriptor{}, err
	}

	if err := descriptor.MatchReference(ref); err != nil {
		return actionregistry.Descriptor{}, err
	}

	return descriptor, nil
}

func (r *Registry) ListDescriptors() ([]actionregistry.Descriptor, error) {
	descriptors := make([]actionregistry.Descriptor, 0, len(r.nodes))
	for _, n := range r.nodes {
		descriptor, err := actionregistry.DescriptorFromNode(n, actionregistry.SourceBuiltin, actionregistry.RuntimeBuiltin)
		if err != nil {
			return nil, err
		}

		descriptors = append(descriptors, descriptor)
	}

	return actionregistry.SortForDisplay(descriptors), nil
}
