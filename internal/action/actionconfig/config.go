package actionconfig

import (
	"fmt"

	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/action/builtins"
	"vectis/internal/config"
	"vectis/internal/job"
)

func DescriptorResolver() (actionregistry.Resolver, error) {
	resolver, err := UnrestrictedDescriptorResolver()
	if err != nil {
		return nil, err
	}

	policy, err := config.ActionRegistryPolicy()
	if err != nil {
		return nil, err
	}

	return actionregistry.NewPolicyResolver(resolver, policy), nil
}

func UnrestrictedDescriptorResolver() (actionregistry.Resolver, error) {
	resolvers := []actionregistry.Resolver{builtins.NewRegistry()}
	for _, root := range config.ActionRegistryLocalRoots() {
		source, err := actionregistry.NewLocalManifestSource(root)
		if err != nil {
			return nil, fmt.Errorf("action_registry.local_roots %q: %w", root, err)
		}

		resolvers = append(resolvers, source)
	}

	return actionregistry.NewCompositeResolver(resolvers...), nil
}

func Resolver() (action.Resolver, error) {
	descriptors, err := DescriptorResolver()
	if err != nil {
		return nil, err
	}

	return job.NewActionResolver(descriptors, nil)
}
