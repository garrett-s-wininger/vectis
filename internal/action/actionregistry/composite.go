package actionregistry

import (
	"fmt"
	"strings"
)

type CompositeResolver struct {
	resolvers []Resolver
}

func NewCompositeResolver(resolvers ...Resolver) *CompositeResolver {
	out := &CompositeResolver{}
	for _, resolver := range resolvers {
		if resolver != nil {
			out.resolvers = append(out.resolvers, resolver)
		}
	}

	return out
}

func (r *CompositeResolver) ResolveDescriptor(uses string) (Descriptor, error) {
	if len(r.resolvers) == 0 {
		return Descriptor{}, fmt.Errorf("no action descriptor resolvers configured")
	}

	errs := make([]string, 0, len(r.resolvers))
	for _, resolver := range r.resolvers {
		descriptor, err := resolver.ResolveDescriptor(uses)
		if err == nil {
			return descriptor, nil
		}

		errs = append(errs, err.Error())
	}

	return Descriptor{}, fmt.Errorf("resolve action %q: %s", uses, strings.Join(errs, "; "))
}

func (r *CompositeResolver) ListDescriptors() ([]Descriptor, error) {
	if len(r.resolvers) == 0 {
		return nil, fmt.Errorf("no action descriptor resolvers configured")
	}

	descriptors := []Descriptor{}
	for _, resolver := range r.resolvers {
		lister, ok := resolver.(DescriptorLister)
		if !ok {
			return nil, fmt.Errorf("action descriptor resolver %T does not support listing", resolver)
		}

		listed, err := lister.ListDescriptors()
		if err != nil {
			return nil, err
		}

		descriptors = append(descriptors, listed...)
	}

	return deduplicateDescriptors(descriptors), nil
}
