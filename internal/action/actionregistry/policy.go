package actionregistry

import (
	"fmt"
	"strings"
)

type Policy struct {
	AllowedNamespaces []string
	AllowedSources    []SourceType
	RequireDigestPins bool
}

type PolicyResolver struct {
	resolver Resolver
	policy   Policy
}

func NewPolicyResolver(resolver Resolver, policy Policy) *PolicyResolver {
	return &PolicyResolver{resolver: resolver, policy: normalizePolicy(policy)}
}

func (r *PolicyResolver) ResolveDescriptor(uses string) (Descriptor, error) {
	if r.resolver == nil {
		return Descriptor{}, fmt.Errorf("action descriptor resolver is required")
	}

	descriptor, err := r.resolver.ResolveDescriptor(uses)
	if err != nil {
		return Descriptor{}, err
	}

	ref, err := ParseReference(uses)
	if err != nil && descriptor.Source == SourceBuiltin {
		ref, err = ParseBuiltinReference(uses)
	}

	if err != nil {
		return Descriptor{}, err
	}

	if err := r.policy.Validate(ref, descriptor); err != nil {
		return Descriptor{}, err
	}

	return descriptor, nil
}

func (r *PolicyResolver) ListDescriptors() ([]Descriptor, error) {
	if r.resolver == nil {
		return nil, fmt.Errorf("action descriptor resolver is required")
	}

	lister, ok := r.resolver.(DescriptorLister)
	if !ok {
		return nil, fmt.Errorf("action descriptor resolver %T does not support listing", r.resolver)
	}

	descriptors, err := lister.ListDescriptors()
	if err != nil {
		return nil, err
	}

	out := make([]Descriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		allowed, err := r.policy.allowsListedDescriptor(descriptor)
		if err != nil {
			return nil, err
		}

		if allowed {
			out = append(out, descriptor)
		}
	}

	sortDescriptors(out)
	return out, nil
}

func (p Policy) Validate(ref Reference, descriptor Descriptor) error {
	p = normalizePolicy(p)
	if err := ValidateDescriptorUseForReference(ref, descriptor); err != nil {
		return err
	}

	if descriptor.Source == SourceBuiltin || ref.Namespace == "builtins" {
		return nil
	}

	namespace := ref.Namespace
	if namespace == "" {
		namespace = descriptorNamespace(descriptor.CanonicalName)
	}

	if len(p.AllowedNamespaces) > 0 && !stringAllowed(namespace, p.AllowedNamespaces) {
		return fmt.Errorf("action namespace %q is not allowed by policy", namespace)
	}

	if len(p.AllowedSources) > 0 && !sourceAllowed(descriptor.Source, p.AllowedSources) {
		return fmt.Errorf("action source %q is not allowed by policy", descriptor.Source)
	}

	if p.RequireDigestPins && ref.SelectorKind != SelectorDigest {
		return fmt.Errorf("action %q must be pinned by digest", ref.CanonicalName())
	}

	return nil
}

func (p Policy) allowsListedDescriptor(descriptor Descriptor) (bool, error) {
	p = normalizePolicy(p)
	if err := ValidateDescriptorStatus(descriptor.Status); err != nil {
		return false, err
	}

	if descriptor.LifecycleStatus() != DescriptorStatusActive {
		return false, nil
	}

	if descriptor.Source == SourceBuiltin {
		return true, nil
	}

	namespace := descriptorNamespace(descriptor.CanonicalName)
	if namespace == "" {
		return false, fmt.Errorf("action descriptor %q does not use namespace/name", descriptor.CanonicalName)
	}

	if len(p.AllowedNamespaces) > 0 && !stringAllowed(namespace, p.AllowedNamespaces) {
		return false, nil
	}

	if len(p.AllowedSources) > 0 && !sourceAllowed(descriptor.Source, p.AllowedSources) {
		return false, nil
	}

	return true, nil
}

func normalizePolicy(in Policy) Policy {
	return Policy{
		AllowedNamespaces: cleanStrings(in.AllowedNamespaces),
		AllowedSources:    cleanSources(in.AllowedSources),
		RequireDigestPins: in.RequireDigestPins,
	}
}

func descriptorNamespace(canonicalName string) string {
	namespace, _, ok := strings.Cut(canonicalName, "/")
	if !ok {
		return ""
	}

	return namespace
}

func stringAllowed(value string, allowed []string) bool {
	for _, item := range allowed {
		if item == value {
			return true
		}
	}

	return false
}

func sourceAllowed(value SourceType, allowed []SourceType) bool {
	for _, item := range allowed {
		if item == value {
			return true
		}
	}

	return false
}

func cleanStrings(in []string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, value := range in {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}

		if _, ok := seen[value]; ok {
			continue
		}

		seen[value] = struct{}{}
		out = append(out, value)
	}

	return out
}

func cleanSources(in []SourceType) []SourceType {
	out := []SourceType{}
	seen := map[SourceType]struct{}{}
	for _, value := range in {
		value = SourceType(strings.TrimSpace(string(value)))
		if value == "" {
			continue
		}

		if _, ok := seen[value]; ok {
			continue
		}

		seen[value] = struct{}{}
		out = append(out, value)
	}

	return out
}
