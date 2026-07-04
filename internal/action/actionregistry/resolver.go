package actionregistry

type Resolver interface {
	ResolveDescriptor(uses string) (Descriptor, error)
}

type DescriptorLister interface {
	ListDescriptors() ([]Descriptor, error)
}

type Source interface {
	ResolveReference(ref Reference) (Descriptor, error)
}
