package actionregistry

import sdkaction "vectis/sdk/action"

func SortForDisplay(descriptors []Descriptor) []Descriptor {
	return sdkaction.SortForDisplay(descriptors)
}

func sortDescriptors(descriptors []Descriptor) {
	sorted := sdkaction.SortForDisplay(descriptors)
	copy(descriptors, sorted)
}

func deduplicateDescriptors(descriptors []Descriptor) []Descriptor {
	if len(descriptors) == 0 {
		return nil
	}

	sortDescriptors(descriptors)
	out := descriptors[:0]
	seen := map[string]struct{}{}
	for _, descriptor := range descriptors {
		key := descriptor.CanonicalName + "\x00" + descriptor.Version + "\x00" + descriptor.Digest
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, descriptor)
	}

	return out
}
