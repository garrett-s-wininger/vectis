package actionregistry

import "sort"

func SortForDisplay(descriptors []Descriptor) []Descriptor {
	out := append([]Descriptor(nil), descriptors...)
	sortDescriptors(out)
	return out
}

func sortDescriptors(descriptors []Descriptor) {
	sort.SliceStable(descriptors, func(i, j int) bool {
		if descriptors[i].CanonicalName != descriptors[j].CanonicalName {
			return descriptors[i].CanonicalName < descriptors[j].CanonicalName
		}

		if descriptors[i].Version != descriptors[j].Version {
			return descriptors[i].Version < descriptors[j].Version
		}

		if descriptors[i].Source != descriptors[j].Source {
			return descriptors[i].Source < descriptors[j].Source
		}

		if descriptors[i].Runtime != descriptors[j].Runtime {
			return descriptors[i].Runtime < descriptors[j].Runtime
		}

		return descriptors[i].Digest < descriptors[j].Digest
	})
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
