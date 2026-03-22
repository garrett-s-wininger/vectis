package resolver

import api "vectis/api/gen/go"

func grpcResolverScheme(comp api.Component) string {
	switch comp {
	case api.Component_COMPONENT_QUEUE:
		return "vectis-queue"
	case api.Component_COMPONENT_LOG:
		return "vectis-log"
	default:
		return "vectis-unknown"
	}
}
