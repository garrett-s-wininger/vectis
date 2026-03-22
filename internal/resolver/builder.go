package resolver

import (
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/registry"

	"google.golang.org/grpc/resolver"
)

func BuildResolver(comp api.Component, regClient *registry.Registry, logger interfaces.Logger) resolver.Builder {
	if pinned := pinnedAddress(comp); pinned != "" {
		logger.Debug("resolver: using pinned address for %s: %s", comp.String(), pinned)
		return &staticBuilder{addr: pinned, logger: logger}
	}

	logger.Debug("resolver: using registry resolution for %s", comp.String())
	return &registryBuilder{reg: regClient, comp: comp, logger: logger}
}

func pinnedAddress(comp api.Component) string {
	switch comp {
	case api.Component_COMPONENT_QUEUE:
		return config.PinnedQueueAddress()
	case api.Component_COMPONENT_LOG:
		return config.PinnedLogAddress()
	}
	return ""
}

func BuildTarget(comp api.Component) string {
	pinned := pinnedAddress(comp)
	scheme := grpcResolverScheme(comp)
	if pinned != "" {
		scheme = "static"
	}
	return fmt.Sprintf("%s:///%s", scheme, pinned)
}
