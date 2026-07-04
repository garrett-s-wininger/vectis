package config

import (
	"fmt"
	"strings"

	"vectis/internal/action/actionregistry"

	"github.com/spf13/viper"
)

const envActionRegistryLocalRoots = "VECTIS_ACTION_REGISTRY_LOCAL_ROOTS"
const envActionRegistryAllowedNamespaces = "VECTIS_ACTION_REGISTRY_ALLOWED_NAMESPACES"
const envActionRegistryAllowedSources = "VECTIS_ACTION_REGISTRY_ALLOWED_SOURCES"
const envActionRegistryRequireDigestPins = "VECTIS_ACTION_REGISTRY_REQUIRE_DIGEST_PINS"

func init() {
	_ = viper.BindEnv("action_registry.local_roots", envActionRegistryLocalRoots)
	_ = viper.BindEnv("action_registry.allowed_namespaces", envActionRegistryAllowedNamespaces)
	_ = viper.BindEnv("action_registry.allowed_sources", envActionRegistryAllowedSources)
	_ = viper.BindEnv("action_registry.require_digest_pins", envActionRegistryRequireDigestPins)
}

func ActionRegistryLocalRoots() []string {
	return coalesceStringSlices(
		stringSliceFromViper("action_registry.local_roots"),
		MustDefaults().ActionRegistry.LocalRoots,
	)
}

func ActionRegistryAllowedNamespaces() []string {
	return coalesceStringSlices(
		stringSliceFromViper("action_registry.allowed_namespaces"),
		MustDefaults().ActionRegistry.AllowedNamespaces,
	)
}

func ActionRegistryAllowedSources() ([]actionregistry.SourceType, error) {
	raw := coalesceStringSlices(
		stringSliceFromViper("action_registry.allowed_sources"),
		MustDefaults().ActionRegistry.AllowedSources,
	)

	sources := make([]actionregistry.SourceType, 0, len(raw))
	for _, value := range raw {
		source := actionregistry.SourceType(strings.TrimSpace(value))
		if source == "" {
			continue
		}

		switch source {
		case actionregistry.SourceBuiltin, actionregistry.SourceLocalFilesystem, actionregistry.SourceOCI:
			sources = append(sources, source)
		default:
			return nil, fmt.Errorf("action_registry.allowed_sources contains unsupported source %q", source)
		}
	}

	return sources, nil
}

func ActionRegistryRequireDigestPins() bool {
	if viper.IsSet("action_registry.require_digest_pins") {
		return viper.GetBool("action_registry.require_digest_pins")
	}

	return MustDefaults().ActionRegistry.RequireDigestPins
}

func ActionRegistryPolicy() (actionregistry.Policy, error) {
	sources, err := ActionRegistryAllowedSources()
	if err != nil {
		return actionregistry.Policy{}, err
	}

	return actionregistry.Policy{
		AllowedNamespaces: ActionRegistryAllowedNamespaces(),
		AllowedSources:    sources,
		RequireDigestPins: ActionRegistryRequireDigestPins(),
	}, nil
}
