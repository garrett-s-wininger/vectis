package config

import (
	"reflect"
	"strings"
	"testing"

	"vectis/internal/action/actionregistry"

	"github.com/spf13/viper"
)

func TestActionRegistryLocalRootsFromViper(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("action_registry.local_roots", []string{
		"/opt/vectis/actions, /srv/vectis/actions",
		"/opt/vectis/actions",
	})

	want := []string{"/opt/vectis/actions", "/srv/vectis/actions"}
	if got := ActionRegistryLocalRoots(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ActionRegistryLocalRoots() = %v, want %v", got, want)
	}
}

func TestActionRegistryPolicyFromViper(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("action_registry.allowed_namespaces", []string{"examples, acme", "examples"})
	viper.Set("action_registry.allowed_sources", []string{"local_filesystem, oci", "local_filesystem"})
	viper.Set("action_registry.require_digest_pins", true)

	policy, err := ActionRegistryPolicy()
	if err != nil {
		t.Fatalf("ActionRegistryPolicy(): %v", err)
	}

	if want := []string{"examples", "acme"}; !reflect.DeepEqual(policy.AllowedNamespaces, want) {
		t.Fatalf("AllowedNamespaces = %v, want %v", policy.AllowedNamespaces, want)
	}

	if want := []actionregistry.SourceType{actionregistry.SourceLocalFilesystem, actionregistry.SourceOCI}; !reflect.DeepEqual(policy.AllowedSources, want) {
		t.Fatalf("AllowedSources = %v, want %v", policy.AllowedSources, want)
	}

	if !policy.RequireDigestPins {
		t.Fatal("RequireDigestPins = false, want true")
	}
}

func TestActionRegistryPolicyRejectsInvalidSource(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("action_registry.allowed_sources", []string{"telepathy"})
	_, err := ActionRegistryPolicy()
	if err == nil || !strings.Contains(err.Error(), "unsupported source") {
		t.Fatalf("ActionRegistryPolicy() error = %v, want unsupported source", err)
	}
}
