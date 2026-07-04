package main

import (
	"testing"

	"vectis/internal/config"
)

func TestSCMPollerDependencyAddressEnvBindings(t *testing.T) {
	t.Setenv("VECTIS_SCM_POLLER_QUEUE_ADDRESS", "queue.env:8081")
	t.Setenv("VECTIS_SCM_POLLER_REGISTRY_ADDRESS", "registry.env:8082")

	if got := config.SCMPollerQueueAddress(); got != "queue.env:8081" {
		t.Fatalf("SCMPollerQueueAddress() = %q, want queue.env:8081", got)
	}

	if got := config.SCMPollerRegistryAddress(); got != "registry.env:8082" {
		t.Fatalf("SCMPollerRegistryAddress() = %q, want registry.env:8082", got)
	}
}

func TestSCMPollerDependencyAddressFlagsExist(t *testing.T) {
	for _, name := range []string{"queue-address", "registry-address"} {
		if rootCmd.PersistentFlags().Lookup(name) == nil {
			t.Fatalf("expected %s flag to be registered", name)
		}
	}
}
