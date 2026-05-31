package main

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/viper"

	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/interfaces/mocks"
)

func resetLocalTestConfig(t *testing.T) {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(database.EnvDatabaseDriver, "")
	t.Setenv(database.EnvDatabaseDSN, "")
	t.Setenv(database.EnvGlobalDatabaseDSN, "")
	t.Setenv(database.EnvCellDatabaseDSN, "")
	t.Setenv("VECTIS_CELL_ID", "")
}

func TestBuildLocalTopology_DefaultCell(t *testing.T) {
	resetLocalTestConfig(t)
	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("VECTIS_CELL_ID", "iad-a")

	topology, err := buildLocalTopology()
	if err != nil {
		t.Fatalf("buildLocalTopology: %v", err)
	}

	if topology.GlobalDB != filepath.Join(dataHome, "vectis", "global", "db.sqlite3") {
		t.Fatalf("global DB: got %q", topology.GlobalDB)
	}

	if len(topology.Cells) != 1 {
		t.Fatalf("cells: got %d, want 1", len(topology.Cells))
	}

	cell := topology.Cells[0]
	if cell.ID != "iad-a" {
		t.Fatalf("cell ID: got %q, want iad-a", cell.ID)
	}

	if cell.CellDB != filepath.Join(dataHome, "vectis", "cells", "iad-a", "db.sqlite3") {
		t.Fatalf("cell DB: got %q", cell.CellDB)
	}

	if cell.QueuePort != config.QueuePort() || cell.CellIngressPort != config.CellIngressPort() {
		t.Fatalf("default ports: queue=%d ingress=%d", cell.QueuePort, cell.CellIngressPort)
	}
}

func TestBuildLocalTopology_ExtraCells(t *testing.T) {
	resetLocalTestConfig(t)
	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("VECTIS_CELL_ID", "iad-a")
	viper.Set("cells", []string{"pdx-b", "sjc-c"})

	topology, err := buildLocalTopology()
	if err != nil {
		t.Fatalf("buildLocalTopology: %v", err)
	}

	gotIDs := localCellIDs(topology.Cells)
	wantIDs := []string{"iad-a", "pdx-b", "sjc-c"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("cell IDs: got %v, want %v", gotIDs, wantIDs)
	}

	if topology.Cells[1].QueuePort != config.QueuePort()+cellPortStride {
		t.Fatalf("second cell queue port: got %d", topology.Cells[1].QueuePort)
	}

	if topology.Cells[2].CellIngressPort != config.CellIngressPort()+2*cellPortStride {
		t.Fatalf("third cell ingress port: got %d", topology.Cells[2].CellIngressPort)
	}

	if topology.Cells[2].CellDB != filepath.Join(dataHome, "vectis", "cells", "sjc-c", "db.sqlite3") {
		t.Fatalf("third cell DB: got %q", topology.Cells[2].CellDB)
	}
}

func TestBuildLocalTopology_RejectsDuplicateCells(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("VECTIS_CELL_ID", "iad-a")
	viper.Set("cells", []string{"pdx-b", "iad-a"})

	if _, err := buildLocalTopology(); err == nil {
		t.Fatal("buildLocalTopology succeeded with duplicate cell")
	}
}

func TestBuildLocalTopology_RejectsCustomDSNsForExtraCells(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv(database.EnvDatabaseDSN, filepath.Join(t.TempDir(), "shared.db"))
	viper.Set("cells", []string{"pdx-b"})

	if _, err := buildLocalTopology(); err == nil {
		t.Fatal("buildLocalTopology succeeded with custom DSN and extra cell")
	}
}

func TestLocalCellIngressEndpointSpecs(t *testing.T) {
	resetLocalTestConfig(t)
	viper.Set("host", "0.0.0.0")
	cells := []localCell{
		{ID: "iad-a", CellIngressPort: 8085},
		{ID: "pdx-b", CellIngressPort: 8185},
	}

	got := localCellIngressEndpointSpecs(cells)
	want := []string{"iad-a=http://localhost:8085", "pdx-b=http://localhost:8185"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpoint specs: got %v, want %v", got, want)
	}
}

func TestLocalCatalogCellDatabaseEnv(t *testing.T) {
	resetLocalTestConfig(t)
	cells := []localCell{
		{ID: "iad-a", CellDB: "/tmp/iad.db"},
		{ID: "pdx-b", CellDB: "/tmp/pdx.db"},
	}

	got := localCatalogCellDatabaseEnv(cells)
	want := []string{"VECTIS_CATALOG_CELL_DATABASE_DSNS=iad-a=/tmp/iad.db,pdx-b=/tmp/pdx.db"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("catalog env: got %v, want %v", got, want)
	}
}

func TestLocalServices_HAProfileBuildsMultiInstanceCell(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("VECTIS_CELL_ID", "iad-a")
	viper.Set("profile", localProfileHA)
	viper.Set("docs_enabled", false)

	topology, err := buildLocalTopology()
	if err != nil {
		t.Fatalf("buildLocalTopology: %v", err)
	}

	services := localServices(mocks.NopLogger{}, topology)

	wantCounts := map[string]int{
		"vectis-registry":     3,
		"vectis-queue":        2,
		"vectis-log":          2,
		"vectis-api":          2,
		"vectis-cell-ingress": 1,
		"vectis-worker":       2,
		"vectis-cron":         2,
		"vectis-reconciler":   2,
		"vectis-catalog":      1,
	}

	gotCounts := map[string]int{}
	for _, svc := range services {
		gotCounts[svc.binary]++
	}

	for binary, want := range wantCounts {
		if gotCounts[binary] != want {
			t.Fatalf("%s count = %d, want %d; services=%v", binary, gotCounts[binary], want, serviceNames(services))
		}
	}

	var foundQueue2 bool
	var foundRegistryPeers bool
	var workersUseRegistry bool
	for _, svc := range services {
		if svc.name == "queue-2" &&
			hasEnv(svc.env, "VECTIS_CELL_ID=iad-a") &&
			hasEnv(svc.env, "VECTIS_QUEUE_PORT=8181") &&
			hasEnv(svc.env, "VECTIS_QUEUE_INSTANCE_ID=queue-2") {
			foundQueue2 = true
		}

		if svc.name == "registry-1" &&
			envContains(svc.env, "VECTIS_REGISTRY_CLUSTER_PEER_ADDRESSES=", "localhost:8182") &&
			envContains(svc.env, "VECTIS_REGISTRY_CLUSTER_PEER_ADDRESSES=", "localhost:8282") {
			foundRegistryPeers = true
		}

		if svc.name == "worker-1" && !hasEnvPrefix(svc.env, "VECTIS_WORKER_QUEUE_ADDRESS=") {
			workersUseRegistry = true
		}
	}

	if !foundQueue2 {
		t.Fatalf("queue-2 did not include expected HA env: %+v", services)
	}

	if !foundRegistryPeers {
		t.Fatalf("registry-1 did not include expected peer env: %+v", services)
	}

	if !workersUseRegistry {
		t.Fatalf("worker-1 did not rely on registry queue discovery: %+v", services)
	}
}

func hasEnv(env []string, want string) bool {
	for _, got := range env {
		if got == want {
			return true
		}
	}

	return false
}

func hasEnvPrefix(env []string, prefix string) bool {
	for _, got := range env {
		if strings.HasPrefix(got, prefix) {
			return true
		}
	}

	return false
}

func envContains(env []string, prefix, want string) bool {
	for _, got := range env {
		if strings.HasPrefix(got, prefix) && strings.Contains(got, want) {
			return true
		}
	}

	return false
}
