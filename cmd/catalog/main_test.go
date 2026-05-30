package main

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/spf13/viper"

	"vectis/internal/database"
)

func TestOpenCatalogFanInSourcesOpensConfiguredCellDBs(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(database.EnvDatabaseDriver, "sqlite3")

	dir := t.TempDir()
	globalDB := filepath.Join(dir, "global.db")
	cellADB := filepath.Join(dir, "iad-a.db")
	cellBDB := filepath.Join(dir, "pdx-b.db")
	for _, dsn := range []string{globalDB, cellADB, cellBDB} {
		if err := database.Migrate(dsn); err != nil {
			t.Fatalf("migrate %s: %v", dsn, err)
		}
	}

	viper.Set("cell_database_dsns", []string{
		"pdx-b=" + cellBDB,
		"local=" + globalDB,
		"iad-a=" + cellADB,
	})

	sources, closeSources, err := openCatalogFanInSources(nil, globalDB)
	if err != nil {
		t.Fatalf("openCatalogFanInSources: %v", err)
	}
	defer closeSources()

	var got []string
	for _, source := range sources {
		got = append(got, source.CellID)
		if source.Events == nil {
			t.Fatalf("source %q has nil events repository", source.CellID)
		}
	}

	want := []string{"iad-a", "pdx-b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sources: got %v, want %v", got, want)
	}
}
