package database

import (
	"path/filepath"
	"testing"

	"vectis/internal/platform"
)

func TestGetDBPathForRoleUsesRoleDSN(t *testing.T) {
	t.Setenv(EnvDatabaseDSN, "{{data_home}}/shared.db")
	t.Setenv(EnvGlobalDatabaseDSN, "{{data_home}}/global.db")
	t.Setenv(EnvCellDatabaseDSN, "{{data_home}}/cell.db")

	if got, want := GetDBPathForRole(RoleGlobal), filepath.Join(platform.DataHome(), "global.db"); got != want {
		t.Fatalf("global DB path: got %q, want %q", got, want)
	}

	if got, want := GetDBPathForRole(RoleCell), filepath.Join(platform.DataHome(), "cell.db"); got != want {
		t.Fatalf("cell DB path: got %q, want %q", got, want)
	}
}

func TestGetDBPathForRoleFallsBackToSharedDSN(t *testing.T) {
	t.Setenv(EnvDatabaseDSN, "{{data_home}}/shared.db")

	want := filepath.Join(platform.DataHome(), "shared.db")
	if got := GetDBPathForRole(RoleGlobal); got != want {
		t.Fatalf("global fallback DB path: got %q, want %q", got, want)
	}

	if got := GetDBPathForRole(RoleCell); got != want {
		t.Fatalf("cell fallback DB path: got %q, want %q", got, want)
	}
}

func TestGlobalAndCellDatabasesAreSplit(t *testing.T) {
	t.Setenv(EnvDatabaseDSN, "{{data_home}}/shared.db")
	if GlobalAndCellDatabasesAreSplit() {
		t.Fatal("shared fallback DSN should not be split")
	}

	t.Setenv(EnvGlobalDatabaseDSN, "{{data_home}}/global.db")
	t.Setenv(EnvCellDatabaseDSN, "{{data_home}}/cell.db")
	if !GlobalAndCellDatabasesAreSplit() {
		t.Fatal("distinct role DSNs should be split")
	}
}
