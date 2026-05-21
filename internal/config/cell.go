package config

import (
	"os"
	"strings"

	"github.com/spf13/viper"
)

const envCellID = "VECTIS_CELL_ID"

// CellID returns the configured identity for the local execution cell.
func CellID() string {
	if v := strings.TrimSpace(os.Getenv(envCellID)); v != "" {
		return v
	}

	if v := strings.TrimSpace(viper.GetString("cell.id")); v != "" {
		return v
	}

	return strings.TrimSpace(MustDefaults().Cell.ID)
}
