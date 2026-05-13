package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"vectis/internal/database"
)

func runMigrate(cmd *cobra.Command, args []string) {
	dbPath := database.GetDBPath()
	fmt.Printf("Migrating database: %s\n", dbPath)
	if err := database.Migrate(dbPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Migrations applied.")
}

var databaseCmd = &cobra.Command{
	Use:     "database",
	Short:   "Inspect and maintain Vectis database state",
	GroupID: cliGroupOperations,
	Run:     showCommandHelp,
}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply database migrations (admin / one-shot)",
	Long: `Run embedded SQL migrations against the database selected by VECTIS_DATABASE_DRIVER and VECTIS_DATABASE_DSN (or defaults).

Runtime services only wait for the schema; they do not migrate. Use this command (or CI/deploy automation) before starting the stack.`,
	Args: cobra.NoArgs,
	Run:  runMigrate,
}
