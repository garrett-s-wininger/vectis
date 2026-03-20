package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/api"
	"vectis/internal/database"
	"vectis/internal/interfaces"

	_ "github.com/mattn/go-sqlite3"
)

func runVectisAPI(cmd *cobra.Command, args []string) {
	logger := interfaces.NewLogger("api")
	logger.Info("Starting API server...")

	// NOTE(garrett): Skip if production.
	dbPath := database.GetDBPath()
	logger.Info("Using database: %s", dbPath)

	// TODO(garrett): Skip if production.
	db, err := database.OpenDB(dbPath)
	if err != nil {
		logger.Fatal("Failed to open database: %v", err)
	}
	defer db.Close()

	server := api.NewAPIServer(logger, db)

	if err := server.ConnectToRegistry(cmd.Context()); err != nil {
		logger.Fatal("Failed to connect to services: %v", err)
	}

	port := viper.GetInt("port")
	if port <= 0 {
		port = 8080
	}
	addr := fmt.Sprintf(":%d", port)
	if err := server.Run(addr); err != nil {
		logger.Fatal("Server failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-api-server",
	Short: "Vectis API Server",
	Long:  `The Vectis API Server provides REST endpoints for triggering stored jobs.`,
	Run:   runVectisAPI,
}

func init() {
	viper.SetDefault("port", 8080)
	rootCmd.PersistentFlags().Int("port", 8080, "Port for the API server")
	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	viper.SetEnvPrefix("VECTIS_API_SERVER")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
