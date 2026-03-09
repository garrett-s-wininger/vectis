package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func runVectis(cmd *cobra.Command, args []string) {
	fmt.Println("Hello, World!")
}

var rootCmd = &cobra.Command{
	Use:   "vectis",
	Short: "A self-hosted, modern build system",
	Long: `Vectis is a modern, self-hosted build system.

It's designed to be easy to use and deploy for a wide-range of build types
and execution environments.`,
	Run: runVectis,
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
