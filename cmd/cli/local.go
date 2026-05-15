package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"sort"
	"vectis/internal/utils"
)

func resetTargets() ([]string, error) {
	var targets []string
	add := func(path string) {
		if path == "" {
			return
		}
		targets = append(targets, filepath.Clean(path))
	}

	configDir, err := os.UserConfigDir()
	if err != nil {
		return nil, fmt.Errorf("resolve user config directory: %w", err)
	}

	add(filepath.Join(configDir, "vectis"))
	add(filepath.Join(utils.DataHome(), "vectis"))

	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return nil, fmt.Errorf("resolve user cache directory: %w", err)
	}

	add(filepath.Join(cacheDir, "vectis"))

	if deployConfigDir := os.Getenv(envDeployConfigDir); deployConfigDir != "" {
		add(filepath.Join(deployConfigDir, "podman"))
	}

	sort.Strings(targets)
	unique := targets[:0]
	for _, target := range targets {
		if len(unique) == 0 || unique[len(unique)-1] != target {
			unique = append(unique, target)
		}
	}

	return unique, nil
}

func runReset(cmd *cobra.Command, args []string) {
	targets, err := resetTargets()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if resetDryRun {
		if outputIsJSON() {
			runCLIError(writeJSON(os.Stdout, map[string]any{
				"dry_run": true,
				"targets": targets,
			}))

			return
		}

		fmt.Println("Would remove:")
		for _, target := range targets {
			fmt.Printf("  %s\n", target)
		}

		return
	}

	if !resetYes {
		fmt.Fprintln(os.Stderr, "Error: reset removes local Vectis config, data, cache, tokens, and generated deployment secrets; pass --yes to confirm")
		fmt.Fprintln(os.Stderr, "Use --dry-run to inspect the directories first.")
		os.Exit(1)
	}

	results := make([]map[string]string, 0, len(targets))
	for _, target := range targets {
		if _, err := os.Stat(target); os.IsNotExist(err) {
			if outputIsJSON() {
				results = append(results, map[string]string{"path": target, "status": "missing"})
			} else {
				fmt.Printf("Skipped missing path: %s\n", target)
			}

			continue
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "Error: inspect %s: %v\n", target, err)
			os.Exit(1)
		}

		if err := os.RemoveAll(target); err != nil {
			fmt.Fprintf(os.Stderr, "Error: remove %s: %v\n", target, err)
			os.Exit(1)
		}

		if outputIsJSON() {
			results = append(results, map[string]string{"path": target, "status": "removed"})
		} else {
			fmt.Printf("Removed: %s\n", target)
		}
	}

	if outputIsJSON() {
		runCLIError(writeJSON(os.Stdout, map[string]any{
			"dry_run": false,
			"targets": results,
		}))
	}
}

var localCmd = &cobra.Command{
	Use:     "local",
	Short:   "Manage local CLI and development-machine state",
	GroupID: cliGroupOperations,
	Run:     showCommandHelp,
}

func configureResetFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&resetYes, "yes", false, "Confirm removal of local Vectis directories")
	cmd.Flags().BoolVar(&resetDryRun, "dry-run", false, "Print the directories that would be removed")
}

var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Remove local Vectis config, data, cache, and generated deploy state",
	Long: `Remove local Vectis application support/config, data, cache, CLI tokens, and generated deployment state.

This is a destructive local reset. It does not stop running services or delete remote/container volumes.`,
	Args: cobra.NoArgs,
	Run:  runReset,
}
