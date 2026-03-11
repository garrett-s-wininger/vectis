package main

import (
	"log"
	"os"
	"os/exec"

	"vectis/internal/supervisor"

	"github.com/spf13/cobra"
)

var childBinaries = []string{
	"vectis-registry",
	"vectis-queue",
	"vectis-worker",
}

func runVectis(cmd *cobra.Command, args []string) {
	commands := make([]*exec.Cmd, 0, len(childBinaries))

	for _, b := range childBinaries {
		path, err := supervisor.FindBinary(b)
		if err != nil {
			log.Fatalf("cannot find %s: %v\n", b, err)
		}

		command := exec.Command(path)
		command.Stdin = nil
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr

		if err := command.Start(); err != nil {
			// TODO(garrett): Abort already started children.
			log.Fatalf("failed to start %s: %v\n", b, err)
		}

		commands = append(commands, command)
	}

	// TODO(garrett): Adjust to kill all children if one fails.
	// TODO(garrett): Propagate signals to children.
	for i, c := range commands {
		if err := c.Wait(); err != nil {
			log.Fatalf("%s exited: %v", childBinaries[i], err)
		}
	}
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
