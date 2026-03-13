package main

import (
	"os"
	"os/exec"

	"vectis/internal/log"
	"vectis/internal/supervisor"

	"github.com/spf13/cobra"
)

var childBinaries = []string{
	"vectis-registry",
	"vectis-queue",
	"vectis-worker",
	"vectis-api-server",
}

func runVectis(cmd *cobra.Command, args []string) {
	logger := log.New("cli")
	commands := make([]*exec.Cmd, 0, len(childBinaries))

	for _, b := range childBinaries {
		path, err := supervisor.FindBinary(b)
		if err != nil {
			logger.Fatal("cannot find %s: %v", b, err)
		}

		command := exec.Command(path)
		command.Stdin = nil
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr

		if err := command.Start(); err != nil {
			// TODO(garrett): Abort already started children.
			logger.Fatal("failed to start %s: %v", b, err)
		}

		commands = append(commands, command)
	}

	// TODO(garrett): Adjust to kill all children if one fails.
	// TODO(garrett): Propagate signals to children.
	for i, c := range commands {
		if err := c.Wait(); err != nil {
			logger.Fatal("%s exited: %v", childBinaries[i], err)
		}
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-local",
	Short: "Run Vectis services locally for development",
	Long: `Vectis Local runs all Vectis services locally for development and testing.

It starts the registry, queue, worker, and API server as child processes.`,
	Run: runVectis,
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
