package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"vectis/internal/version"
)

func ConfigureVersion(cmd *cobra.Command) {
	cmd.Version = version.String()
	cmd.SetVersionTemplate(fmt.Sprintf("%s version %s\n", cmd.CommandPath(), "{{.Version}}"))
}
