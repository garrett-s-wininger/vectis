package cli

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

func RootContextForShutdown() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func ExecuteWithShutdownSignals(cmd *cobra.Command) error {
	ctx, stop := RootContextForShutdown()
	defer stop()
	return cmd.ExecuteContext(ctx)
}
