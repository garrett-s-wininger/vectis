package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"vectis/internal/cli"
	"vectis/internal/interfaces"
	"vectis/internal/logserver"

	"github.com/spf13/viper"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		cancel()
	}()

	viper.SetEnvPrefix("VECTIS_LOG")
	viper.AutomaticEnv()

	logger := interfaces.NewLogger("log-aggregator")
	cli.SetLogLevel(logger)
	logger.Info("Starting log service...")

	if err := logserver.Run(ctx, logger); err != nil {
		logger.Fatal("Log service failed: %v", err)
	}
}
