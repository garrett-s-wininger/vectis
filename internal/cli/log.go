package cli

import (
	"vectis/internal/interfaces"

	"github.com/spf13/viper"
)

func SetLogLevel(logger interfaces.Logger) {
	if levelStr := viper.GetString("log_level"); levelStr != "" {
		level, err := interfaces.ParseLevel(levelStr)
		if err != nil {
			logger.Fatal("%v", err)
		}

		logger.SetLevel(level)
	}
}
