package main

import (
	"context"
	"io"

	"github.com/spf13/viper"

	scmgerrit "vectis/extensions/scm/gerrit"
	"vectis/extensions/scm/sshstream"
	"vectis/internal/interfaces"
)

const (
	defaultGerritSSHPort    = 29418
	defaultGerritSSHCommand = "gerrit stream-events"
)

func sshStreamOptionsFromViper() sshstream.Options {
	return normalizeGerritSSHStreamOptions(sshstream.Options{
		Host:                  viper.GetString("ssh_host"),
		Port:                  viper.GetInt("ssh_port"),
		User:                  viper.GetString("ssh_user"),
		KeyFile:               viper.GetString("ssh_key_file"),
		UseAgent:              viper.GetBool("ssh_use_agent"),
		KnownHostsFile:        viper.GetString("ssh_known_hosts_file"),
		InsecureIgnoreHostKey: viper.GetBool("ssh_insecure_ignore_host_key"),
		Command:               viper.GetString("ssh_command"),
		ConnectTimeout:        viper.GetDuration("ssh_connect_timeout"),
	})
}

func sshReconnectOptionsFromViper(logger interfaces.Logger) sshstream.ReconnectOptions {
	return sshstream.ReconnectOptions{
		BaseDelay:   viper.GetDuration("ssh_reconnect_base_delay"),
		MaxDelay:    viper.GetDuration("ssh_reconnect_max_delay"),
		MaxAttempts: viper.GetInt("ssh_reconnect_max_attempts"),
		Clock:       interfaces.SystemClock{},
		Logger:      logger,
		Label:       "Gerrit SSH stream",
	}
}

func consumeSSHStreamWithReconnect(ctx context.Context, sshOpts sshstream.Options, reconnectOpts sshstream.ReconnectOptions, streamOpts scmgerrit.StreamOptions, handle scmgerrit.StreamHandler) error {
	sshOpts = normalizeGerritSSHStreamOptions(sshOpts)
	if logger := reconnectOpts.Logger; logger != nil {
		normalized, err := sshstream.NormalizeOptions(sshOpts)
		if err == nil {
			logger.Info("Gerrit stream bridge reading managed SSH stream %s as %s", normalized.Address(), normalized.User)
		}
	}

	return sshstream.ConsumeWithReconnect(ctx, sshOpts, reconnectOpts, func(ctx context.Context, reader io.Reader) error {
		return scmgerrit.ConsumeStream(ctx, reader, streamOpts, handle)
	})
}

func normalizeGerritSSHStreamOptions(opts sshstream.Options) sshstream.Options {
	if opts.Port <= 0 {
		opts.Port = defaultGerritSSHPort
	}

	if opts.Command == "" {
		opts.Command = defaultGerritSSHCommand
	}

	if opts.ConnectTimeout <= 0 {
		opts.ConnectTimeout = sshstream.DefaultConnectTimeout
	}

	return opts
}
