package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/viper"

	scmgerrit "vectis/extensions/scm/gerrit"
	"vectis/internal/interfaces"
)

const (
	streamTransportAuto  = "auto"
	streamTransportInput = "input"
	streamTransportSSH   = "ssh"
)

func consumeConfiguredStream(ctx context.Context, streamOpts scmgerrit.StreamOptions, handle scmgerrit.StreamHandler, logger interfaces.Logger) error {
	transport, err := resolveStreamTransport(viper.GetString("transport"), viper.GetString("ssh_host"))
	if err != nil {
		return err
	}

	switch transport {
	case streamTransportInput:
		reader, closeInput, err := openStreamInput(viper.GetString("input"))
		if err != nil {
			return fmt.Errorf("open Gerrit stream input: %w", err)
		}
		defer func() { _ = closeInput() }()

		logger.Info("Gerrit stream bridge reading %s", streamInputLabel(viper.GetString("input")))
		return scmgerrit.ConsumeStream(ctx, reader, streamOpts, handle)
	case streamTransportSSH:
		return consumeSSHStreamWithReconnect(ctx, sshStreamOptionsFromViper(), sshReconnectOptionsFromViper(logger), streamOpts, handle)
	default:
		return fmt.Errorf("unsupported Gerrit stream transport %q", transport)
	}
}

func resolveStreamTransport(transport, sshHost string) (string, error) {
	transport = strings.ToLower(strings.TrimSpace(transport))
	if transport == "" {
		transport = streamTransportAuto
	}

	switch transport {
	case streamTransportAuto:
		if strings.TrimSpace(sshHost) != "" {
			return streamTransportSSH, nil
		}
		return streamTransportInput, nil
	case streamTransportInput, streamTransportSSH:
		return transport, nil
	default:
		return "", fmt.Errorf("unsupported Gerrit stream transport %q", transport)
	}
}

func openStreamInput(path string) (io.Reader, func() error, error) {
	path = strings.TrimSpace(path)
	if path == "" || path == "-" {
		return os.Stdin, func() error { return nil }, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	return f, f.Close, nil
}

func streamInputLabel(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || path == "-" {
		return "stdin"
	}

	return path
}
