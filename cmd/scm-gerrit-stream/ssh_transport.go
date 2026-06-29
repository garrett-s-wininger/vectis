package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"

	scmgerrit "vectis/extensions/scm/gerrit"
	"vectis/internal/backoff"
	"vectis/internal/interfaces"
)

const (
	defaultGerritSSHPort    = 29418
	defaultGerritSSHCommand = "gerrit stream-events"
)

type sshStreamOptions struct {
	Host                  string
	Port                  int
	User                  string
	KeyFile               string
	UseAgent              bool
	KnownHostsFile        string
	InsecureIgnoreHostKey bool
	Command               string
	ConnectTimeout        time.Duration
}

type streamReconnectOptions struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
	Clock       interfaces.Clock
	Logger      interfaces.Logger
	Label       string
}

func sshStreamOptionsFromViper() sshStreamOptions {
	return sshStreamOptions{
		Host:                  viper.GetString("ssh_host"),
		Port:                  viper.GetInt("ssh_port"),
		User:                  viper.GetString("ssh_user"),
		KeyFile:               viper.GetString("ssh_key_file"),
		UseAgent:              viper.GetBool("ssh_use_agent"),
		KnownHostsFile:        viper.GetString("ssh_known_hosts_file"),
		InsecureIgnoreHostKey: viper.GetBool("ssh_insecure_ignore_host_key"),
		Command:               viper.GetString("ssh_command"),
		ConnectTimeout:        viper.GetDuration("ssh_connect_timeout"),
	}
}

func sshReconnectOptionsFromViper(logger interfaces.Logger) streamReconnectOptions {
	return streamReconnectOptions{
		BaseDelay:   viper.GetDuration("ssh_reconnect_base_delay"),
		MaxDelay:    viper.GetDuration("ssh_reconnect_max_delay"),
		MaxAttempts: viper.GetInt("ssh_reconnect_max_attempts"),
		Clock:       interfaces.SystemClock{},
		Logger:      logger,
		Label:       "Gerrit SSH stream",
	}
}

func consumeSSHStreamWithReconnect(ctx context.Context, sshOpts sshStreamOptions, reconnectOpts streamReconnectOptions, streamOpts scmgerrit.StreamOptions, handle scmgerrit.StreamHandler) error {
	sshOpts, err := normalizeSSHStreamOptions(sshOpts)
	if err != nil {
		return err
	}

	logger := reconnectOpts.Logger
	if logger == nil {
		logger = interfaces.NewLogger("scm-gerrit-stream")
		reconnectOpts.Logger = logger
	}

	logger.Info("Gerrit stream bridge reading managed SSH stream %s as %s", sshOpts.Address(), sshOpts.User)
	return consumeWithReconnect(ctx, reconnectOpts, func() error {
		return consumeSSHStreamOnce(ctx, sshOpts, streamOpts, handle, logger)
	})
}

func consumeWithReconnect(ctx context.Context, opts streamReconnectOptions, consume func() error) error {
	if consume == nil {
		return fmt.Errorf("stream consumer is required")
	}

	clock := opts.Clock
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	logger := opts.Logger
	if logger == nil {
		logger = interfaces.NewLogger("scm-gerrit-stream")
	}

	label := strings.TrimSpace(opts.Label)
	if label == "" {
		label = "stream"
	}

	baseDelay := opts.BaseDelay
	if baseDelay <= 0 {
		baseDelay = time.Second
	}

	var lastErr error
	for attempt := 0; ; attempt++ {
		lastErr = consume()
		if err := ctx.Err(); err != nil {
			return err
		}

		if lastErr == nil {
			lastErr = fmt.Errorf("%s ended", label)
		}

		if opts.MaxAttempts > 0 && attempt+1 >= opts.MaxAttempts {
			return lastErr
		}

		delay := backoff.ExponentialDelay(baseDelay, attempt, opts.MaxDelay)
		logger.Warn("%s disconnected: %v; reconnecting in %v", label, lastErr, delay)
		if err := clock.Sleep(ctx, delay); err != nil {
			return err
		}
	}
}

func consumeSSHStreamOnce(ctx context.Context, opts sshStreamOptions, streamOpts scmgerrit.StreamOptions, handle scmgerrit.StreamHandler, logger interfaces.Logger) error {
	clientConfig, closeAuth, err := newSSHClientConfig(opts)
	if err != nil {
		return err
	}
	defer func() { _ = closeAuth() }()

	client, err := dialSSHClient(ctx, opts.Address(), opts.ConnectTimeout, clientConfig)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("open Gerrit SSH session: %w", err)
	}
	defer func() { _ = session.Close() }()

	stdout, err := session.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open Gerrit SSH stdout: %w", err)
	}

	stderr, err := session.StderrPipe()
	if err != nil {
		return fmt.Errorf("open Gerrit SSH stderr: %w", err)
	}
	go logSSHStderr(logger, stderr)

	closeOnCancel := make(chan struct{})
	defer close(closeOnCancel)
	go func() {
		select {
		case <-ctx.Done():
			_ = session.Close()
			_ = client.Close()
		case <-closeOnCancel:
		}
	}()

	if err := session.Start(opts.Command); err != nil {
		return fmt.Errorf("start Gerrit SSH stream command %q: %w", opts.Command, err)
	}

	consumeErr := scmgerrit.ConsumeStream(ctx, stdout, streamOpts, handle)
	if err := ctx.Err(); err != nil {
		return err
	}

	waitErr := session.Wait()
	if consumeErr != nil {
		return consumeErr
	}

	if waitErr != nil {
		return fmt.Errorf("Gerrit SSH stream command exited: %w", waitErr)
	}

	return fmt.Errorf("Gerrit SSH stream command exited")
}

func newSSHClientConfig(opts sshStreamOptions) (*ssh.ClientConfig, func() error, error) {
	auths, closeAuth, err := sshAuthMethods(opts)
	if err != nil {
		return nil, nil, err
	}

	hostKeyCallback, err := sshHostKeyCallback(opts)
	if err != nil {
		_ = closeAuth()
		return nil, nil, err
	}

	return &ssh.ClientConfig{
		User:            opts.User,
		Auth:            auths,
		HostKeyCallback: hostKeyCallback,
		Timeout:         opts.ConnectTimeout,
	}, closeAuth, nil
}

func sshAuthMethods(opts sshStreamOptions) ([]ssh.AuthMethod, func() error, error) {
	var auths []ssh.AuthMethod
	var closers []io.Closer

	if keyFile := strings.TrimSpace(opts.KeyFile); keyFile != "" {
		key, err := os.ReadFile(keyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("read SSH key file %q: %w", keyFile, err)
		}

		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, nil, fmt.Errorf("parse SSH key file %q: %w", keyFile, err)
		}

		auths = append(auths, ssh.PublicKeys(signer))
	}

	if opts.UseAgent {
		agentSock := strings.TrimSpace(os.Getenv("SSH_AUTH_SOCK"))
		if agentSock != "" {
			conn, err := net.Dial("unix", agentSock)
			if err != nil {
				return nil, nil, fmt.Errorf("connect SSH agent %q: %w", agentSock, err)
			}

			closers = append(closers, conn)
			auths = append(auths, ssh.PublicKeysCallback(agent.NewClient(conn).Signers))
		}
	}

	closeAuth := func() error {
		var errs []string
		for _, closer := range closers {
			if err := closer.Close(); err != nil {
				errs = append(errs, err.Error())
			}
		}

		if len(errs) > 0 {
			return fmt.Errorf("%s", strings.Join(errs, "; "))
		}

		return nil
	}

	if len(auths) == 0 {
		_ = closeAuth()
		return nil, nil, fmt.Errorf("managed SSH stream requires --ssh-key-file or SSH_AUTH_SOCK with --ssh-use-agent")
	}

	return auths, closeAuth, nil
}

func sshHostKeyCallback(opts sshStreamOptions) (ssh.HostKeyCallback, error) {
	if opts.InsecureIgnoreHostKey {
		return ssh.InsecureIgnoreHostKey(), nil
	}

	knownHostsFile := strings.TrimSpace(opts.KnownHostsFile)
	if knownHostsFile == "" {
		knownHostsFile = defaultKnownHostsFile()
	}

	if knownHostsFile == "" {
		return nil, fmt.Errorf("managed SSH stream requires --ssh-known-hosts-file or ~/.ssh/known_hosts")
	}

	if _, err := os.Stat(knownHostsFile); err != nil {
		return nil, fmt.Errorf("read SSH known hosts file %q: %w", knownHostsFile, err)
	}

	hostKeyCallback, err := knownhosts.New(knownHostsFile)
	if err != nil {
		return nil, fmt.Errorf("load SSH known hosts file %q: %w", knownHostsFile, err)
	}

	return hostKeyCallback, nil
}

func dialSSHClient(ctx context.Context, address string, timeout time.Duration, config *ssh.ClientConfig) (*ssh.Client, error) {
	if config == nil {
		return nil, fmt.Errorf("SSH client config is required")
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("dial Gerrit SSH %s: %w", address, err)
	}

	if timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(timeout))
	}

	type sshConnResult struct {
		conn  ssh.Conn
		chans <-chan ssh.NewChannel
		reqs  <-chan *ssh.Request
		err   error
	}

	resultCh := make(chan sshConnResult, 1)
	go func() {
		sshConn, chans, reqs, err := ssh.NewClientConn(conn, address, config)
		resultCh <- sshConnResult{conn: sshConn, chans: chans, reqs: reqs, err: err}
	}()

	select {
	case <-ctx.Done():
		_ = conn.Close()
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("establish Gerrit SSH session %s: %w", address, result.err)
		}

		_ = conn.SetDeadline(time.Time{})
		return ssh.NewClient(result.conn, result.chans, result.reqs), nil
	}
}

func logSSHStderr(logger interfaces.Logger, r io.Reader) {
	if logger == nil || r == nil {
		return
	}

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			logger.Warn("Gerrit SSH stderr: %s", line)
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Warn("Read Gerrit SSH stderr: %v", err)
	}
}

func normalizeSSHStreamOptions(opts sshStreamOptions) (sshStreamOptions, error) {
	opts.Host = strings.TrimSpace(opts.Host)
	if opts.Host == "" {
		return sshStreamOptions{}, fmt.Errorf("managed SSH stream requires --ssh-host")
	}

	if opts.Port <= 0 {
		opts.Port = defaultGerritSSHPort
	}

	opts.User = strings.TrimSpace(opts.User)
	if opts.User == "" {
		opts.User = currentUsername()
	}

	if opts.User == "" {
		return sshStreamOptions{}, fmt.Errorf("managed SSH stream requires --ssh-user when the current user cannot be inferred")
	}

	opts.KeyFile = strings.TrimSpace(opts.KeyFile)
	opts.KnownHostsFile = strings.TrimSpace(opts.KnownHostsFile)
	opts.Command = strings.TrimSpace(opts.Command)
	if opts.Command == "" {
		opts.Command = defaultGerritSSHCommand
	}

	if opts.ConnectTimeout <= 0 {
		opts.ConnectTimeout = 10 * time.Second
	}

	return opts, nil
}

func (o sshStreamOptions) Address() string {
	host := strings.TrimSpace(o.Host)
	port := o.Port
	if port <= 0 {
		port = defaultGerritSSHPort
	}

	return net.JoinHostPort(host, strconv.Itoa(port))
}

func defaultKnownHostsFile() string {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return ""
	}

	return filepath.Join(home, ".ssh", "known_hosts")
}

func currentUsername() string {
	for _, key := range []string{"LOGNAME", "USER", "USERNAME"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}

	return ""
}
