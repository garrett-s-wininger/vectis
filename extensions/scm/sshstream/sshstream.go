package sshstream

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

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"

	"vectis/internal/backoff"
	"vectis/internal/interfaces"
)

const DefaultConnectTimeout = 10 * time.Second

type Options struct {
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

type ReconnectOptions struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
	Clock       interfaces.Clock
	Logger      interfaces.Logger
	Label       string
}

type Consumer func(context.Context, io.Reader) error

func ConsumeWithReconnect(ctx context.Context, opts Options, reconnect ReconnectOptions, consume Consumer) error {
	opts, err := NormalizeOptions(opts)
	if err != nil {
		return err
	}

	return consumeWithReconnect(ctx, reconnect, func() error {
		return ConsumeOnce(ctx, opts, consume, reconnect.Logger)
	})
}

func ConsumeOnce(ctx context.Context, opts Options, consume Consumer, logger interfaces.Logger) error {
	opts, err := NormalizeOptions(opts)
	if err != nil {
		return err
	}

	if consume == nil {
		return fmt.Errorf("SSH stream consumer is required")
	}

	clientConfig, closeAuth, err := NewClientConfig(opts)
	if err != nil {
		return err
	}
	defer func() { _ = closeAuth() }()

	client, err := DialClient(ctx, opts.Address(), opts.ConnectTimeout, clientConfig)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("open SSH session: %w", err)
	}
	defer func() { _ = session.Close() }()

	stdout, err := session.StdoutPipe()
	if err != nil {
		return fmt.Errorf("open SSH stdout: %w", err)
	}

	stderr, err := session.StderrPipe()
	if err != nil {
		return fmt.Errorf("open SSH stderr: %w", err)
	}

	go logStderr(logger, stderr)

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
		return fmt.Errorf("start SSH stream command %q: %w", opts.Command, err)
	}

	consumeErr := consume(ctx, stdout)
	if err := ctx.Err(); err != nil {
		return err
	}

	waitErr := session.Wait()
	if consumeErr != nil {
		return consumeErr
	}

	if waitErr != nil {
		return fmt.Errorf("SSH stream command exited: %w", waitErr)
	}

	return fmt.Errorf("SSH stream command exited")
}

func NormalizeOptions(opts Options) (Options, error) {
	opts.Host = strings.TrimSpace(opts.Host)
	if opts.Host == "" {
		return Options{}, fmt.Errorf("SSH stream requires host")
	}

	if opts.Port <= 0 {
		return Options{}, fmt.Errorf("SSH stream requires port")
	}

	opts.User = strings.TrimSpace(opts.User)
	if opts.User == "" {
		opts.User = currentUsername()
	}

	if opts.User == "" {
		return Options{}, fmt.Errorf("SSH stream requires user when the current user cannot be inferred")
	}

	opts.KeyFile = strings.TrimSpace(opts.KeyFile)
	opts.KnownHostsFile = strings.TrimSpace(opts.KnownHostsFile)
	opts.Command = strings.TrimSpace(opts.Command)
	if opts.Command == "" {
		return Options{}, fmt.Errorf("SSH stream requires command")
	}

	if opts.ConnectTimeout <= 0 {
		opts.ConnectTimeout = DefaultConnectTimeout
	}

	return opts, nil
}

func NewClientConfig(opts Options) (*ssh.ClientConfig, func() error, error) {
	auths, closeAuth, err := authMethods(opts)
	if err != nil {
		return nil, nil, err
	}

	hostKeyCallback, err := HostKeyCallback(opts)
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

func HostKeyCallback(opts Options) (ssh.HostKeyCallback, error) {
	if opts.InsecureIgnoreHostKey {
		return ssh.InsecureIgnoreHostKey(), nil
	}

	knownHostsFile := strings.TrimSpace(opts.KnownHostsFile)
	if knownHostsFile == "" {
		knownHostsFile = defaultKnownHostsFile()
	}

	if knownHostsFile == "" {
		return nil, fmt.Errorf("SSH stream requires known_hosts file")
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

func DialClient(ctx context.Context, address string, timeout time.Duration, config *ssh.ClientConfig) (*ssh.Client, error) {
	if config == nil {
		return nil, fmt.Errorf("SSH client config is required")
	}

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("dial SSH %s: %w", address, err)
	}

	if timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(timeout))
	}

	type connResult struct {
		conn  ssh.Conn
		chans <-chan ssh.NewChannel
		reqs  <-chan *ssh.Request
		err   error
	}

	resultCh := make(chan connResult, 1)
	go func() {
		sshConn, chans, reqs, err := ssh.NewClientConn(conn, address, config)
		resultCh <- connResult{conn: sshConn, chans: chans, reqs: reqs, err: err}
	}()

	select {
	case <-ctx.Done():
		_ = conn.Close()
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("establish SSH session %s: %w", address, result.err)
		}

		_ = conn.SetDeadline(time.Time{})
		return ssh.NewClient(result.conn, result.chans, result.reqs), nil
	}
}

func (o Options) Address() string {
	return net.JoinHostPort(strings.TrimSpace(o.Host), strconv.Itoa(o.Port))
}

func consumeWithReconnect(ctx context.Context, opts ReconnectOptions, consume func() error) error {
	if consume == nil {
		return fmt.Errorf("stream consumer is required")
	}

	clock := opts.Clock
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	logger := opts.Logger
	if logger == nil {
		logger = interfaces.NewLogger("ssh-stream")
	}

	label := strings.TrimSpace(opts.Label)
	if label == "" {
		label = "SSH stream"
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

func authMethods(opts Options) ([]ssh.AuthMethod, func() error, error) {
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
		return nil, nil, fmt.Errorf("SSH stream requires --ssh-key-file or SSH_AUTH_SOCK with agent auth enabled")
	}

	return auths, closeAuth, nil
}

func logStderr(logger interfaces.Logger, r io.Reader) {
	if logger == nil || r == nil {
		return
	}

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			logger.Warn("SSH stderr: %s", line)
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Warn("Read SSH stderr: %v", err)
	}
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
