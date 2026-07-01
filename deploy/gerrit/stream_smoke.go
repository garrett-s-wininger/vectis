package gerrit

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	gerritaction "vectis/extensions/actions/gerrit"
	scmgerrit "vectis/extensions/scm/gerrit"
	"vectis/extensions/scm/sshstream"
	"vectis/internal/interfaces"
	"vectis/sdk/scm"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

const (
	DefaultStreamSmokeSSHHost = "127.0.0.1"
	DefaultStreamSmokeSSHPort = 29418
)

type StreamSmokeOptions struct {
	URL           string
	AccountID     string
	Username      string
	Project       string
	ProjectPrefix string
	SSHHost       string
	SSHPort       int
	Timeout       time.Duration
	GitBin        string
	Stdout        io.Writer
}

type StreamSmokeResult struct {
	Status             string `json:"status"`
	URL                string `json:"url"`
	Project            string `json:"project"`
	Change             string `json:"change"`
	Revision           string `json:"revision"`
	FetchRef           string `json:"fetch_ref"`
	SSHHost            string `json:"ssh_host"`
	SSHPort            int    `json:"ssh_port"`
	KnownHostsVerified bool   `json:"known_hosts_verified"`
	StreamObserved     bool   `json:"stream_observed"`
}

func RunStreamSmoke(ctx context.Context, opts StreamSmokeOptions) (StreamSmokeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	opts = normalizeStreamSmokeOptions(opts)
	if err := validateStreamSmokeOptions(opts); err != nil {
		return StreamSmokeResult{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	runner := smokeRunner{
		opts: SmokeOptions{
			URL:           opts.URL,
			AccountID:     opts.AccountID,
			Username:      opts.Username,
			Project:       opts.Project,
			ProjectPrefix: opts.ProjectPrefix,
			Timeout:       opts.Timeout,
			GitBin:        opts.GitBin,
			Stdout:        opts.Stdout,
		},
		client: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}

	if err := runner.waitForGerrit(ctx); err != nil {
		return StreamSmokeResult{}, err
	}

	return runner.runStreamSmoke(ctx, opts)
}

func normalizeStreamSmokeOptions(opts StreamSmokeOptions) StreamSmokeOptions {
	base := normalizeSmokeOptions(SmokeOptions{
		URL:           opts.URL,
		AccountID:     opts.AccountID,
		Username:      opts.Username,
		Project:       opts.Project,
		ProjectPrefix: opts.ProjectPrefix,
		Timeout:       opts.Timeout,
		GitBin:        opts.GitBin,
		Stdout:        opts.Stdout,
	})

	opts.URL = base.URL
	opts.AccountID = base.AccountID
	opts.Username = base.Username
	opts.Project = base.Project
	opts.ProjectPrefix = base.ProjectPrefix
	opts.Timeout = base.Timeout
	opts.GitBin = base.GitBin
	opts.Stdout = base.Stdout

	opts.SSHHost = strings.TrimSpace(opts.SSHHost)
	if opts.SSHHost == "" {
		opts.SSHHost = DefaultStreamSmokeSSHHost
	}

	if opts.SSHPort <= 0 {
		opts.SSHPort = DefaultStreamSmokeSSHPort
	}

	return opts
}

func validateStreamSmokeOptions(opts StreamSmokeOptions) error {
	if err := validateSmokeOptions(SmokeOptions{
		URL:       opts.URL,
		AccountID: opts.AccountID,
		Username:  opts.Username,
		Project:   opts.Project,
		Timeout:   opts.Timeout,
		GitBin:    opts.GitBin,
		Stdout:    opts.Stdout,
	}); err != nil {
		return err
	}

	if opts.SSHHost == "" {
		return fmt.Errorf("gerrit stream smoke ssh host is required")
	}

	if opts.SSHPort <= 0 {
		return fmt.Errorf("gerrit stream smoke ssh port must be > 0")
	}

	return nil
}

func (r smokeRunner) runStreamSmoke(ctx context.Context, opts StreamSmokeOptions) (StreamSmokeResult, error) {
	accessToken, err := r.loginDevelopmentAccount(ctx)
	if err != nil {
		return StreamSmokeResult{}, err
	}

	password, err := r.generateHTTPPassword(ctx, accessToken)
	if err != nil {
		return StreamSmokeResult{}, err
	}

	if err := r.createProject(ctx, password); err != nil {
		return StreamSmokeResult{}, err
	}

	changeID, err := randomChangeID()
	if err != nil {
		return StreamSmokeResult{}, err
	}

	workspaceRoot, err := os.MkdirTemp("", "vectis-gerrit-stream-smoke-*")
	if err != nil {
		return StreamSmokeResult{}, fmt.Errorf("create gerrit stream smoke workspace: %w", err)
	}
	defer os.RemoveAll(workspaceRoot)

	keyFile, signer, err := writeStreamSmokeSSHKey(workspaceRoot)
	if err != nil {
		return StreamSmokeResult{}, err
	}

	if err := r.addSSHKey(ctx, password, signer.PublicKey()); err != nil {
		return StreamSmokeResult{}, err
	}

	hostKey, err := r.waitForSSHKeyAuth(ctx, opts, signer)
	if err != nil {
		return StreamSmokeResult{}, err
	}

	knownHostsFile, err := writeStreamSmokeKnownHosts(workspaceRoot, opts, hostKey)
	if err != nil {
		return StreamSmokeResult{}, err
	}

	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()

	ready := make(chan struct{})
	events := make(chan scm.Event, 8)
	streamErr := make(chan error, 1)
	go func() {
		streamErr <- sshstream.ConsumeOnce(streamCtx, sshstream.Options{
			Host:           opts.SSHHost,
			Port:           opts.SSHPort,
			User:           opts.Username,
			KeyFile:        keyFile,
			KnownHostsFile: knownHostsFile,
			Command:        "gerrit stream-events",
			ConnectTimeout: 10 * time.Second,
		}, func(ctx context.Context, reader io.Reader) error {
			close(ready)
			return scmgerrit.ConsumeStream(ctx, reader, scmgerrit.StreamOptions{
				Provider: "gerrit",
				BaseURL:  opts.URL,
			}, func(ctx context.Context, event scm.Event) error {
				info, err := scmgerrit.StreamEventInfoFromEvent(event)
				if err != nil {
					return err
				}

				if info.Project == opts.Project && info.Branch == "master" && info.ChangeID == changeID {
					select {
					case events <- event:
					default:
					}
				}

				return nil
			})
		}, interfaces.NewLogger("gerrit-stream-smoke"))
	}()

	if err := waitForStreamReady(ctx, ready, streamErr); err != nil {
		return StreamSmokeResult{}, err
	}

	if err := r.pushChange(ctx, workspaceRoot, password, changeID); err != nil {
		return StreamSmokeResult{}, err
	}

	event, err := waitForStreamEvent(ctx, events, streamErr)
	if err != nil {
		return StreamSmokeResult{}, err
	}

	stopStream()
	_ = drainStreamErr(streamErr)

	change := fmt.Sprintf("%s~master~%s", opts.Project, changeID)
	client := gerritaction.Client{
		BaseURL:    opts.URL,
		Username:   opts.Username,
		Password:   password,
		HTTPClient: r.client,
	}

	info, err := client.ChangeDetail(ctx, change)
	if err != nil {
		return StreamSmokeResult{}, err
	}

	revision, fetchRef, err := info.CurrentRevisionRef()
	if err != nil {
		return StreamSmokeResult{}, err
	}

	streamInfo, err := scmgerrit.StreamEventInfoFromEvent(event)
	if err != nil {
		return StreamSmokeResult{}, err
	}
	if streamInfo.CurrentRevision != revision || streamInfo.Ref != fetchRef {
		return StreamSmokeResult{}, fmt.Errorf("gerrit stream event revision/ref %s/%s, detail returned %s/%s", streamInfo.CurrentRevision, streamInfo.Ref, revision, fetchRef)
	}

	return StreamSmokeResult{
		Status:             "ok",
		URL:                opts.URL,
		Project:            opts.Project,
		Change:             change,
		Revision:           revision,
		FetchRef:           fetchRef,
		SSHHost:            opts.SSHHost,
		SSHPort:            opts.SSHPort,
		KnownHostsVerified: true,
		StreamObserved:     true,
	}, nil
}

func writeStreamSmokeSSHKey(workspaceRoot string) (string, ssh.Signer, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", nil, fmt.Errorf("generate stream smoke SSH key: %w", err)
	}

	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		return "", nil, fmt.Errorf("create stream smoke SSH signer: %w", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	keyFile := filepath.Join(workspaceRoot, "id_rsa")
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		return "", nil, fmt.Errorf("write stream smoke SSH key: %w", err)
	}

	return keyFile, signer, nil
}

func writeStreamSmokeKnownHosts(workspaceRoot string, opts StreamSmokeOptions, key ssh.PublicKey) (string, error) {
	knownHostsFile := filepath.Join(workspaceRoot, "known_hosts")
	host := net.JoinHostPort(opts.SSHHost, strconv.Itoa(opts.SSHPort))
	line := knownhosts.Line([]string{host}, key)
	if err := os.WriteFile(knownHostsFile, []byte(line+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("write stream smoke known_hosts: %w", err)
	}

	return knownHostsFile, nil
}

func (r smokeRunner) addSSHKey(ctx context.Context, password string, key ssh.PublicKey) error {
	endpoint := r.opts.URL + "/a/accounts/self/sshkeys"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(ssh.MarshalAuthorizedKey(key))))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=UTF-8")
	req.SetBasicAuth(r.opts.Username, password)

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("add gerrit SSH key: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("add gerrit SSH key failed: status=%d", resp.StatusCode)
	}

	return nil
}

func (r smokeRunner) waitForSSHKeyAuth(ctx context.Context, opts StreamSmokeOptions, signer ssh.Signer) (ssh.PublicKey, error) {
	address := net.JoinHostPort(opts.SSHHost, strconv.Itoa(opts.SSHPort))
	var lastErr error
	for {
		key, err := captureSSHHostKey(ctx, address, opts.Username, signer)
		if err == nil {
			return key, nil
		}

		lastErr = err

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("gerrit stream smoke did not authenticate to SSH %s: %w", address, lastErr)
		case <-time.After(time.Second):
			fmt.Fprintf(r.opts.Stdout, "Waiting for Gerrit SSH %s: %v\n", address, lastErr)
		}
	}
}

func captureSSHHostKey(ctx context.Context, address, username string, signer ssh.Signer) (ssh.PublicKey, error) {
	var observed ssh.PublicKey
	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			observed = key
			return nil
		},
		Timeout: 10 * time.Second,
	}

	client, err := sshstream.DialClient(ctx, address, 10*time.Second, config)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if observed == nil {
		return nil, fmt.Errorf("SSH handshake did not expose host key")
	}

	return observed, nil
}

func waitForStreamReady(ctx context.Context, ready <-chan struct{}, streamErr <-chan error) error {
	select {
	case <-ready:
		return nil
	case err := <-streamErr:
		if err == nil {
			return fmt.Errorf("gerrit stream ended before becoming ready")
		}
		return fmt.Errorf("gerrit stream failed before becoming ready: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitForStreamEvent(ctx context.Context, events <-chan scm.Event, streamErr <-chan error) (scm.Event, error) {
	for {
		select {
		case event := <-events:
			return event, nil
		case err := <-streamErr:
			if err == nil {
				return scm.Event{}, fmt.Errorf("gerrit stream ended before target event")
			}

			if errors.Is(err, context.Canceled) {
				return scm.Event{}, err
			}

			return scm.Event{}, fmt.Errorf("gerrit stream failed before target event: %w", err)
		case <-ctx.Done():
			return scm.Event{}, ctx.Err()
		}
	}
}

func drainStreamErr(streamErr <-chan error) error {
	select {
	case err := <-streamErr:
		if errors.Is(err, context.Canceled) {
			return nil
		}

		return err
	case <-time.After(time.Second):
		return nil
	}
}
