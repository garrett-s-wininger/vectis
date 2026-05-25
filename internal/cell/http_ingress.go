package cell

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"vectis/internal/interfaces"

	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultHTTPIngressTimeout = 30 * time.Second
	httpIngressErrorBytes     = 4096
)

type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type HTTPExecutionIngress struct {
	endpoint string
	client   HTTPDoer
	logger   interfaces.Logger
}

type httpExecutionRequest struct {
	JobRequest json.RawMessage `json:"job_request"`
}

func NewHTTPExecutionIngress(endpoint string, client HTTPDoer, logger interfaces.Logger) HTTPExecutionIngress {
	endpoint = strings.TrimSpace(endpoint)
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPIngressTimeout}
	}

	return HTTPExecutionIngress{
		endpoint: endpoint,
		client:   client,
		logger:   logger,
	}
}

func (i HTTPExecutionIngress) SubmitExecution(ctx context.Context, submission ExecutionSubmission) error {
	if submission.Request == nil {
		return fmt.Errorf("job request is required")
	}

	endpoint, err := cellIngressExecutionURL(i.endpoint)
	if err != nil {
		return err
	}

	jobRequest, err := protojson.Marshal(submission.Request)
	if err != nil {
		return fmt.Errorf("marshal job request: %w", err)
	}

	body, err := json.Marshal(httpExecutionRequest{JobRequest: jobRequest})
	if err != nil {
		return fmt.Errorf("marshal execution request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build execution request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := i.client.Do(req)
	if err != nil {
		return fmt.Errorf("submit execution to cell ingress: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && i.logger != nil {
			i.logger.Warn("Cell ingress response close failed: %v", err)
		}
	}()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	errBody, _ := io.ReadAll(io.LimitReader(resp.Body, httpIngressErrorBytes))
	return fmt.Errorf("cell ingress returned %s: %s", resp.Status, strings.TrimSpace(string(errBody)))
}

func cellIngressExecutionURL(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", fmt.Errorf("cell ingress endpoint is required")
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse cell ingress endpoint: %w", err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("cell ingress endpoint must use http or https")
	}

	if strings.TrimSpace(u.Host) == "" {
		return "", fmt.Errorf("cell ingress endpoint host is required")
	}

	u.Path = strings.TrimRight(u.Path, "/") + "/cell/v1/executions"
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

var _ ExecutionIngress = HTTPExecutionIngress{}
