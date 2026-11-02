package cell

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"vectis/internal/dal"
)

func (i HTTPExecutionIngress) CancelRun(ctx context.Context, runID, rootExecutionID string) error {
	endpoint, err := cellIngressExecutionURL(i.endpoint)
	if err != nil {
		return err
	}

	endpoint = strings.TrimSuffix(endpoint, "/executions") + "/runs/cancel"
	body, err := json.Marshal(map[string]string{"run_id": runID, "root_execution_id": rootExecutionID})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	client, err := i.httpClient(endpoint)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("cancel run at cell ingress: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("%w: run is no longer executing", dal.ErrConflict)
	}

	body, _ = io.ReadAll(io.LimitReader(resp.Body, httpIngressErrorBytes))
	return fmt.Errorf("cell cancellation returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
}
