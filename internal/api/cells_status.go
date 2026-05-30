package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"vectis/internal/config"
)

const cellStatusTimeout = 2 * time.Second

type cellsStatusResponse struct {
	Cells []cellStatusResponse `json:"cells"`
}

type cellStatusResponse struct {
	CellID            string `json:"cell_id"`
	IngressConfigured bool   `json:"ingress_configured"`
	IngressReachable  bool   `json:"ingress_reachable"`
	Status            string `json:"status"`
	HTTPStatus        int    `json:"http_status,omitempty"`
	Error             string `json:"error,omitempty"`
}

func (s *APIServer) GetCellsStatus(w http.ResponseWriter, r *http.Request) {
	endpoints, err := config.APICellIngressEndpoints()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "invalid_cell_ingress_endpoints", err.Error(), nil)
		return
	}

	cellIDs := make([]string, 0, len(endpoints))
	for cellID := range endpoints {
		cellIDs = append(cellIDs, cellID)
	}
	sort.Strings(cellIDs)

	cells := make([]cellStatusResponse, 0, len(cellIDs))
	for _, cellID := range cellIDs {
		cells = append(cells, checkCellIngressReady(r.Context(), cellID, endpoints[cellID]))
	}

	writeJSON(w, http.StatusOK, cellsStatusResponse{Cells: cells})
}

func checkCellIngressReady(ctx context.Context, cellID, endpoint string) cellStatusResponse {
	resp := cellStatusResponse{
		CellID:            cellID,
		IngressConfigured: true,
		Status:            "unknown",
	}

	readyURL, err := cellIngressReadyURL(endpoint)
	if err != nil {
		resp.Status = "invalid"
		resp.Error = err.Error()
		return resp
	}

	reqCtx, cancel := context.WithTimeout(ctx, cellStatusTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, readyURL, nil)
	if err != nil {
		resp.Status = "invalid"
		resp.Error = err.Error()
		return resp
	}

	httpResp, err := http.DefaultClient.Do(req)
	if err != nil {
		resp.Status = "unreachable"
		resp.Error = err.Error()
		return resp
	}
	defer httpResp.Body.Close()

	resp.HTTPStatus = httpResp.StatusCode
	if httpResp.StatusCode >= http.StatusOK && httpResp.StatusCode < http.StatusMultipleChoices {
		resp.IngressReachable = true
		resp.Status = "ready"
		return resp
	}

	resp.Status = "unhealthy"
	resp.Error = httpResp.Status
	return resp
}

func cellIngressReadyURL(endpoint string) (string, error) {
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

	u.Path = strings.TrimRight(u.Path, "/") + "/health/ready"
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}
