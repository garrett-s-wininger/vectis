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
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/reconciler"
)

const cellStatusTimeout = 2 * time.Second

type cellsStatusResponse struct {
	Cells []cellStatusResponse `json:"cells"`
}

type cellStatusResponse struct {
	CellID            string `json:"cell_id"`
	IngressRequired   bool   `json:"ingress_required"`
	IngressConfigured bool   `json:"ingress_configured"`
	IngressReachable  bool   `json:"ingress_reachable"`
	Status            string `json:"status"`
	HTTPStatus        int    `json:"http_status,omitempty"`
	Error             string `json:"error,omitempty"`
	Queued            int64  `json:"queued"`
	Stuck             int64  `json:"stuck"`
	CatalogPending    int64  `json:"catalog_pending"`
	CatalogFailed     int64  `json:"catalog_failed"`
	CatalogTotal      int64  `json:"catalog_total"`
}

func (s *APIServer) GetCellsStatus(w http.ResponseWriter, r *http.Request) {
	endpoints, err := config.APICellIngressEndpoints()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "invalid_cell_ingress_endpoints", err.Error(), nil)
		return
	}

	known := make(map[string]*cellStatusResponse, len(endpoints))
	localCellID := config.CellID()
	splitDatabases := database.GlobalAndCellDatabasesAreSplit()

	cellIDs := make([]string, 0, len(endpoints))
	for cellID := range endpoints {
		cellIDs = append(cellIDs, cellID)
	}
	sort.Strings(cellIDs)

	for _, cellID := range cellIDs {
		status := checkCellIngressReady(r.Context(), cellID, endpoints[cellID])
		known[cellID] = &status
	}

	if s.runs != nil || s.catalogEvents != nil {
		ctx, cancel := s.handlerDBCtx(r)
		defer cancel()

		if s.runs != nil {
			if err := s.addRunCellStatus(ctx, known, localCellID, splitDatabases); err != nil {
				if s.handleDBUnavailableError(w, err) {
					return
				}

				s.logger.Error("cell run status query failed: %v", err)
				writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
				return
			}
		}

		if s.catalogEvents != nil {
			if err := s.addCatalogCellStatus(ctx, known, localCellID, splitDatabases); err != nil {
				if s.handleDBUnavailableError(w, err) {
					return
				}

				s.logger.Error("cell catalog status query failed: %v", err)
				writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
				return
			}
		}
	}

	cellIDs = cellIDs[:0]
	for cellID := range known {
		cellIDs = append(cellIDs, cellID)
	}
	sort.Strings(cellIDs)

	cells := make([]cellStatusResponse, 0, len(cellIDs))
	for _, cellID := range cellIDs {
		cells = append(cells, *known[cellID])
	}
	writeJSON(w, http.StatusOK, cellsStatusResponse{Cells: cells})
}

func checkCellIngressReady(ctx context.Context, cellID, endpoint string) cellStatusResponse {
	resp := cellStatusResponse{
		CellID:            cellID,
		IngressRequired:   true,
		IngressConfigured: true,
		Status:            "unknown",
	}

	readyURL, err := cellIngressReadyURL(endpoint)
	if err != nil {
		resp.Status = "invalid"
		resp.Error = err.Error()
		return resp
	}

	client := &http.Client{Timeout: cellStatusTimeout}
	if tlsConfig, err := config.CellIngressHTTPClientTLSConfig(endpoint); err != nil {
		resp.Status = "invalid"
		resp.Error = err.Error()
		return resp
	} else if tlsConfig != nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConfig
		client.Transport = transport
	}

	reqCtx, cancel := context.WithTimeout(ctx, cellStatusTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, readyURL, nil)
	if err != nil {
		resp.Status = "invalid"
		resp.Error = err.Error()
		return resp
	}

	httpResp, err := client.Do(req)
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

func (s *APIServer) addRunCellStatus(ctx context.Context, cells map[string]*cellStatusResponse, localCellID string, splitDatabases bool) error {
	queued, err := s.runs.CountByStatusByCell(ctx, dal.RunStatusQueued)
	if err != nil {
		return err
	}

	for _, count := range queued {
		cell := ensureCellStatus(cells, count.CellID, localCellID, splitDatabases)
		cell.Queued = count.Count
	}

	cutoff := time.Now().UTC().Add(-reconciler.MinDispatchGap).Unix()
	stuck, err := s.runs.CountStuckBeforeDispatchCutoffByCell(ctx, cutoff)
	if err != nil {
		return err
	}

	for _, count := range stuck {
		cell := ensureCellStatus(cells, count.CellID, localCellID, splitDatabases)
		cell.Stuck = count.Count
	}

	return nil
}

func (s *APIServer) addCatalogCellStatus(ctx context.Context, cells map[string]*cellStatusResponse, localCellID string, splitDatabases bool) error {
	sources, err := s.catalogEvents.SummaryBySource(ctx)
	if err != nil {
		return err
	}

	for _, source := range sources {
		cell := ensureCellStatus(cells, source.SourceCell, localCellID, splitDatabases)
		cell.CatalogPending = source.Pending
		cell.CatalogFailed = source.Failed
		cell.CatalogTotal = source.Total
	}

	return nil
}

func ensureCellStatus(cells map[string]*cellStatusResponse, cellID, localCellID string, splitDatabases bool) *cellStatusResponse {
	if existing, ok := cells[cellID]; ok {
		return existing
	}

	required := cellIngressRequired(cellID, localCellID, splitDatabases)
	status := "local"
	errorMessage := ""
	if required {
		status = "missing_route"
		errorMessage = "cell ingress endpoint is not configured"
	}

	cell := &cellStatusResponse{
		CellID:          cellID,
		IngressRequired: required,
		Status:          status,
		Error:           errorMessage,
	}

	cells[cellID] = cell
	return cell
}

func cellIngressRequired(cellID, localCellID string, splitDatabases bool) bool {
	if splitDatabases {
		return true
	}

	return strings.TrimSpace(cellID) != strings.TrimSpace(localCellID)
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
