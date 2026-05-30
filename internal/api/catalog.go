package api

import "net/http"

type catalogStatusResponse struct {
	Pending          int64                         `json:"pending"`
	Applied          int64                         `json:"applied"`
	Failed           int64                         `json:"failed"`
	Total            int64                         `json:"total"`
	LastReceivedUnix *int64                        `json:"last_received_unix,omitempty"`
	LastAppliedUnix  *int64                        `json:"last_applied_unix,omitempty"`
	Sources          []catalogStatusSourceResponse `json:"sources,omitempty"`
}

type catalogStatusSourceResponse struct {
	SourceCell       string `json:"source_cell"`
	Pending          int64  `json:"pending"`
	Applied          int64  `json:"applied"`
	Failed           int64  `json:"failed"`
	Total            int64  `json:"total"`
	LastReceivedUnix *int64 `json:"last_received_unix,omitempty"`
	LastAppliedUnix  *int64 `json:"last_applied_unix,omitempty"`
}

func (s *APIServer) GetCatalogStatus(w http.ResponseWriter, r *http.Request) {
	if s.catalogEvents == nil {
		writeJSON(w, http.StatusOK, catalogStatusResponse{})
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	summary, err := s.catalogEvents.Summary(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("catalog status query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	sourceSummaries, err := s.catalogEvents.SummaryBySource(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("catalog status by source query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	sources := make([]catalogStatusSourceResponse, 0, len(sourceSummaries))
	for _, source := range sourceSummaries {
		sources = append(sources, catalogStatusSourceResponse{
			SourceCell:       source.SourceCell,
			Pending:          source.Pending,
			Applied:          source.Applied,
			Failed:           source.Failed,
			Total:            source.Total,
			LastReceivedUnix: source.LastReceivedUnix,
			LastAppliedUnix:  source.LastAppliedUnix,
		})
	}

	writeJSON(w, http.StatusOK, catalogStatusResponse{
		Pending:          summary.Pending,
		Applied:          summary.Applied,
		Failed:           summary.Failed,
		Total:            summary.Total,
		LastReceivedUnix: summary.LastReceivedUnix,
		LastAppliedUnix:  summary.LastAppliedUnix,
		Sources:          sources,
	})
}
