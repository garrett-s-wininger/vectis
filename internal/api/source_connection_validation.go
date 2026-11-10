package api

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"vectis/internal/dal"
	sourcepkg "vectis/internal/source"
)

func (s *APIServer) validateSourceConnectionBeforeSave(ctx context.Context, w http.ResponseWriter, rec dal.SourceRepositoryRecord) (sourcepkg.GitCheckoutStatus, bool) {
	if rec.CheckoutMode == dal.SourceCheckoutModeManaged && strings.TrimSpace(rec.CanonicalURL) == "" && len(rec.FallbackRemoteURLs) == 0 {
		rec.CheckoutMode = dal.SourceCheckoutModeExternal
	}

	if rec.CheckoutMode == dal.SourceCheckoutModeManaged {
		dir, err := os.MkdirTemp("", "vectis-source-validation-")
		if err != nil {
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return sourcepkg.GitCheckoutStatus{}, false
		}
		defer func() {
			if err := os.RemoveAll(dir); err != nil {
				s.logger.Error("Removing source connection validation checkout: %v", err)
			}
		}()

		rec.CheckoutPath = filepath.Join(dir, "checkout")
	}

	status := s.sourceRepositorySyncCheckoutStatus(ctx, rec, sourceRepositorySyncRef(rec))
	if status.ErrorCode != "" || !status.DefaultRefResolved {
		message := "The selected ref could not be resolved."
		if status.ErrorCode != "" {
			message = sourceRepositoryStatusSyncError(status)
		}

		writeAPIError(w, http.StatusUnprocessableEntity, "source_connection_validation_failed", message, nil)
		return sourcepkg.GitCheckoutStatus{}, false
	}

	return status, true
}

type sourceConnectionValidationResponse struct {
	RequestedRef   string `json:"requested_ref"`
	ResolvedCommit string `json:"resolved_commit"`
}
