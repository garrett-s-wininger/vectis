package secrets

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidResolveIdentity = errors.New("secrets: invalid resolve identity")

func ValidateResolveIdentityBinding(req *ResolveRequest) error {
	if req == nil {
		return fmt.Errorf("%w: resolve request is required", ErrInvalidResolveIdentity)
	}

	req.RunID = strings.TrimSpace(req.RunID)
	req.ExecutionID = strings.TrimSpace(req.ExecutionID)
	if req.RunID == "" {
		return fmt.Errorf("%w: run_id is required", ErrInvalidResolveIdentity)
	}

	if req.ExecutionID == "" {
		return fmt.Errorf("%w: execution_id is required", ErrInvalidResolveIdentity)
	}

	if err := normalizeResolveSPIFFEIDField("peer SPIFFE ID", &req.PeerSPIFFEID, false); err != nil {
		return err
	}

	if err := normalizeExecutionScope(&req.Scope); err != nil {
		return err
	}

	if req.Workload != nil {
		workload := req.Workload
		workload.RunID = strings.TrimSpace(workload.RunID)
		workload.ExecutionID = strings.TrimSpace(workload.ExecutionID)
		workload.TrustDomain = strings.ToLower(strings.TrimSpace(workload.TrustDomain))
		workload.NamespacePath = strings.TrimSpace(workload.NamespacePath)
		workload.CellID = strings.TrimSpace(workload.CellID)
		workload.JobID = strings.TrimSpace(workload.JobID)

		if err := normalizeResolveSPIFFEIDField("workload SPIFFE ID", &workload.SPIFFEID, true); err != nil {
			return err
		}

		if workload.X509SVID != nil {
			if err := normalizeResolveSPIFFEIDField("workload X.509-SVID SPIFFE ID", &workload.X509SVID.SPIFFEID, true); err != nil {
				return err
			}

			if err := sameResolveIdentityField("workload SPIFFE ID", workload.SPIFFEID, "workload X.509-SVID SPIFFE ID", workload.X509SVID.SPIFFEID); err != nil {
				return err
			}
		}

		if err := sameResolveIdentityField("request run_id", req.RunID, "workload run_id", workload.RunID); err != nil {
			return err
		}

		if err := sameResolveIdentityField("request execution_id", req.ExecutionID, "workload execution_id", workload.ExecutionID); err != nil {
			return err
		}

		if err := sameResolveIdentityField("peer SPIFFE ID", req.PeerSPIFFEID, "workload SPIFFE ID", workload.SPIFFEID); err != nil {
			return err
		}
	}

	if resolveScopePresent(req.Scope) {
		scope := req.Scope
		if err := sameResolveIdentityField("request run_id", req.RunID, "execution scope run_id", scope.RunID); err != nil {
			return err
		}

		if err := sameResolveIdentityField("request execution_id", req.ExecutionID, "execution scope execution_id", scope.ExecutionID); err != nil {
			return err
		}

		if err := sameResolveIdentityField("peer SPIFFE ID", req.PeerSPIFFEID, "execution scope SPIFFE ID", scope.SPIFFEID); err != nil {
			return err
		}

		if req.Workload != nil {
			workload := req.Workload
			if err := sameResolveIdentityField("workload SPIFFE ID", workload.SPIFFEID, "execution scope SPIFFE ID", scope.SPIFFEID); err != nil {
				return err
			}

			if err := sameResolveIdentityField("workload trust domain", workload.TrustDomain, "execution scope trust domain", scope.TrustDomain); err != nil {
				return err
			}

			if err := sameResolveIdentityField("workload namespace", workload.NamespacePath, "execution scope namespace", scope.NamespacePath); err != nil {
				return err
			}

			if err := sameResolveIdentityField("workload cell_id", workload.CellID, "execution scope cell_id", scope.CellID); err != nil {
				return err
			}

			if err := sameResolveIdentityField("workload job_id", workload.JobID, "execution scope job_id", scope.JobID); err != nil {
				return err
			}
		}
	}

	return nil
}

func normalizeExecutionScope(scope *ExecutionScope) error {
	if scope == nil || !resolveScopePresent(*scope) {
		return nil
	}

	scope.RunID = strings.TrimSpace(scope.RunID)
	scope.ExecutionID = strings.TrimSpace(scope.ExecutionID)
	scope.TrustDomain = strings.ToLower(strings.TrimSpace(scope.TrustDomain))
	scope.NamespacePath = strings.TrimSpace(scope.NamespacePath)
	scope.CellID = strings.TrimSpace(scope.CellID)
	scope.JobID = strings.TrimSpace(scope.JobID)
	scope.TaskID = strings.TrimSpace(scope.TaskID)
	scope.TaskKey = strings.TrimSpace(scope.TaskKey)
	scope.SegmentID = strings.TrimSpace(scope.SegmentID)
	scope.DefinitionHash = strings.TrimSpace(scope.DefinitionHash)

	return normalizeResolveSPIFFEIDField("execution scope SPIFFE ID", &scope.SPIFFEID, true)
}

func normalizeResolveSPIFFEIDField(label string, value *string, required bool) error {
	if value == nil {
		if required {
			return fmt.Errorf("%w: %s is required", ErrInvalidResolveIdentity, label)
		}

		return nil
	}

	raw := strings.TrimSpace(*value)
	if raw == "" {
		if required {
			return fmt.Errorf("%w: %s is required", ErrInvalidResolveIdentity, label)
		}

		*value = ""
		return nil
	}

	normalized, err := normalizePeerSPIFFEID(raw)
	if err != nil {
		return fmt.Errorf("%w: %s is invalid: %w", ErrInvalidResolveIdentity, label, err)
	}

	*value = normalized
	return nil
}

func sameResolveIdentityField(leftLabel, left, rightLabel, right string) error {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return nil
	}

	if left != right {
		return fmt.Errorf("%w: %s %q does not match %s %q", ErrInvalidResolveIdentity, leftLabel, left, rightLabel, right)
	}

	return nil
}

func resolveScopePresent(scope ExecutionScope) bool {
	return strings.TrimSpace(scope.SPIFFEID) != "" ||
		strings.TrimSpace(scope.TrustDomain) != "" ||
		strings.TrimSpace(scope.NamespacePath) != "" ||
		strings.TrimSpace(scope.CellID) != "" ||
		strings.TrimSpace(scope.JobID) != "" ||
		strings.TrimSpace(scope.RunID) != "" ||
		scope.RunIndex != 0 ||
		strings.TrimSpace(scope.TaskID) != "" ||
		strings.TrimSpace(scope.TaskKey) != "" ||
		strings.TrimSpace(scope.SegmentID) != "" ||
		strings.TrimSpace(scope.ExecutionID) != "" ||
		scope.Attempt != 0 ||
		scope.DefinitionVersion != 0 ||
		strings.TrimSpace(scope.DefinitionHash) != ""
}
