package api

import (
	"context"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/interfaces"
)

func (s *APIServer) submitExecution(ctx context.Context, q interfaces.QueueService, req *api.JobRequest) error {
	submission, err := cell.NewExecutionSubmission(req)
	if err != nil {
		return err
	}

	ingress, err := s.executionRouter(q)
	if err != nil {
		return err
	}

	return ingress.SubmitExecution(ctx, submission)
}

func (s *APIServer) executionRouter(q interfaces.QueueService) (cell.ExecutionIngress, error) {
	s.mu.RLock()
	if s.executionIngress != nil {
		ingress := s.executionIngress
		s.mu.RUnlock()
		return ingress, nil
	}
	s.mu.RUnlock()

	localCellID := strings.TrimSpace(config.CellID())
	routes := map[string]cell.ExecutionIngress{
		localCellID: cell.NewQueueExecutionIngress(q, s.logger),
	}

	endpoints, err := config.APICellIngressEndpoints()
	if err != nil {
		return nil, fmt.Errorf("cell ingress endpoints: %w", err)
	}

	for cellID, endpoint := range endpoints {
		cellID = strings.TrimSpace(cellID)
		if cellID == "" || cellID == localCellID {
			continue
		}

		routes[cellID] = cell.NewHTTPExecutionIngress(endpoint, nil, s.logger)
	}

	return cell.NewStaticExecutionRouter(routes), nil
}
