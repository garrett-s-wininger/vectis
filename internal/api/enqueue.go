package api

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/database"
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

	endpoints, err := config.APICellIngressEndpoints()
	if err != nil {
		return nil, fmt.Errorf("cell ingress endpoints: %w", err)
	}

	if database.GlobalAndCellDatabasesAreSplit() {
		q = nil
	}

	return cell.NewExecutionRouterWithOptions(config.CellID(), q, endpoints, s.logger, cell.ExecutionRouterOptions{
		TLSConfigForEndpoint: config.CellIngressHTTPClientTLSConfig,
	}), nil
}
