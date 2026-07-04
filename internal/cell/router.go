package cell

import (
	"strings"

	"vectis/internal/interfaces"
)

type ExecutionRouterOptions struct {
	TLSConfigForEndpoint HTTPTLSConfigProvider
}

func NewExecutionRouter(localCellID string, queue interfaces.QueueService, endpoints map[string]string, logger interfaces.Logger) ExecutionIngress {
	return NewExecutionRouterWithOptions(localCellID, queue, endpoints, logger, ExecutionRouterOptions{})
}

func NewExecutionRouterWithOptions(localCellID string, queue interfaces.QueueService, endpoints map[string]string, logger interfaces.Logger, opts ExecutionRouterOptions) ExecutionIngress {
	localCellID = normalizeRouteCellID(localCellID)
	routes := make(map[string]ExecutionIngress, len(endpoints)+1)
	if queue != nil {
		routes[localCellID] = NewQueueExecutionIngress(queue, logger)
	}

	for cellID, endpoint := range endpoints {
		cellID = normalizeRouteCellID(cellID)
		if strings.TrimSpace(endpoint) == "" {
			continue
		}

		routes[cellID] = NewHTTPExecutionIngressWithOptions(endpoint, nil, logger, HTTPExecutionIngressOptions(opts))
	}

	return NewStaticExecutionRouter(routes)
}
