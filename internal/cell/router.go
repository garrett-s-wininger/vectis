package cell

import (
	"strings"

	"vectis/internal/interfaces"
)

func NewExecutionRouter(localCellID string, queue interfaces.QueueService, endpoints map[string]string, logger interfaces.Logger) ExecutionIngress {
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

		routes[cellID] = NewHTTPExecutionIngress(endpoint, nil, logger)
	}

	return NewStaticExecutionRouter(routes)
}
