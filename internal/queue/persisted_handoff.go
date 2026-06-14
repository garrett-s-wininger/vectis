package queue

import (
	"fmt"
	"sort"
	"strings"
)

func validatePersistedQueueState(state *queueState) error {
	if state == nil {
		return nil
	}

	for i, req := range state.jobs {
		if err := validateEnqueueHandoff(req); err != nil {
			return fmt.Errorf("restore pending[%d]: %w", i, err)
		}
	}

	deliveryIDs := make([]string, 0, len(state.inflight))
	for deliveryID := range state.inflight {
		deliveryIDs = append(deliveryIDs, deliveryID)
	}

	sort.Strings(deliveryIDs)

	for _, deliveryID := range deliveryIDs {
		if strings.TrimSpace(deliveryID) == "" {
			return fmt.Errorf("restore inflight delivery id is required")
		}

		if err := validateEnqueueHandoff(state.inflight[deliveryID].JobRequest); err != nil {
			return fmt.Errorf("restore inflight[%s]: %w", deliveryID, err)
		}
	}

	for i, item := range state.deadLetter {
		if strings.TrimSpace(item.deliveryID) == "" {
			return fmt.Errorf("restore dead_letter[%d] delivery id is required", i)
		}

		if err := validateEnqueueHandoff(item.jobRequest); err != nil {
			return fmt.Errorf("restore dead_letter[%d]: %w", i, err)
		}
	}

	return nil
}
