package audit

import (
	"fmt"
	"sort"
	"strings"
)

// Durability describes how hard the API should try to persist an audit event.
type Durability string

const (
	DurabilityDisabled          Durability = "disabled"
	DurabilityBestEffort        Durability = "best_effort"
	DurabilityDurableBestEffort Durability = "durable_best_effort"
	DurabilityFailClosed        Durability = "fail_closed"
)

// EventDefinition is the central registry entry for an audit event type.
type EventDefinition struct {
	Type              string
	DefaultDurability Durability
}

// Policy controls whether audit emission is enabled and which durability each
// event type uses.
type Policy struct {
	Enabled   bool
	Overrides map[string]Durability
}

// DefaultPolicy enables auditing with the per-event defaults from EventDefinitions.
func DefaultPolicy() Policy {
	return Policy{Enabled: true}
}

// EventDefinitions is the source of truth for emitted API audit event policy.
var EventDefinitions = []EventDefinition{
	{Type: EventTokenCreated, DefaultDurability: DurabilityFailClosed},
	{Type: EventTokenDeleted, DefaultDurability: DurabilityFailClosed},
	{Type: EventPasswordChanged, DefaultDurability: DurabilityFailClosed},
	{Type: EventUserCreated, DefaultDurability: DurabilityFailClosed},
	{Type: EventUserUpdated, DefaultDurability: DurabilityFailClosed},
	{Type: EventUserDeleted, DefaultDurability: DurabilityFailClosed},
	{Type: EventBindingCreated, DefaultDurability: DurabilityFailClosed},
	{Type: EventBindingDeleted, DefaultDurability: DurabilityFailClosed},
	{Type: EventSetupCompleted, DefaultDurability: DurabilityFailClosed},
	{Type: EventSetupBootstrapFailed, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventNamespaceCreated, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventNamespaceDeleted, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventJobCreated, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventJobDeleted, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventJobUpdated, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventSourceRepositoryCreated, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventSourceRepositoryUpdated, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventSourceRepositoryDeleted, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventSourceRepositorySyncRequested, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventSourceScheduleUpdated, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventSourceScheduleDeleted, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventSourceScheduleOverrideSet, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventSourceScheduleOverrideCleared, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventRunRepairMarked, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventRunForceFailed, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventRunForceRequeued, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventRunCancelled, DefaultDurability: DurabilityDurableBestEffort},
	{Type: EventAuthSuccess, DefaultDurability: DurabilityBestEffort},
	{Type: EventAuthFailure, DefaultDurability: DurabilityBestEffort},
	{Type: EventAuthLogout, DefaultDurability: DurabilityBestEffort},
	{Type: EventRunTriggered, DefaultDurability: DurabilityBestEffort},
}

var eventDefinitionsByType = func() map[string]EventDefinition {
	out := make(map[string]EventDefinition, len(EventDefinitions))
	for _, def := range EventDefinitions {
		out[def.Type] = def
	}

	return out
}()

// DefinitionFor returns the registered definition for eventType.
func DefinitionFor(eventType string) (EventDefinition, bool) {
	def, ok := eventDefinitionsByType[eventType]
	return def, ok
}

// DurabilityFor returns the effective durability for eventType under p.
func (p Policy) DurabilityFor(eventType string) Durability {
	if !p.Enabled {
		return DurabilityDisabled
	}

	if d, ok := p.Overrides[eventType]; ok {
		return d
	}

	if def, ok := DefinitionFor(eventType); ok {
		return def.DefaultDurability
	}

	return DurabilityBestEffort
}

// ParseDurability parses a config value into a durability.
func ParseDurability(s string) (Durability, error) {
	switch Durability(strings.ToLower(strings.TrimSpace(s))) {
	case DurabilityDisabled:
		return DurabilityDisabled, nil
	case DurabilityBestEffort:
		return DurabilityBestEffort, nil
	case DurabilityDurableBestEffort:
		return DurabilityDurableBestEffort, nil
	case DurabilityFailClosed:
		return DurabilityFailClosed, nil
	default:
		return "", fmt.Errorf("unknown audit durability %q", s)
	}
}

// ParseDurabilityOverrides parses comma-separated event=durability pairs.
func ParseDurabilityOverrides(s string) (map[string]Durability, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return map[string]Durability{}, nil
	}

	overrides := map[string]Durability{}
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		eventType, value, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("audit override %q must be event=durability", part)
		}

		eventType = strings.TrimSpace(eventType)
		if _, ok := DefinitionFor(eventType); !ok {
			return nil, fmt.Errorf("audit override references unknown event %q", eventType)
		}

		d, err := ParseDurability(value)
		if err != nil {
			return nil, err
		}

		overrides[eventType] = d
	}

	return overrides, nil
}

// RegisteredEventTypes returns stable event names for docs and diagnostics.
func RegisteredEventTypes() []string {
	types := make([]string, 0, len(EventDefinitions))
	for _, def := range EventDefinitions {
		types = append(types, def.Type)
	}

	sort.Strings(types)
	return types
}
