package scmstream

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/scmtrigger"
	"vectis/sdk/scm"
)

const DefaultSpecLimit = 1000

type SpecRepository interface {
	ListEnabledByProvider(ctx context.Context, provider string, limit int) ([]dal.SCMPollTriggerSpec, error)
}

type EventProcessor interface {
	HandleEvent(ctx context.Context, spec dal.SCMPollTriggerSpec, event scm.Event) (scmtrigger.HandleResult, error)
}

type Matcher func(dal.SCMPollTriggerSpec, scm.Event) bool

type Router struct {
	Specs     SpecRepository
	Processor EventProcessor
	Logger    interfaces.Logger
	Matcher   Matcher
	Limit     int
}

type EventTarget struct {
	Provider string
	BaseURL  string
	Project  string
	Branch   string
}

type RouteResult struct {
	Candidates        int
	Matched           int
	Handled           int
	EventsCreated     int
	RunsCreated       int
	RunsReused        int
	Dispatched        int
	AlreadyDispatched int
}

func (r Router) HandleEvent(ctx context.Context, target EventTarget, event scm.Event) (RouteResult, error) {
	if r.Specs == nil {
		return RouteResult{}, fmt.Errorf("scm stream spec repository is required")
	}

	if r.Processor == nil {
		return RouteResult{}, fmt.Errorf("scm stream event processor is required")
	}

	provider := strings.ToLower(strings.TrimSpace(target.Provider))
	if provider == "" {
		return RouteResult{}, fmt.Errorf("scm stream provider is required")
	}

	if strings.TrimSpace(event.Key) == "" {
		return RouteResult{}, fmt.Errorf("scm stream event key is required")
	}

	specs, err := r.Specs.ListEnabledByProvider(ctx, provider, r.limit())
	if err != nil {
		return RouteResult{}, fmt.Errorf("list enabled scm trigger specs for %q: %w", provider, err)
	}

	result := RouteResult{Candidates: len(specs)}
	for _, spec := range specs {
		if !targetMatchesSpec(target, spec) {
			continue
		}

		if r.Matcher != nil && !r.Matcher(spec, event) {
			continue
		}

		result.Matched++
		handled, err := r.Processor.HandleEvent(ctx, spec, event)
		if err != nil {
			return result, err
		}

		result.Handled++
		if handled.EventCreated {
			result.EventsCreated++
		}

		if handled.RunCreated {
			result.RunsCreated++
		}

		if handled.Dispatched {
			result.Dispatched++
			if !handled.RunCreated {
				result.RunsReused++
			}
		}

		if handled.AlreadyDispatched {
			result.AlreadyDispatched++
		}
	}

	if result.Handled == 0 {
		r.logger().Debug("scm stream event %s matched no enabled %s trigger specs", event.Key, provider)
	}

	return result, nil
}

func targetMatchesSpec(target EventTarget, spec dal.SCMPollTriggerSpec) bool {
	if !sameProvider(target.Provider, spec.Provider) {
		return false
	}

	if !sameBaseURL(target.BaseURL, spec.BaseURL) {
		return false
	}

	if !fieldMatchesSpec(target.Project, spec.Project) {
		return false
	}

	if !fieldMatchesSpec(target.Branch, spec.Branch) {
		return false
	}

	return true
}

func sameProvider(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}

func sameBaseURL(a, b string) bool {
	return strings.TrimRight(strings.TrimSpace(a), "/") == strings.TrimRight(strings.TrimSpace(b), "/")
}

func fieldMatchesSpec(value, specValue string) bool {
	specValue = strings.TrimSpace(specValue)
	return specValue == "" || strings.TrimSpace(value) == specValue
}

func (r Router) limit() int {
	if r.Limit > 0 {
		return r.Limit
	}

	return DefaultSpecLimit
}

func (r Router) logger() interfaces.Logger {
	if r.Logger != nil {
		return r.Logger
	}

	return interfaces.NewLogger("scm-stream")
}
