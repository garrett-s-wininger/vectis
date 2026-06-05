package api

import (
	"fmt"
	"net/http"

	"vectis/internal/httpsecurity"
)

type routeAcceptMode int

const (
	routeAcceptJSON routeAcceptMode = iota
	routeAcceptSSE
	routeAcceptMetrics
	routeAcceptAny
)

type routeAcceptPolicy struct {
	mode routeAcceptMode
}

func routeAcceptSSEPolicy() routeAcceptPolicy {
	return routeAcceptPolicy{mode: routeAcceptSSE}
}

func routeAcceptMetricsPolicy() routeAcceptPolicy {
	return routeAcceptPolicy{mode: routeAcceptMetrics}
}

func routeAcceptAnyPolicy() routeAcceptPolicy {
	return routeAcceptPolicy{mode: routeAcceptAny}
}

func (p routeAcceptPolicy) validate() error {
	switch p.mode {
	case routeAcceptJSON, routeAcceptSSE, routeAcceptMetrics, routeAcceptAny:
		return nil
	default:
		return fmt.Errorf("unknown accept mode %d", p.mode)
	}
}

func (p routeAcceptPolicy) allows(header string) bool {
	if p.mode == routeAcceptAny {
		return true
	}

	return httpsecurity.AcceptsAny(header, p.mediaTypes()...)
}

func (p routeAcceptPolicy) mediaTypes() []string {
	switch p.mode {
	case routeAcceptSSE:
		return []string{httpsecurity.MediaTypeEventStream}
	case routeAcceptMetrics:
		return []string{httpsecurity.MediaTypePlainText, httpsecurity.MediaTypeOpenMetricsText}
	default:
		return []string{httpsecurity.MediaTypeJSON}
	}
}

func routeAcceptMiddleware(policy routeAcceptPolicy, next http.Handler, recorders ...securityRejectionRecorder) http.Handler {
	var record securityRejectionRecorder
	if len(recorders) > 0 {
		record = recorders[0]
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !policy.allows(r.Header.Get("Accept")) {
			if record != nil {
				record(r, securityReasonNotAcceptable, http.StatusNotAcceptable)
			}

			setNoStore(w)
			writeAPIErrorCode(w, http.StatusNotAcceptable, apiErrNotAcceptable)
			return
		}

		next.ServeHTTP(w, r)
	})
}
