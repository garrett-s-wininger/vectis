package api

import (
	"fmt"
	"net/http"
)

type routeCacheMode int

const (
	routeCacheDefault routeCacheMode = iota
	routeCacheNoStore
	routeCacheHandlerManaged
)

type routeCachePolicy struct {
	mode routeCacheMode
}

func (p routeCachePolicy) validate() error {
	switch p.mode {
	case routeCacheDefault, routeCacheNoStore, routeCacheHandlerManaged:
		return nil
	default:
		return fmt.Errorf("unknown route cache mode %d", p.mode)
	}
}

func (p routeCachePolicy) shouldSetNoStore(auth routeAuthPolicy) bool {
	if p.mode == routeCacheNoStore {
		return true
	}

	if p.mode == routeCacheHandlerManaged {
		return false
	}

	return !auth.isPublic()
}

func noStoreMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setNoStore(w)
		next.ServeHTTP(w, r)
	})
}
