package api

import (
	"net/http"
	"strconv"
)

const (
	defaultPageLimit = 50
	maxPageLimit     = 200
)

type pageParams struct {
	Cursor int64
	Limit  int
}

func parsePageParams(r *http.Request) pageParams {
	p := pageParams{Limit: defaultPageLimit}
	if c := r.URL.Query().Get("cursor"); c != "" {
		if v, err := strconv.ParseInt(c, 10, 64); err == nil && v > 0 {
			p.Cursor = v
		}
	}

	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			p.Limit = v

			if p.Limit > maxPageLimit {
				p.Limit = maxPageLimit
			}
		}
	}

	return p
}

type paginatedResponse struct {
	Data       any    `json:"data"`
	NextCursor *int64 `json:"next_cursor,omitempty"`
}

func buildPaginatedResponse(data any, nextCursor int64) paginatedResponse {
	if nextCursor > 0 {
		return paginatedResponse{Data: data, NextCursor: &nextCursor}
	}

	return paginatedResponse{Data: data}
}
