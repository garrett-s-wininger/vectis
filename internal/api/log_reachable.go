package api

import (
	"net/http"

	"google.golang.org/grpc/connectivity"
)

type logReachableResponse struct {
	Reachable bool   `json:"reachable"`
	State     string `json:"state"`
}

func (s *APIServer) GetLogReachable(w http.ResponseWriter, _ *http.Request) {
	holder := s.logClient.Load()
	if holder == nil || holder.conn == nil {
		writeJSON(w, http.StatusOK, logReachableResponse{Reachable: false, State: "not_connected"})
		return
	}

	gs := holder.conn.GetState()
	reachable := gs == connectivity.Ready || gs == connectivity.Idle

	writeJSON(w, http.StatusOK, logReachableResponse{
		Reachable: reachable,
		State:     gs.String(),
	})
}
