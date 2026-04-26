package main

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
)

type workerControlServer struct {
	api.UnimplementedWorkerControlServiceServer
	workerID   string
	cancelCh   chan string
	getRunInfo func() (runID, claimToken string)
	logger     interfaces.Logger
}

func newWorkerControlServer(workerID string, cancelCh chan string, getRunInfo func() (string, string), logger interfaces.Logger) *workerControlServer {
	return &workerControlServer{
		workerID:   workerID,
		cancelCh:   cancelCh,
		getRunInfo: getRunInfo,
		logger:     logger,
	}
}

func (s *workerControlServer) CancelRun(ctx context.Context, req *api.CancelRunRequest) (*api.Empty, error) {
	runID, claimToken := s.getRunInfo()
	if runID == "" || runID != req.GetRunId() {
		return nil, fmt.Errorf("worker is not executing run %s", req.GetRunId())
	}

	if claimToken == "" || claimToken != req.GetCancelToken() {
		return nil, fmt.Errorf("invalid cancel token for run %s", req.GetRunId())
	}

	s.logger.Info("Received cancel request for run %s", runID)
	select {
	case s.cancelCh <- runID:
	default:
		s.logger.Warn("Cancel channel full for run %s; cancel may already be in progress", runID)
	}

	return &api.Empty{}, nil
}

func startWorkerControlServer(ctx context.Context, listener net.Listener, server *workerControlServer, logger interfaces.Logger) {
	srv := grpc.NewServer()
	api.RegisterWorkerControlServiceServer(srv, server)

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	go func() {
		if err := srv.Serve(listener); err != nil {
			logger.Error("Worker control server: %v", err)
		}
	}()
}
