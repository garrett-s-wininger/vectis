package job

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/timestamppb"
	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/protobuf/proto"
)

type executionLogStream struct {
	interfaces.LogStream
	executionID string
}

func (s *executionLogStream) Send(chunk *api.LogChunk) error {
	if s.executionID == "" {
		return s.LogStream.Send(chunk)
	}

	copy := proto.Clone(chunk).(*api.LogChunk)
	copy.ExecutionId = proto.String(s.executionID)
	return s.LogStream.Send(copy)
}

func PublishRunLogCompletion(
	ctx context.Context,
	client interfaces.LogClient,
	logger interfaces.Logger,
	runID, status string,
	outcome api.RunOutcome,
	rootExecutionID ...string,
) error {
	stream, err := newDurableLogStream(ctx, client, logger, runID)
	if err != nil {
		return err
	}

	data := fmt.Sprintf(`{"event":"completed","status":%q}`, status)
	if len(rootExecutionID) > 0 && rootExecutionID[0] != "" {
		data = fmt.Sprintf(`{"event":"completed","status":%q,"root_execution_id":%q}`, status, rootExecutionID[0])
	}

	err = stream.Send(&api.LogChunk{
		RunId:     proto.String(runID),
		Stream:    api.Stream_STREAM_CONTROL.Enum(),
		Data:      []byte(data),
		Completed: outcome.Enum(),
		Timestamp: timestamppb.Now(),
	})

	_ = stream.CloseSend()
	if err != nil {
		return err
	}

	select {
	case <-stream.done:
		return stream.SenderErr()
	case <-ctx.Done():
		return ctx.Err()
	}
}
