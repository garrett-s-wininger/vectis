package reaction

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"vectis/internal/dal"
)

type LocalMessageStore interface {
	RecordLocalMessage(ctx context.Context, create dal.ReactionLocalMessageCreate) (dal.ReactionLocalMessageRecord, error)
}

type LocalNotifyAction struct {
	Store LocalMessageStore
}

type LocalNotifyActionRequest struct {
	Event      dal.ReactionEventRecord
	Invocation dal.ReactionInvocationRecord
	Payload    []byte
}

func (a *LocalNotifyAction) Type() string {
	return dal.ReactionActionNotifyLocal
}

func (a *LocalNotifyAction) Execute(ctx context.Context, req LocalNotifyActionRequest) (dal.ReactionLocalMessageRecord, error) {
	if a == nil || a.Store == nil {
		return dal.ReactionLocalMessageRecord{}, fmt.Errorf("local notify reaction action requires a store")
	}

	eventID := strings.TrimSpace(req.Event.EventID)
	if eventID == "" {
		eventID = strings.TrimSpace(req.Invocation.EventID)
	}

	if eventID == "" {
		return dal.ReactionLocalMessageRecord{}, fmt.Errorf("local notify reaction action requires event_id")
	}

	invocationID := strings.TrimSpace(req.Invocation.InvocationID)
	if invocationID == "" {
		return dal.ReactionLocalMessageRecord{}, fmt.Errorf("local notify reaction action requires invocation_id")
	}

	payload := bytes.TrimSpace(req.Payload)
	if len(payload) == 0 {
		payload = bytes.TrimSpace(req.Event.PayloadJSON)
	}

	if len(payload) == 0 {
		payload = []byte("{}")
	}

	mailbox, err := localMailbox(req.Invocation.TargetConfigJSON)
	if err != nil {
		return dal.ReactionLocalMessageRecord{}, err
	}

	return a.Store.RecordLocalMessage(ctx, dal.ReactionLocalMessageCreate{
		EventID:      eventID,
		InvocationID: invocationID,
		Mailbox:      mailbox,
		PayloadJSON:  payload,
	})
}

func localMailbox(configJSON []byte) (string, error) {
	configJSON = bytes.TrimSpace(configJSON)
	if len(configJSON) == 0 {
		return "default", nil
	}

	var config struct {
		Mailbox string `json:"mailbox"`
	}

	if err := json.Unmarshal(configJSON, &config); err != nil {
		return "", fmt.Errorf("local notify reaction action config: %w", err)
	}

	mailbox := strings.TrimSpace(config.Mailbox)
	if mailbox == "" {
		return "default", nil
	}

	return mailbox, nil
}
