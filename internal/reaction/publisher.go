package reaction

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"vectis/internal/dal"
)

type PublishStore interface {
	RecordEvent(ctx context.Context, create dal.ReactionEventCreate) (dal.ReactionEventRecord, error)
	GetTarget(ctx context.Context, targetID string) (dal.ReactionTargetRecord, error)
	ListMatchingSubscriptions(ctx context.Context, event dal.ReactionEventRecord) ([]dal.ReactionSubscriptionMatch, error)
	CreateInvocation(ctx context.Context, create dal.ReactionInvocationCreate) (dal.ReactionInvocationRecord, error)
}

type Publisher struct {
	Store PublishStore
}

type Publication struct {
	Event         dal.ReactionEventRecord
	Matches       []dal.ReactionSubscriptionMatch
	DirectTargets []dal.ReactionTargetRecord
	Invocations   []dal.ReactionInvocationRecord
}

type actionDescriptor struct {
	CanonicalName  string `json:"canonical_name"`
	SubscriptionID string `json:"subscription_id,omitempty"`
	TargetID       string `json:"target_id,omitempty"`
}

func (p *Publisher) Publish(ctx context.Context, create dal.ReactionEventCreate) (Publication, error) {
	return p.publish(ctx, create, nil)
}

func (p *Publisher) publish(ctx context.Context, create dal.ReactionEventCreate, targetIDs []string) (Publication, error) {
	if p == nil || p.Store == nil {
		return Publication{}, fmt.Errorf("reaction publisher requires a store")
	}

	directTargets, err := p.resolveDirectTargets(ctx, targetIDs)
	if err != nil {
		return Publication{}, err
	}

	event, err := p.Store.RecordEvent(ctx, create)
	if err != nil {
		return Publication{}, err
	}

	matches, err := p.Store.ListMatchingSubscriptions(ctx, event)
	if err != nil {
		return Publication{Event: event}, err
	}

	publication := Publication{
		Event:         event,
		Matches:       matches,
		DirectTargets: directTargets,
	}

	seenTargets := make(map[string]struct{}, len(matches)+len(directTargets))
	for _, match := range matches {
		if err := p.createInvocation(ctx, &publication, seenTargets, match.Target, match.Subscription.SubscriptionID); err != nil {
			return publication, err
		}
	}

	for _, target := range directTargets {
		if err := p.createInvocation(ctx, &publication, seenTargets, target, ""); err != nil {
			return publication, err
		}
	}

	return publication, nil
}

func (p *Publisher) resolveDirectTargets(ctx context.Context, targetIDs []string) ([]dal.ReactionTargetRecord, error) {
	seenDirectTargets := make(map[string]struct{}, len(targetIDs))
	var out []dal.ReactionTargetRecord
	for _, targetID := range targetIDs {
		targetID = strings.TrimSpace(targetID)
		if targetID == "" {
			return nil, fmt.Errorf("%w: manual reaction target_id is required", dal.ErrConflict)
		}

		if _, seen := seenDirectTargets[targetID]; seen {
			continue
		}

		seenDirectTargets[targetID] = struct{}{}
		target, err := p.Store.GetTarget(ctx, targetID)
		if err != nil {
			return nil, err
		}

		if !target.Enabled {
			continue
		}

		out = append(out, target)
	}

	return out, nil
}

func (p *Publisher) createInvocation(ctx context.Context, publication *Publication, seenTargets map[string]struct{}, target dal.ReactionTargetRecord, subscriptionID string) error {
	targetID := target.TargetID
	if _, seen := seenTargets[targetID]; seen {
		return nil
	}

	seenTargets[targetID] = struct{}{}
	descriptor, err := json.Marshal(actionDescriptor{
		CanonicalName:  target.Uses,
		SubscriptionID: subscriptionID,
		TargetID:       targetID,
	})

	if err != nil {
		return err
	}

	invocation, err := p.Store.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              publication.Event.EventID,
		TargetID:             targetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: descriptor,
		TargetConfigJSON:     target.ConfigJSON,
	})

	if err != nil {
		return err
	}

	publication.Invocations = append(publication.Invocations, invocation)
	return nil
}
