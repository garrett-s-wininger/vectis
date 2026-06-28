package reaction

import (
	"context"
	"encoding/json"
	"fmt"

	"vectis/internal/dal"
)

type PublishStore interface {
	RecordEvent(ctx context.Context, create dal.ReactionEventCreate) (dal.ReactionEventRecord, error)
	ListMatchingSubscriptions(ctx context.Context, event dal.ReactionEventRecord) ([]dal.ReactionSubscriptionMatch, error)
	CreateInvocation(ctx context.Context, create dal.ReactionInvocationCreate) (dal.ReactionInvocationRecord, error)
}

type Publisher struct {
	Store PublishStore
}

type Publication struct {
	Event       dal.ReactionEventRecord
	Matches     []dal.ReactionSubscriptionMatch
	Invocations []dal.ReactionInvocationRecord
}

type actionDescriptor struct {
	CanonicalName  string `json:"canonical_name"`
	SubscriptionID string `json:"subscription_id"`
}

func (p *Publisher) Publish(ctx context.Context, create dal.ReactionEventCreate) (Publication, error) {
	if p == nil || p.Store == nil {
		return Publication{}, fmt.Errorf("reaction publisher requires a store")
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
		Event:   event,
		Matches: matches,
	}

	seenTargets := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		targetID := match.Target.TargetID
		if _, seen := seenTargets[targetID]; seen {
			continue
		}

		seenTargets[targetID] = struct{}{}
		descriptor, err := json.Marshal(actionDescriptor{
			CanonicalName:  match.Target.Uses,
			SubscriptionID: match.Subscription.SubscriptionID,
		})

		if err != nil {
			return publication, err
		}

		invocation, err := p.Store.CreateInvocation(ctx, dal.ReactionInvocationCreate{
			EventID:              event.EventID,
			TargetID:             targetID,
			ActionUses:           match.Target.Uses,
			ActionDescriptorJSON: descriptor,
			TargetConfigJSON:     match.Target.ConfigJSON,
		})

		if err != nil {
			return publication, err
		}

		publication.Invocations = append(publication.Invocations, invocation)
	}

	return publication, nil
}
