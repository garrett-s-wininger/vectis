package secrets

import (
	"context"
	"fmt"
)

type UnconfiguredProvider struct{}

func (UnconfiguredProvider) ValidateRef(context.Context, Reference) error {
	return fmt.Errorf("%w: provider is not configured", ErrNotFound)
}

func (UnconfiguredProvider) Resolve(context.Context, ResolveRequest) (Bundle, error) {
	return Bundle{}, fmt.Errorf("%w: provider is not configured", ErrNotFound)
}
