package conformance_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"vectis/sdk/secrets"
	"vectis/sdk/secrets/conformance"
)

func TestRunProviderSuite(t *testing.T) {
	conformance.RunProviderSuite(t, func(t *testing.T) secrets.Provider {
		t.Helper()
		return fakeProvider{}
	}, conformance.Options{
		ProviderKind: "fake",
		ValidRef:     fakeRef("valid", "fake://valid"),
		InvalidRef:   fakeRef("invalid", "other://invalid"),
		NotFoundRef:  fakeRef("missing", "fake://missing"),
		DeniedRef:    fakeRef("denied", "fake://denied"),
		WantData:     []byte("secret-value"),
	})
}

type fakeProvider struct{}

func (fakeProvider) ProviderKind() string { return "fake" }

func (fakeProvider) ValidateRef(_ context.Context, ref secrets.Reference) error {
	if err := secrets.ValidateFileDelivery(ref); err != nil {
		return err
	}

	if !strings.HasPrefix(ref.Ref, "fake://") {
		return fmt.Errorf("%w: unsupported ref", secrets.ErrNotFound)
	}

	return nil
}

func (fakeProvider) Resolve(ctx context.Context, req secrets.ResolveRequest) (secrets.Bundle, error) {
	var files []secrets.FileMaterial
	for _, ref := range req.Secrets {
		if err := (fakeProvider{}).ValidateRef(ctx, ref); err != nil {
			return secrets.Bundle{}, err
		}

		switch ref.Ref {
		case "fake://missing":
			return secrets.Bundle{}, fmt.Errorf("%w: missing", secrets.ErrNotFound)
		case "fake://denied":
			return secrets.Bundle{}, fmt.Errorf("%w: denied", secrets.ErrDenied)
		}

		files = append(files, secrets.FileMaterial{
			ID:   ref.ID,
			Path: ref.Delivery.Path,
			Data: []byte("secret-value"),
			Mode: secrets.DefaultFileMode,
		})
	}

	return secrets.Bundle{Files: files}, nil
}

func fakeRef(id, ref string) secrets.Reference {
	return secrets.Reference{
		ID:  id,
		Ref: ref,
		Delivery: secrets.Delivery{
			Type: secrets.DeliveryTypeFile,
			Path: id,
		},
	}
}
