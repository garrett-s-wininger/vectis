// Package conformance contains reusable tests for secret provider extensions.
package conformance

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	"vectis/sdk/secrets"
)

type ProviderFactory func(*testing.T) secrets.Provider

type Options struct {
	ProviderKind string
	ValidRef     secrets.Reference
	InvalidRef   secrets.Reference
	NotFoundRef  secrets.Reference
	DeniedRef    secrets.Reference
	WantData     []byte
	WantMode     fs.FileMode
	Timeout      time.Duration
}

func RunProviderSuite(t *testing.T, factory ProviderFactory, opts Options) {
	t.Helper()

	if factory == nil {
		t.Fatal("secret provider conformance factory is required")
	}

	if opts.ProviderKind == "" {
		t.Fatal("secret provider conformance ProviderKind is required")
	}

	validateConformanceRef(t, "ValidRef", opts.ValidRef)
	validateConformanceRef(t, "InvalidRef", opts.InvalidRef)

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	wantMode := opts.WantMode
	if wantMode == 0 {
		wantMode = secrets.DefaultFileMode
	}

	t.Run("provider_kind", func(t *testing.T) {
		provider := factory(t)
		kinded, ok := provider.(secrets.KindedProvider)
		if !ok {
			t.Fatalf("%T does not implement secrets.KindedProvider", provider)
		}

		if got := kinded.ProviderKind(); got != opts.ProviderKind {
			t.Fatalf("ProviderKind = %q, want %q", got, opts.ProviderKind)
		}
	})

	t.Run("validate_supported_ref", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		if err := factory(t).ValidateRef(ctx, opts.ValidRef); err != nil {
			t.Fatalf("ValidateRef(valid): %v", err)
		}
	})

	t.Run("validate_unsupported_ref", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		err := factory(t).ValidateRef(ctx, opts.InvalidRef)
		if !errors.Is(err, secrets.ErrNotFound) {
			t.Fatalf("ValidateRef(invalid) error = %v, want ErrNotFound", err)
		}
	})

	t.Run("resolve_file_delivery", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		bundle, err := factory(t).Resolve(ctx, secrets.ResolveRequest{
			RunID:       "conformance-run",
			ExecutionID: "conformance-execution",
			Secrets:     []secrets.Reference{opts.ValidRef},
		})
		if err != nil {
			t.Fatalf("Resolve(valid): %v", err)
		}

		if len(bundle.Files) != 1 {
			t.Fatalf("files = %+v, want one", bundle.Files)
		}

		file := bundle.Files[0]
		if file.ID != opts.ValidRef.ID {
			t.Fatalf("file ID = %q, want %q", file.ID, opts.ValidRef.ID)
		}

		if file.Path != opts.ValidRef.Delivery.Path {
			t.Fatalf("file path = %q, want %q", file.Path, opts.ValidRef.Delivery.Path)
		}

		if string(file.Data) != string(opts.WantData) {
			t.Fatalf("file data = %q, want %q", file.Data, opts.WantData)
		}

		if file.Mode != wantMode {
			t.Fatalf("file mode = %v, want %v", file.Mode, wantMode)
		}
	})

	t.Run("reject_unsupported_delivery_type", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		ref := opts.ValidRef
		ref.Delivery.Type = secrets.DeliveryType("env")
		if err := factory(t).ValidateRef(ctx, ref); err == nil {
			t.Fatal("ValidateRef accepted unsupported delivery type")
		}

		_, err := factory(t).Resolve(ctx, secrets.ResolveRequest{Secrets: []secrets.Reference{ref}})
		if err == nil {
			t.Fatal("Resolve accepted unsupported delivery type")
		}
	})

	if opts.NotFoundRef.Ref != "" {
		validateConformanceRef(t, "NotFoundRef", opts.NotFoundRef)
		t.Run("preserve_not_found_error", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			_, err := factory(t).Resolve(ctx, secrets.ResolveRequest{Secrets: []secrets.Reference{opts.NotFoundRef}})
			if !errors.Is(err, secrets.ErrNotFound) {
				t.Fatalf("Resolve(not found) error = %v, want ErrNotFound", err)
			}
		})
	}

	if opts.DeniedRef.Ref != "" {
		validateConformanceRef(t, "DeniedRef", opts.DeniedRef)
		t.Run("preserve_denied_error", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			_, err := factory(t).Resolve(ctx, secrets.ResolveRequest{Secrets: []secrets.Reference{opts.DeniedRef}})
			if !errors.Is(err, secrets.ErrDenied) {
				t.Fatalf("Resolve(denied) error = %v, want ErrDenied", err)
			}
		})
	}
}

func validateConformanceRef(t *testing.T, name string, ref secrets.Reference) {
	t.Helper()

	if ref.ID == "" {
		t.Fatalf("%s ID is required", name)
	}

	if ref.Ref == "" {
		t.Fatalf("%s Ref is required", name)
	}

	if ref.Delivery.Type != secrets.DeliveryTypeFile {
		t.Fatalf("%s delivery type = %q, want %q", name, ref.Delivery.Type, secrets.DeliveryTypeFile)
	}

	if ref.Delivery.Path == "" {
		t.Fatalf("%s delivery path is required", name)
	}
}
