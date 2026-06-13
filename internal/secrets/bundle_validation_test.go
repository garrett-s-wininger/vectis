package secrets

import (
	"errors"
	"testing"
)

func TestValidateResolvedBundleAllowsExactRequestedFiles(t *testing.T) {
	t.Parallel()

	err := ValidateResolvedBundle(ResolveRequest{
		Secrets: []Reference{
			{ID: "npm-token", Delivery: Delivery{Type: DeliveryTypeFile, Path: "npm/token"}},
			{ID: "deploy-token", Delivery: Delivery{Type: DeliveryTypeFile, Path: "deploy/token"}},
		},
	}, Bundle{Files: []FileMaterial{
		{ID: "deploy-token", Path: "deploy/token", Data: []byte("deploy"), Mode: 0o600},
		{ID: "npm-token", Path: "npm/token", Data: []byte("npm"), Mode: 0},
	}})

	if err != nil {
		t.Fatalf("ValidateResolvedBundle: %v", err)
	}
}

func TestValidateResolvedBundleRejectsSecretMaterialInvariantViolations(t *testing.T) {
	t.Parallel()

	validReq := ResolveRequest{
		Secrets: []Reference{
			{ID: "npm-token", Delivery: Delivery{Type: DeliveryTypeFile, Path: "npm/token"}},
		},
	}

	validBundle := Bundle{Files: []FileMaterial{{
		ID:   "npm-token",
		Path: "npm/token",
		Data: []byte("secret-value"),
		Mode: DefaultFileMode,
	}}}

	tests := []struct {
		name   string
		req    ResolveRequest
		bundle Bundle
	}{
		{
			name:   "extra file material",
			req:    validReq,
			bundle: Bundle{Files: []FileMaterial{{ID: "npm-token", Path: "npm/token"}, {ID: "deploy-token", Path: "deploy/token"}}},
		},
		{
			name:   "missing file material",
			req:    validReq,
			bundle: Bundle{},
		},
		{
			name:   "duplicate file material",
			req:    validReq,
			bundle: Bundle{Files: []FileMaterial{{ID: "npm-token", Path: "npm/token"}, {ID: "npm-token", Path: "npm/token"}}},
		},
		{
			name:   "retargeted file path",
			req:    validReq,
			bundle: Bundle{Files: []FileMaterial{{ID: "npm-token", Path: "other/token"}}},
		},
		{
			name: "unsupported delivery type",
			req: ResolveRequest{
				Secrets: []Reference{{ID: "npm-token", Delivery: Delivery{Path: "npm/token"}}},
			},
			bundle: Bundle{},
		},
		{
			name: "duplicate requested path",
			req: ResolveRequest{
				Secrets: []Reference{
					{ID: "npm-token", Delivery: Delivery{Type: DeliveryTypeFile, Path: "shared/token"}},
					{ID: "deploy-token", Delivery: Delivery{Type: DeliveryTypeFile, Path: "shared/token"}},
				},
			},
			bundle: validBundle,
		},
		{
			name:   "unsafe file mode",
			req:    validReq,
			bundle: Bundle{Files: []FileMaterial{{ID: "npm-token", Path: "npm/token", Mode: 0o644}}},
		},
		{
			name:   "noncanonical material path",
			req:    validReq,
			bundle: Bundle{Files: []FileMaterial{{ID: "npm-token", Path: " npm/token "}}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := ValidateResolvedBundle(tt.req, tt.bundle); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("ValidateResolvedBundle error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}
