package secrets

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestAccessPolicyAllowsMatchingRule(t *testing.T) {
	t.Parallel()

	policy, err := NewAccessPolicy([]string{
		"namespace=/team-a;job=job-1;task=publish;ref=encryptedfs://team-a/npm-token",
	})

	if err != nil {
		t.Fatalf("NewAccessPolicy: %v", err)
	}

	req := ResolveRequest{
		Scope: ExecutionScope{
			NamespacePath: "/team-a",
			JobID:         "job-1",
			TaskKey:       "publish",
		},
		Secrets: []Reference{{
			ID:  "npm-token",
			Ref: "encryptedfs://team-a/npm-token",
		}},
	}

	if err := policy.AuthorizeResolve(context.Background(), req); err != nil {
		t.Fatalf("AuthorizeResolve: %v", err)
	}
}

func TestAccessPolicyDeniesWithoutMatchingRule(t *testing.T) {
	t.Parallel()

	policy, err := NewAccessPolicy([]string{
		"namespace=/team-a;job=job-1;task=publish;ref=encryptedfs://team-a/npm-token",
	})

	if err != nil {
		t.Fatalf("NewAccessPolicy: %v", err)
	}

	req := ResolveRequest{
		Scope: ExecutionScope{
			NamespacePath: "/team-a",
			JobID:         "job-1",
			TaskKey:       "publish",
		},
		Secrets: []Reference{{
			ID:  "deploy-token",
			Ref: "encryptedfs://team-a/deploy-token",
		}},
	}

	if err := policy.AuthorizeResolve(context.Background(), req); !errors.Is(err, ErrDenied) {
		t.Fatalf("AuthorizeResolve error = %v, want ErrDenied", err)
	}
}

func TestAccessPolicyDeniesWhenNoRulesConfigured(t *testing.T) {
	t.Parallel()

	policy, err := NewAccessPolicy(nil)
	if err != nil {
		t.Fatalf("NewAccessPolicy: %v", err)
	}

	err = policy.AuthorizeResolve(context.Background(), ResolveRequest{
		Secrets: []Reference{{ID: "npm-token", Ref: "encryptedfs://team-a/npm-token"}},
	})

	if !errors.Is(err, ErrDenied) {
		t.Fatalf("AuthorizeResolve error = %v, want ErrDenied", err)
	}
}

func TestAccessPolicyRequiresAllRequestedSecretsAllowed(t *testing.T) {
	t.Parallel()

	policy, err := NewAccessPolicy([]string{
		"namespace=/team-a;job=job-1;task=*;ref=encryptedfs://team-a/npm-token",
	})

	if err != nil {
		t.Fatalf("NewAccessPolicy: %v", err)
	}

	err = policy.AuthorizeResolve(context.Background(), ResolveRequest{
		Scope: ExecutionScope{
			NamespacePath: "/team-a",
			JobID:         "job-1",
			TaskKey:       "publish",
		},
		Secrets: []Reference{
			{ID: "npm-token", Ref: "encryptedfs://team-a/npm-token"},
			{ID: "deploy-token", Ref: "encryptedfs://team-a/deploy-token"},
		},
	})

	if !errors.Is(err, ErrDenied) {
		t.Fatalf("AuthorizeResolve error = %v, want ErrDenied", err)
	}
}

func TestAccessPolicySupportsWildcardPrefixes(t *testing.T) {
	t.Parallel()

	policy, err := NewAccessPolicy([]string{
		"namespace=/teams/*;job=job-*;task=*;ref=encryptedfs://teams/*",
	})

	if err != nil {
		t.Fatalf("NewAccessPolicy: %v", err)
	}

	req := ResolveRequest{
		Scope: ExecutionScope{
			NamespacePath: "/teams/build",
			JobID:         "job-release",
			TaskKey:       "publish",
		},
		Secrets: []Reference{{
			ID:  "npm-token",
			Ref: "encryptedfs://teams/build/npm-token",
		}},
	}

	if err := policy.AuthorizeResolve(context.Background(), req); err != nil {
		t.Fatalf("AuthorizeResolve: %v", err)
	}
}

func TestParseAccessRuleRejectsInvalidSpecs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec string
		want string
	}{
		{name: "empty", spec: "", want: "empty"},
		{name: "missing equals", spec: "namespace=/team;ref", want: "key=value"},
		{name: "unknown key", spec: "namespace=/team;owner=ops;ref=encryptedfs://team/token", want: "unknown key"},
		{name: "missing ref", spec: "namespace=/team;job=job-1", want: "ref is required"},
		{name: "duplicate", spec: "namespace=/team;namespace=/other;ref=encryptedfs://team/token", want: "duplicate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseAccessRule(tt.spec)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ParseAccessRule error = %v, want %q", err, tt.want)
			}
		})
	}
}
