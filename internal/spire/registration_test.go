package spire

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"vectis/internal/workloadidentity"
)

func TestNewExecutionRegistrationIntent(t *testing.T) {
	execution := testRegistrationExecution()
	spiffeID, err := workloadidentity.SPIFFEID("prod.example", "", execution)
	if err != nil {
		t.Fatalf("SPIFFEID: %v", err)
	}

	now := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	expiresAt := now.Add(5 * time.Minute)
	intent, err := NewExecutionRegistrationIntent(spiffeID, execution, ExecutionRegistrationOptions{
		ParentSPIFFEID: "spiffe://prod.example/spire/agent/local-worker",
		Selectors: []Selector{
			{Type: "unix", Value: "uid:1000"},
			{Type: "k8s", Value: "sa:vectis:worker"},
		},
		ExpiresAt: expiresAt,
		Now:       now,
		MinTTL:    time.Minute,
		MaxTTL:    10 * time.Minute,
	})

	if err != nil {
		t.Fatalf("NewExecutionRegistrationIntent: %v", err)
	}

	if intent.SPIFFEID != spiffeID {
		t.Fatalf("SPIFFEID = %q, want %q", intent.SPIFFEID, spiffeID)
	}

	if intent.ParentSPIFFEID != "spiffe://prod.example/spire/agent/local-worker" {
		t.Fatalf("ParentSPIFFEID = %q", intent.ParentSPIFFEID)
	}

	if !strings.HasPrefix(intent.Key, "sha256:") {
		t.Fatalf("Key = %q, want sha256 prefix", intent.Key)
	}

	if got, want := len(intent.Key), len("sha256:")+64; got != want {
		t.Fatalf("Key length = %d, want %d", got, want)
	}

	if !intent.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("ExpiresAt = %s, want %s", intent.ExpiresAt, expiresAt)
	}

	if got, want := intent.Selectors, []Selector{
		{Type: "k8s", Value: "sa:vectis:worker"},
		{Type: "unix", Value: "uid:1000"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Selectors = %#v, want %#v", got, want)
	}

	metadata := intent.Metadata
	if metadata.CellID != "cell-a" ||
		metadata.NamespacePath != "/teams/build" ||
		metadata.JobID != "job-1" ||
		metadata.RunID != "run-1" ||
		metadata.RunIndex != 2 ||
		metadata.SegmentID != "segment-1" ||
		metadata.ExecutionID != "execution-1" ||
		metadata.Attempt != 3 ||
		metadata.DefinitionVersion != 4 ||
		metadata.DefinitionHash != "sha256:abcdef" {
		t.Fatalf("Metadata = %#v", metadata)
	}

	if err := intent.Validate(now); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestRegistrationKeyIsStableAcrossSelectorOrder(t *testing.T) {
	execution := testRegistrationExecution()
	spiffeID, err := workloadidentity.SPIFFEID("prod.example", "", execution)
	if err != nil {
		t.Fatalf("SPIFFEID: %v", err)
	}

	parent := "spiffe://prod.example/spire/agent/local-worker"
	keyA, err := RegistrationKey(spiffeID, parent, []Selector{
		{Type: "unix", Value: "uid:1000"},
		{Type: "k8s", Value: "sa:vectis:worker"},
	})

	if err != nil {
		t.Fatalf("RegistrationKey A: %v", err)
	}

	keyB, err := RegistrationKey(spiffeID, parent, []Selector{
		{Type: "k8s", Value: "sa:vectis:worker"},
		{Type: "unix", Value: "uid:1000"},
	})

	if err != nil {
		t.Fatalf("RegistrationKey B: %v", err)
	}

	if keyA != keyB {
		t.Fatalf("RegistrationKey changed with selector order: %q != %q", keyA, keyB)
	}
}

func TestNewExecutionRegistrationIntentValidatesInputs(t *testing.T) {
	execution := testRegistrationExecution()
	spiffeID, err := workloadidentity.SPIFFEID("prod.example", "", execution)
	if err != nil {
		t.Fatalf("SPIFFEID: %v", err)
	}

	now := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	base := ExecutionRegistrationOptions{
		ParentSPIFFEID: "spiffe://prod.example/spire/agent/local-worker",
		Selectors:      []Selector{{Type: "unix", Value: "uid:1000"}},
		ExpiresAt:      now.Add(5 * time.Minute),
		Now:            now,
		MinTTL:         time.Minute,
		MaxTTL:         10 * time.Minute,
	}

	tests := []struct {
		name    string
		id      string
		opts    ExecutionRegistrationOptions
		wantErr string
	}{
		{
			name:    "bad execution ID",
			id:      "https://prod.example/not-spiffe",
			opts:    base,
			wantErr: "execution SPIFFE ID",
		},
		{
			name: "bad parent ID",
			id:   spiffeID,
			opts: func() ExecutionRegistrationOptions {
				opts := base
				opts.ParentSPIFFEID = "worker-parent"
				return opts
			}(),
			wantErr: "parent SPIFFE ID",
		},
		{
			name: "missing selectors",
			id:   spiffeID,
			opts: func() ExecutionRegistrationOptions {
				opts := base
				opts.Selectors = nil
				return opts
			}(),
			wantErr: "selector",
		},
		{
			name: "bad selector type",
			id:   spiffeID,
			opts: func() ExecutionRegistrationOptions {
				opts := base
				opts.Selectors = []Selector{{Type: "unix:uid", Value: "1000"}}
				return opts
			}(),
			wantErr: "must not contain ':'",
		},
		{
			name: "duplicate selector",
			id:   spiffeID,
			opts: func() ExecutionRegistrationOptions {
				opts := base
				opts.Selectors = []Selector{
					{Type: "unix", Value: "uid:1000"},
					{Type: " unix ", Value: " uid:1000 "},
				}
				return opts
			}(),
			wantErr: "duplicate",
		},
		{
			name: "missing expiry",
			id:   spiffeID,
			opts: func() ExecutionRegistrationOptions {
				opts := base
				opts.ExpiresAt = time.Time{}
				return opts
			}(),
			wantErr: "expiry is required",
		},
		{
			name: "expired",
			id:   spiffeID,
			opts: func() ExecutionRegistrationOptions {
				opts := base
				opts.ExpiresAt = now.Add(-time.Second)
				return opts
			}(),
			wantErr: "future",
		},
		{
			name: "below minimum TTL",
			id:   spiffeID,
			opts: func() ExecutionRegistrationOptions {
				opts := base
				opts.ExpiresAt = now.Add(30 * time.Second)
				return opts
			}(),
			wantErr: "below minimum",
		},
		{
			name: "above maximum TTL",
			id:   spiffeID,
			opts: func() ExecutionRegistrationOptions {
				opts := base
				opts.ExpiresAt = now.Add(15 * time.Minute)
				return opts
			}(),
			wantErr: "exceeds maximum",
		},
		{
			name: "minimum exceeds maximum",
			id:   spiffeID,
			opts: func() ExecutionRegistrationOptions {
				opts := base
				opts.MinTTL = 10 * time.Minute
				opts.MaxTTL = time.Minute
				return opts
			}(),
			wantErr: "minimum TTL must not exceed maximum TTL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewExecutionRegistrationIntent(tt.id, execution, tt.opts)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("NewExecutionRegistrationIntent error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestRegistrationIntentValidateRejectsMutation(t *testing.T) {
	execution := testRegistrationExecution()
	spiffeID, err := workloadidentity.SPIFFEID("prod.example", "", execution)
	if err != nil {
		t.Fatalf("SPIFFEID: %v", err)
	}

	now := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	intent, err := NewExecutionRegistrationIntent(spiffeID, execution, ExecutionRegistrationOptions{
		ParentSPIFFEID: "spiffe://prod.example/spire/agent/local-worker",
		Selectors: []Selector{
			{Type: "unix", Value: "uid:1000"},
			{Type: "k8s", Value: "sa:vectis:worker"},
		},
		ExpiresAt: now.Add(5 * time.Minute),
		Now:       now,
	})

	if err != nil {
		t.Fatalf("NewExecutionRegistrationIntent: %v", err)
	}

	t.Run("changed key", func(t *testing.T) {
		mutated := intent
		mutated.Key = "sha256:bad"
		err := mutated.Validate(now)
		if err == nil || !strings.Contains(err.Error(), "key does not match") {
			t.Fatalf("Validate error = %v, want key mismatch", err)
		}
	})

	t.Run("unsorted selectors", func(t *testing.T) {
		mutated := intent
		mutated.Selectors = []Selector{
			{Type: "unix", Value: "uid:1000"},
			{Type: "k8s", Value: "sa:vectis:worker"},
		}

		err := mutated.Validate(now)
		if err == nil || !strings.Contains(err.Error(), "selectors are not normalized") {
			t.Fatalf("Validate error = %v, want selector normalization error", err)
		}
	})

	t.Run("expired", func(t *testing.T) {
		err := intent.Validate(now.Add(6 * time.Minute))
		if err == nil || !strings.Contains(err.Error(), "expiry must be in the future") {
			t.Fatalf("Validate error = %v, want expiry error", err)
		}
	})
}

func TestExecutionRegistrationMetadataDoesNotIncludeSecretFields(t *testing.T) {
	execution := testRegistrationExecution()
	spiffeID, err := workloadidentity.SPIFFEID("prod.example", "", execution)
	if err != nil {
		t.Fatalf("SPIFFEID: %v", err)
	}

	intent, err := NewExecutionRegistrationIntent(spiffeID, execution, ExecutionRegistrationOptions{
		ParentSPIFFEID: "spiffe://prod.example/spire/agent/local-worker",
		Selectors:      []Selector{{Type: "unix", Value: "uid:1000"}},
		ExpiresAt:      time.Now().Add(time.Minute),
	})

	if err != nil {
		t.Fatalf("NewExecutionRegistrationIntent: %v", err)
	}

	typ := reflect.TypeOf(intent.Metadata)
	for i := 0; i < typ.NumField(); i++ {
		name := strings.ToLower(typ.Field(i).Name)
		if strings.Contains(name, "secret") ||
			strings.Contains(name, "token") ||
			strings.Contains(name, "ref") ||
			strings.Contains(name, "path") && name != "namespacepath" {
			t.Fatalf("metadata field %q looks like secret material or delivery detail", typ.Field(i).Name)
		}
	}
}

func testRegistrationExecution() workloadidentity.Execution {
	return workloadidentity.Execution{
		CellID:            "cell-a",
		NamespacePath:     "/teams/build",
		JobID:             "job-1",
		RunID:             "run-1",
		RunIndex:          2,
		SegmentID:         "segment-1",
		ExecutionID:       "execution-1",
		Attempt:           3,
		DefinitionVersion: 4,
		DefinitionHash:    "sha256:abcdef",
	}
}
