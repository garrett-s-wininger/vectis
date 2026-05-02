package audit

import (
	"context"
	"errors"
	"testing"
	"time"

	"vectis/internal/dal"
)

type mockAuthRepo struct {
	events []*dal.AuditEventRecord
	err    error
}

func (m *mockAuthRepo) InsertAuditEvents(_ context.Context, events []*dal.AuditEventRecord) error {
	if m.err != nil {
		return m.err
	}
	m.events = append(m.events, events...)
	return nil
}

func (m *mockAuthRepo) IsSetupComplete(_ context.Context) (bool, error) { return false, nil }
func (m *mockAuthRepo) CompleteInitialSetup(_ context.Context, _, _, _, _ string) (int64, error) {
	return 0, nil
}
func (m *mockAuthRepo) ResolveAPIToken(_ context.Context, _ string) (int64, string, int64, error) {
	return 0, "", 0, nil
}
func (m *mockAuthRepo) TouchAPITokenUsed(_ context.Context, _ string) error  { return nil }
func (m *mockAuthRepo) UserExists(_ context.Context, _ int64) (bool, error)  { return false, nil }
func (m *mockAuthRepo) UserEnabled(_ context.Context, _ int64) (bool, error) { return false, nil }
func (m *mockAuthRepo) GetLocalUserByUsername(_ context.Context, _ string) (int64, string, bool, error) {
	return 0, "", false, nil
}
func (m *mockAuthRepo) GetUserPasswordHash(_ context.Context, _ int64) (string, error) {
	return "", nil
}
func (m *mockAuthRepo) UpdateUserPassword(_ context.Context, _ int64, _ string) error { return nil }
func (m *mockAuthRepo) DeleteAllAPITokensForUser(_ context.Context, _ int64) error    { return nil }
func (m *mockAuthRepo) ChangePasswordAndRevokeTokens(_ context.Context, _ int64, _ string) error {
	return nil
}
func (m *mockAuthRepo) ListAPITokens(_ context.Context, _ int64) ([]*dal.APITokenRecord, error) {
	return nil, nil
}
func (m *mockAuthRepo) CreateAPIToken(_ context.Context, _ int64, _, _ string, _ *time.Time) (int64, error) {
	return 0, nil
}
func (m *mockAuthRepo) DeleteAPIToken(_ context.Context, _ int64) error               { return nil }
func (m *mockAuthRepo) GetAPITokenOwner(_ context.Context, _ int64) (int64, error)    { return 0, nil }
func (m *mockAuthRepo) CreateLocalUser(_ context.Context, _, _ string) (int64, error) { return 0, nil }
func (m *mockAuthRepo) ListLocalUsers(_ context.Context) ([]*dal.LocalUserRecord, error) {
	return nil, nil
}
func (m *mockAuthRepo) GetLocalUser(_ context.Context, _ int64) (*dal.LocalUserRecord, error) {
	return nil, nil
}
func (m *mockAuthRepo) UpdateLocalUserEnabled(_ context.Context, _ int64, _ bool) error { return nil }
func (m *mockAuthRepo) DeleteLocalUser(_ context.Context, _ int64) error                { return nil }
func (m *mockAuthRepo) CountEnabledAdmins(_ context.Context) (int, error)               { return 0, nil }
func (m *mockAuthRepo) IsUserAdmin(_ context.Context, _ int64) (bool, error)            { return false, nil }
func (m *mockAuthRepo) CountEnabledRootAdmins(_ context.Context) (int, error)           { return 0, nil }
func (m *mockAuthRepo) IsUserRootAdmin(_ context.Context, _ int64) (bool, error)        { return false, nil }
func (m *mockAuthRepo) CreateAPITokenWithScopes(_ context.Context, _ int64, _, _ string, _ *time.Time, _ []*dal.TokenScopeRecord) (int64, error) {
	return 0, nil
}
func (m *mockAuthRepo) GetTokenScopes(_ context.Context, _ int64) ([]*dal.TokenScopeRecord, error) {
	return nil, nil
}

func TestDALRepository_InsertAuditEvents_empty(t *testing.T) {
	repo := &mockAuthRepo{}
	dalRepo := &DALRepository{Auth: repo}

	if err := dalRepo.InsertAuditEvents(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := dalRepo.InsertAuditEvents(context.Background(), []Event{}); err != nil {
		t.Fatal(err)
	}
}

func TestDALRepository_InsertAuditEvents_mapsFields(t *testing.T) {
	repo := &mockAuthRepo{}
	dalRepo := &DALRepository{Auth: repo}

	events := []Event{
		{
			Type:          EventAuthSuccess,
			ActorID:       1,
			TargetID:      2,
			IPAddress:     "127.0.0.1",
			CorrelationID: "abc-123",
			Timestamp:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			Metadata:      map[string]any{"key": "value"},
		},
	}

	if err := dalRepo.InsertAuditEvents(context.Background(), events); err != nil {
		t.Fatal(err)
	}

	if len(repo.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(repo.events))
	}

	rec := repo.events[0]
	if rec.Type != EventAuthSuccess {
		t.Fatalf("type=%s", rec.Type)
	}

	if rec.ActorID.Int64 != 1 || !rec.ActorID.Valid {
		t.Fatal("actor id mismatch")
	}

	if rec.TargetID.Int64 != 2 || !rec.TargetID.Valid {
		t.Fatal("target id mismatch")
	}

	if rec.IPAddress != "127.0.0.1" {
		t.Fatalf("ip=%s", rec.IPAddress)
	}

	if rec.CorrelationID != "abc-123" {
		t.Fatalf("correlation=%s", rec.CorrelationID)
	}

	if !rec.CreatedAt.Valid || !rec.CreatedAt.Time.Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("timestamp mismatch")
	}

	if string(rec.Metadata) != `{"key":"value"}` {
		t.Fatalf("metadata=%s", string(rec.Metadata))
	}
}

func TestDALRepository_InsertAuditEvents_zeroValuesOmitted(t *testing.T) {
	repo := &mockAuthRepo{}
	dalRepo := &DALRepository{Auth: repo}

	events := []Event{
		{Type: EventAuthFailure, Timestamp: time.Time{}},
	}

	if err := dalRepo.InsertAuditEvents(context.Background(), events); err != nil {
		t.Fatal(err)
	}

	rec := repo.events[0]
	if rec.CreatedAt.Valid {
		t.Fatal("expected zero timestamp to be omitted")
	}

	if rec.ActorID.Valid {
		t.Fatal("expected zero actor to be omitted")
	}

	if rec.TargetID.Valid {
		t.Fatal("expected zero target to be omitted")
	}

	if len(rec.Metadata) != 0 {
		t.Fatal("expected nil metadata to be omitted")
	}
}

func TestDALRepository_InsertAuditEvents_propagatesError(t *testing.T) {
	expectedErr := errors.New("db down")
	repo := &mockAuthRepo{err: expectedErr}
	dalRepo := &DALRepository{Auth: repo}

	events := []Event{{Type: EventAuthSuccess}}
	err := dalRepo.InsertAuditEvents(context.Background(), events)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

func TestDALRepository_InsertAuditEvents_multipleEvents(t *testing.T) {
	repo := &mockAuthRepo{}
	dalRepo := &DALRepository{Auth: repo}

	events := []Event{
		{Type: EventAuthSuccess},
		{Type: EventAuthFailure},
	}

	if err := dalRepo.InsertAuditEvents(context.Background(), events); err != nil {
		t.Fatal(err)
	}

	if len(repo.events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(repo.events))
	}
}
