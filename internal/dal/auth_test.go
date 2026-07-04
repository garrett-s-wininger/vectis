package dal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"vectis/internal/testutil/dbtest"

	"golang.org/x/crypto/bcrypt"
)

func TestLocalUsers_usernameUniqueConstraint(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, rebindQueryForPgx(
		`INSERT INTO local_users (username, password_hash) VALUES (?, ?)`),
		"dup", "hash1",
	); err != nil {
		t.Fatal(err)
	}

	_, err := db.ExecContext(ctx, rebindQueryForPgx(
		`INSERT INTO local_users (username, password_hash) VALUES (?, ?)`),
		"dup", "hash2",
	)
	if !IsConflict(normalizeSQLError(err)) {
		t.Fatalf("expected unique conflict, got %v", err)
	}
}

func TestAuthRepository_ExternalIdentities(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)

	provider, err := repo.EnsureAuthProvider(ctx, "corp-ldap", "ldap")
	if err != nil {
		t.Fatalf("EnsureAuthProvider: %v", err)
	}

	if provider.ID == 0 || provider.ProviderID != "corp-ldap" || provider.Kind != "ldap" || !provider.Enabled {
		t.Fatalf("unexpected provider: %+v", provider)
	}

	sameProvider, err := repo.EnsureAuthProvider(ctx, "corp-ldap", "ldap")
	if err != nil {
		t.Fatalf("EnsureAuthProvider existing: %v", err)
	}

	if sameProvider.ID != provider.ID {
		t.Fatalf("existing provider id = %d, want %d", sameProvider.ID, provider.ID)
	}

	if _, err := repo.EnsureAuthProvider(ctx, "corp-ldap", "oidc"); !IsConflict(err) {
		t.Fatalf("EnsureAuthProvider kind mismatch error = %v, want conflict", err)
	}

	userID, err := repo.CreateLocalUser(ctx, "alice", "hash")
	if err != nil {
		t.Fatalf("CreateLocalUser: %v", err)
	}

	link, err := repo.LinkExternalIdentity(ctx, userID, "corp-ldap", "uid=alice,ou=people,dc=example,dc=org", "alice", "Alice")
	if err != nil {
		t.Fatalf("LinkExternalIdentity: %v", err)
	}

	if link.LocalUserID != userID || link.ProviderID != "corp-ldap" || link.Subject != "uid=alice,ou=people,dc=example,dc=org" || link.LocalUsername != "alice" {
		t.Fatalf("unexpected link: %+v", link)
	}

	links, err := repo.ListExternalIdentitiesForUser(ctx, userID)
	if err != nil {
		t.Fatalf("ListExternalIdentitiesForUser: %v", err)
	}

	if len(links) != 1 || links[0].ID != link.ID {
		t.Fatalf("links = %+v, want one created link", links)
	}

	found, err := repo.GetExternalIdentity(ctx, "corp-ldap", "uid=alice,ou=people,dc=example,dc=org")
	if err != nil {
		t.Fatalf("GetExternalIdentity: %v", err)
	}

	if found.ID != link.ID {
		t.Fatalf("found identity id = %d, want %d", found.ID, link.ID)
	}

	if err := repo.TouchExternalIdentity(ctx, link.ID, "alice.renamed", "Alice Renamed"); err != nil {
		t.Fatalf("TouchExternalIdentity: %v", err)
	}

	found, err = repo.GetExternalIdentity(ctx, "corp-ldap", "uid=alice,ou=people,dc=example,dc=org")
	if err != nil {
		t.Fatalf("GetExternalIdentity after touch: %v", err)
	}

	if found.Username != "alice.renamed" || found.DisplayName != "Alice Renamed" {
		t.Fatalf("touched identity = %+v", found)
	}

	_, err = repo.LinkExternalIdentity(ctx, userID, "corp-ldap", "uid=alice2,ou=people,dc=example,dc=org", "alice", "Alice")
	if !IsConflict(err) {
		t.Fatalf("second subject for provider/local user error = %v, want conflict", err)
	}

	if _, err := repo.EnsureAuthProvider(ctx, "contractor-ldap", "ldap"); err != nil {
		t.Fatalf("EnsureAuthProvider contractor: %v", err)
	}

	if _, err := repo.LinkExternalIdentity(ctx, userID, "contractor-ldap", "uid=alice,ou=people,dc=example,dc=org", "alice", "Alice"); err != nil {
		t.Fatalf("same subject under different provider should link: %v", err)
	}

	if err := repo.DeleteExternalIdentity(ctx, userID, link.ID); err != nil {
		t.Fatalf("DeleteExternalIdentity: %v", err)
	}

	_, err = repo.GetExternalIdentity(ctx, "corp-ldap", "uid=alice,ou=people,dc=example,dc=org")
	if !IsNotFound(err) {
		t.Fatalf("GetExternalIdentity after delete error = %v, want not found", err)
	}

	if err := repo.DeleteExternalIdentity(ctx, userID, link.ID); !IsNotFound(err) {
		t.Fatalf("DeleteExternalIdentity missing error = %v, want not found", err)
	}
}

func TestAuthRepository_IsSetupComplete_initiallyFalse(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)
	ctx := context.Background()

	ok, err := repo.IsSetupComplete(ctx)
	if err != nil {
		t.Fatalf("IsSetupComplete: %v", err)
	}

	if ok {
		t.Fatal("expected setup not complete on fresh DB")
	}
}

func TestAuthRepository_CompleteInitialSetup_and_IsSetupComplete(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)
	ctx := context.Background()

	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	tokenHash := "aabbccdd" + "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"

	uid, err := repo.CompleteInitialSetup(ctx, "admin", string(passHash), tokenHash, "initial")
	if err != nil {
		t.Fatalf("CompleteInitialSetup: %v", err)
	}

	if uid <= 0 {
		t.Fatalf("expected positive user id, got %d", uid)
	}

	ok, err := repo.IsSetupComplete(ctx)
	if err != nil {
		t.Fatalf("IsSetupComplete: %v", err)
	}

	if !ok {
		t.Fatal("expected setup complete after CompleteInitialSetup")
	}

	_, err = repo.CompleteInitialSetup(ctx, "other", string(passHash), tokenHash+"x", "x")
	if !errors.Is(err, ErrSetupAlreadyComplete) {
		t.Fatalf("expected ErrSetupAlreadyComplete, got %v", err)
	}
}

func TestAuthRepository_CompleteInitialSetupWithExternalIdentity(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)
	ctx := context.Background()

	if _, err := repo.EnsureAuthProvider(ctx, "corp-ldap", "ldap"); err != nil {
		t.Fatalf("EnsureAuthProvider: %v", err)
	}

	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	uid, err := repo.CompleteInitialSetupWithOptions(ctx, CompleteInitialSetupOptions{
		Username:            "admin",
		PasswordHash:        string(passHash),
		PasswordAuthEnabled: false,
		TokenHash:           "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
		TokenLabel:          "initial",
		ExternalIdentity: &InitialSetupExternalIdentity{
			ProviderID:  "corp-ldap",
			Subject:     "entryUUID=admin-uuid",
			Username:    "admin",
			DisplayName: "Admin User",
		},
	})

	if err != nil {
		t.Fatalf("CompleteInitialSetupWithOptions: %v", err)
	}

	user, err := repo.GetLocalUser(ctx, uid)
	if err != nil {
		t.Fatalf("GetLocalUser: %v", err)
	}

	if user.PasswordAuthEnabled {
		t.Fatal("expected setup admin password auth disabled")
	}

	if _, err := repo.GetUserPasswordHash(ctx, uid); !errors.Is(err, ErrPasswordAuthDisabled) {
		t.Fatalf("GetUserPasswordHash error = %v, want ErrPasswordAuthDisabled", err)
	}

	identity, err := repo.GetExternalIdentity(ctx, "corp-ldap", "entryUUID=admin-uuid")
	if err != nil {
		t.Fatalf("GetExternalIdentity: %v", err)
	}

	if identity.LocalUserID != uid || identity.Username != "admin" || identity.DisplayName != "Admin User" {
		t.Fatalf("bad external identity: %+v", identity)
	}
}

func TestAuthRepository_CompleteInitialSetupExternalIdentityMissingProviderRollsBack(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)
	ctx := context.Background()

	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	_, err = repo.CompleteInitialSetupWithOptions(ctx, CompleteInitialSetupOptions{
		Username:            "admin",
		PasswordHash:        string(passHash),
		PasswordAuthEnabled: false,
		TokenHash:           "aabbccdd00112233445566778899aabbccddeeff00112233445566778899aabbccdd0000",
		TokenLabel:          "initial",
		ExternalIdentity: &InitialSetupExternalIdentity{
			ProviderID: "missing-ldap",
			Subject:    "entryUUID=admin-uuid",
			Username:   "admin",
		},
	})

	if !IsNotFound(err) {
		t.Fatalf("CompleteInitialSetupWithOptions error = %v, want not found", err)
	}

	complete, err := repo.IsSetupComplete(ctx)
	if err != nil {
		t.Fatalf("IsSetupComplete: %v", err)
	}

	if complete {
		t.Fatal("setup should remain incomplete after failed external identity link")
	}
}

func TestAuthRepository_CompleteInitialSetup_concurrentExactlyOneSucceeds(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)
	ctx := context.Background()

	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			h := strings.Repeat("3", 62) + fmt.Sprintf("%02x", idx)
			if len(h) != 64 {
				panic(h)
			}

			_, errs[idx] = repo.CompleteInitialSetup(ctx, fmt.Sprintf("admin-%d", idx), string(passHash), h, "race")
		}(i)
	}

	wg.Wait()

	var nOK, nSetupDone int
	for _, e := range errs {
		switch {
		case e == nil:
			nOK++
		case errors.Is(e, ErrSetupAlreadyComplete):
			nSetupDone++
		default:
			t.Fatalf("unexpected error: %v", e)
		}
	}

	if nOK != 1 {
		t.Fatalf("expected exactly one successful setup, got %d (already_complete=%d)", nOK, nSetupDone)
	}

	if nSetupDone != len(errs)-1 {
		t.Fatalf("expected %d ErrSetupAlreadyComplete, got %d", len(errs)-1, nSetupDone)
	}
}

func TestAuthRepository_ResolveAPIToken(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)
	ctx := context.Background()

	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	tokenHash := "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"

	if _, err := repo.CompleteInitialSetup(ctx, "alice", string(passHash), tokenHash, "t"); err != nil {
		t.Fatal(err)
	}

	uid, name, tokenID, err := repo.ResolveAPIToken(ctx, tokenHash)
	if err != nil {
		t.Fatalf("ResolveAPIToken: %v", err)
	}

	if name != "alice" || uid <= 0 || tokenID <= 0 {
		t.Fatalf("unexpected principal: id=%d name=%q tokenID=%d", uid, name, tokenID)
	}

	_, _, _, err = repo.ResolveAPIToken(ctx, "0000000000000000000000000000000000000000000000000000000000000000")
	if !IsNotFound(err) {
		t.Fatalf("expected ErrNotFound for unknown token hash, got %v", err)
	}
}

func TestAuthRepository_ResolveAPIToken_expired(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)
	ctx := context.Background()

	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	tokenHash := "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

	if _, err := repo.CompleteInitialSetup(ctx, "bob", string(passHash), tokenHash, "exp"); err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, rebindQueryForPgx(
		`UPDATE api_tokens SET expires_at = ? WHERE token_hash = ?`),
		time.Now().UTC().Add(-time.Hour),
		tokenHash,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, _, _, err = repo.ResolveAPIToken(ctx, tokenHash)
	if !IsNotFound(err) {
		t.Fatalf("expected ErrNotFound for expired token, got %v", err)
	}
}

func TestAuthRepository_TouchAPITokenUsed(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)
	ctx := context.Background()

	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	tokenHash := "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

	if _, err := repo.CompleteInitialSetup(ctx, "carol", string(passHash), tokenHash, "t"); err != nil {
		t.Fatal(err)
	}

	if err := repo.TouchAPITokenUsed(ctx, tokenHash); err != nil {
		t.Fatalf("TouchAPITokenUsed: %v", err)
	}

	var cnt int
	if err := db.QueryRowContext(ctx, rebindQueryForPgx(
		`SELECT COUNT(*) FROM api_tokens WHERE token_hash = ? AND last_used_at IS NOT NULL`),
		tokenHash,
	).Scan(&cnt); err != nil {
		t.Fatal(err)
	}

	if cnt != 1 {
		t.Fatalf("expected last_used_at set for token, count=%d", cnt)
	}
}

func TestAuthRepository_ListAuditEventsFiltersAndOrders(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)
	ctx := context.Background()

	actorID, err := repo.CreateLocalUser(ctx, "audit-primary", "hash")
	if err != nil {
		t.Fatalf("CreateLocalUser primary: %v", err)
	}
	otherActorID, err := repo.CreateLocalUser(ctx, "audit-other", "hash")
	if err != nil {
		t.Fatalf("CreateLocalUser other: %v", err)
	}

	base := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	events := []*AuditEventRecord{
		{
			Type:          "auth.success",
			ActorID:       sql.NullInt64{Int64: actorID, Valid: true},
			TargetID:      sql.NullInt64{Int64: 10, Valid: true},
			Metadata:      []byte(`{"old":true}`),
			IPAddress:     "127.0.0.1",
			CorrelationID: "corr-old",
			CreatedAt:     sql.NullTime{Time: base.Add(-2 * time.Hour), Valid: true},
		},
		{
			Type:          "token.created",
			ActorID:       sql.NullInt64{Int64: actorID, Valid: true},
			TargetID:      sql.NullInt64{Int64: 20, Valid: true},
			Metadata:      []byte(`{"label":"first"}`),
			IPAddress:     "127.0.0.2",
			CorrelationID: "corr-match",
			CreatedAt:     sql.NullTime{Time: base.Add(-1 * time.Hour), Valid: true},
		},
		{
			Type:          "token.created",
			ActorID:       sql.NullInt64{Int64: actorID, Valid: true},
			TargetID:      sql.NullInt64{Int64: 20, Valid: true},
			Metadata:      []byte(`{"label":"second"}`),
			IPAddress:     "127.0.0.3",
			CorrelationID: "corr-match",
			CreatedAt:     sql.NullTime{Time: base, Valid: true},
		},
		{
			Type:          "token.created",
			ActorID:       sql.NullInt64{Int64: otherActorID, Valid: true},
			TargetID:      sql.NullInt64{Int64: 20, Valid: true},
			Metadata:      []byte(`{"label":"other-actor"}`),
			CorrelationID: "corr-match",
			CreatedAt:     sql.NullTime{Time: base, Valid: true},
		},
	}
	if err := repo.InsertAuditEvents(ctx, events); err != nil {
		t.Fatalf("InsertAuditEvents: %v", err)
	}

	since := base.Add(-90 * time.Minute)
	until := base.Add(30 * time.Minute)
	got, err := repo.ListAuditEvents(ctx, AuditEventListFilter{
		EventType:     "token.created",
		ActorID:       sql.NullInt64{Int64: actorID, Valid: true},
		TargetID:      sql.NullInt64{Int64: 20, Valid: true},
		CorrelationID: "corr-match",
		Since:         &since,
		Until:         &until,
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("ListAuditEvents: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	if got[0].ID <= got[1].ID {
		t.Fatalf("events are not newest first by created_at/id: got ids %d then %d", got[0].ID, got[1].ID)
	}
	if string(got[0].Metadata) != `{"label":"second"}` {
		t.Fatalf("first metadata=%s", string(got[0].Metadata))
	}
	if got[0].IPAddress != "127.0.0.3" {
		t.Fatalf("first ip=%s", got[0].IPAddress)
	}
}

func TestAuthRepository_ChangePasswordAndRevokeTokens_revokesSessions(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)
	ctx := context.Background()

	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	tokenHash := "abababababababababababababababababababababababababababababababab"
	uid, err := repo.CompleteInitialSetup(ctx, "dana", string(passHash), tokenHash, "initial")
	if err != nil {
		t.Fatalf("CompleteInitialSetup: %v", err)
	}

	_, err = db.ExecContext(ctx, rebindQueryForPgx(
		`INSERT INTO api_sessions (session_hash, csrf_token_hash, local_user_id, expires_at_unix_nano, last_used_unix_nano) VALUES (?, ?, ?, ?, ?)`),
		"session-hash", "csrf-hash", uid, time.Now().UTC().Add(time.Hour).UnixNano(), time.Now().UTC().UnixNano(),
	)

	if err != nil {
		t.Fatalf("insert session: %v", err)
	}

	newHash, err := bcrypt.GenerateFromPassword([]byte("newpassword123"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	if err := repo.ChangePasswordAndRevokeTokens(ctx, uid, string(newHash)); err != nil {
		t.Fatalf("ChangePasswordAndRevokeTokens: %v", err)
	}

	for table, query := range map[string]string{
		"api_tokens":   `SELECT COUNT(*) FROM api_tokens WHERE local_user_id = ?`,
		"api_sessions": `SELECT COUNT(*) FROM api_sessions WHERE local_user_id = ?`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, rebindQueryForPgx(query), uid).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}

		if count != 0 {
			t.Fatalf("expected no %s rows after password change, got %d", table, count)
		}
	}
}

func TestAuthRepository_DisableLocalUserAndRevokeTokens(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)
	ctx := context.Background()

	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	tokenHash := "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
	uid, err := repo.CompleteInitialSetup(ctx, "disabled-user", string(passHash), tokenHash, "initial")
	if err != nil {
		t.Fatalf("CompleteInitialSetup: %v", err)
	}

	_, err = db.ExecContext(ctx, rebindQueryForPgx(
		`INSERT INTO api_sessions (session_hash, csrf_token_hash, local_user_id, expires_at_unix_nano, last_used_unix_nano) VALUES (?, ?, ?, ?, ?)`),
		"disabled-session-hash", "disabled-csrf-hash", uid, time.Now().UTC().Add(time.Hour).UnixNano(), time.Now().UTC().UnixNano(),
	)

	if err != nil {
		t.Fatalf("insert session: %v", err)
	}

	if err := repo.DisableLocalUserAndRevokeTokens(ctx, uid); err != nil {
		t.Fatalf("DisableLocalUserAndRevokeTokens: %v", err)
	}

	enabled, err := repo.UserEnabled(ctx, uid)
	if err != nil {
		t.Fatalf("UserEnabled: %v", err)
	}

	if enabled {
		t.Fatal("disabled user still enabled")
	}

	for table, query := range map[string]string{
		"api_tokens":   `SELECT COUNT(*) FROM api_tokens WHERE local_user_id = ?`,
		"api_sessions": `SELECT COUNT(*) FROM api_sessions WHERE local_user_id = ?`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, rebindQueryForPgx(query), uid).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}

		if count != 0 {
			t.Fatalf("expected no %s rows after disabling user, got %d", table, count)
		}
	}

	if err := repo.UpdateLocalUserEnabled(ctx, uid, true); err != nil {
		t.Fatalf("re-enable user: %v", err)
	}

	if _, _, _, err := repo.ResolveAPIToken(ctx, tokenHash); !IsNotFound(err) {
		t.Fatalf("old token resolved after disable/re-enable, err=%v", err)
	}
}

func TestAuthRepository_RootAdminQueries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repo := NewSQLAuthRepository(db)

	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := repo.CompleteInitialSetup(ctx, "root", string(passHash), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "t"); err != nil {
		t.Fatal(err)
	}

	rootCount, err := repo.CountEnabledRootAdmins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rootCount != 1 {
		t.Fatalf("expected 1 root admin after setup, got %d", rootCount)
	}

	rootIsRootAdmin, err := repo.IsUserRootAdmin(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !rootIsRootAdmin {
		t.Fatal("setup user should be root admin")
	}

	if _, err := db.ExecContext(ctx, rebindQueryForPgx(`INSERT INTO namespaces (name, path, parent_id) VALUES (?, ?, ?)`), "team-a", "/team-a", 1); err != nil {
		t.Fatal(err)
	}

	var teamNSID int64
	if err := db.QueryRowContext(ctx, rebindQueryForPgx(`SELECT id FROM namespaces WHERE path = ?`), "/team-a").Scan(&teamNSID); err != nil {
		t.Fatal(err)
	}

	teamAdminID, err := repo.CreateLocalUser(ctx, "team-admin", string(passHash))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.ExecContext(ctx, rebindQueryForPgx(`INSERT INTO role_bindings (local_user_id, namespace_id, role) VALUES (?, ?, ?)`), teamAdminID, teamNSID, "admin"); err != nil {
		t.Fatal(err)
	}

	rootCount, err = repo.CountEnabledRootAdmins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rootCount != 1 {
		t.Fatalf("expected namespace admin not to count as root admin, got %d", rootCount)
	}

	isTeamRootAdmin, err := repo.IsUserRootAdmin(ctx, teamAdminID)
	if err != nil {
		t.Fatal(err)
	}
	if isTeamRootAdmin {
		t.Fatal("namespace admin should not be considered root admin")
	}
}
