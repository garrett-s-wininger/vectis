package dal

import (
	"context"
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

	uid, name, err := repo.ResolveAPIToken(ctx, tokenHash)
	if err != nil {
		t.Fatalf("ResolveAPIToken: %v", err)
	}

	if name != "alice" || uid <= 0 {
		t.Fatalf("unexpected principal: id=%d name=%q", uid, name)
	}

	_, _, err = repo.ResolveAPIToken(ctx, "0000000000000000000000000000000000000000000000000000000000000000")
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

	_, _, err = repo.ResolveAPIToken(ctx, tokenHash)
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
