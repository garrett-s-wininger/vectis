package auth

import (
	"errors"
	"testing"
)

func TestErrorsAreComparable(t *testing.T) {
	if !errors.Is(ErrInvalidCredentials, ErrInvalidCredentials) {
		t.Fatal("ErrInvalidCredentials should be errors.Is comparable")
	}
	if !errors.Is(ErrIdentityNotAllowed, ErrIdentityNotAllowed) {
		t.Fatal("ErrIdentityNotAllowed should be errors.Is comparable")
	}
	if !errors.Is(ErrUnavailable, ErrUnavailable) {
		t.Fatal("ErrUnavailable should be errors.Is comparable")
	}
}
