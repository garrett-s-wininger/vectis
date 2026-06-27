package auth

import (
	"context"
	"errors"
)

var (
	ErrInvalidCredentials = errors.New("auth: invalid credentials")
	ErrIdentityNotAllowed = errors.New("auth: identity is not allowed")
	ErrUnavailable        = errors.New("auth: provider unavailable")
)

type Identity struct {
	Provider    string
	Subject     string
	Username    string
	DisplayName string
	Attributes  map[string][]string
}

type LoginProvider interface {
	Authenticate(context.Context, string, string) (Identity, error)
}
