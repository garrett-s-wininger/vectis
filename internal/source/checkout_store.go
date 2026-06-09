package source

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
)

const checkoutStoreHashLen = 12

// CheckoutStore maps repository IDs onto stable paths under a Vectis-owned root.
type CheckoutStore struct {
	root string
}

func NewCheckoutStore(root string) (CheckoutStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return CheckoutStore{}, fmt.Errorf("%w: checkout root is required", ErrInvalidReference)
	}
	if !filepath.IsAbs(root) {
		return CheckoutStore{}, fmt.Errorf("%w: checkout root must be absolute", ErrInvalidReference)
	}

	return CheckoutStore{root: filepath.Clean(root)}, nil
}

func (s CheckoutStore) Root() string {
	return s.root
}

func (s CheckoutStore) Path(repositoryID string) (string, error) {
	repositoryID = strings.TrimSpace(repositoryID)
	if repositoryID == "" {
		return "", fmt.Errorf("%w: repository_id is required", ErrInvalidReference)
	}

	if strings.ContainsRune(repositoryID, 0) {
		return "", fmt.Errorf("%w: repository_id contains a NUL byte", ErrInvalidReference)
	}

	hash := sha256.Sum256([]byte(repositoryID))
	hashSuffix := hex.EncodeToString(hash[:])[:checkoutStoreHashLen]
	return filepath.Join(s.root, checkoutSlug(repositoryID)+"-"+hashSuffix), nil
}

func checkoutSlug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))

	var b strings.Builder
	lastDash := false
	for _, r := range value {
		valid := (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') ||
			r == '-' ||
			r == '_' ||
			r == '.'

		if valid {
			b.WriteRune(r)
			lastDash = false
			continue
		}

		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}

	slug := strings.Trim(b.String(), "-.")
	if slug == "" || slug == "." || slug == ".." {
		return "repo"
	}
	if len(slug) > 64 {
		slug = strings.Trim(slug[:64], "-.")
		if slug == "" || slug == "." || slug == ".." {
			return "repo"
		}
	}

	return slug
}
