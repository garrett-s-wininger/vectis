package encryptedfs

import (
	"context"
	"io/fs"
	"net/url"
	"path/filepath"
	"strings"
)

func (p *EncryptedFSProvider) ListReferences(ctx context.Context) ([]string, error) {
	refs := []string{}
	err := filepath.WalkDir(p.root, func(path string, entry fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil {
			return err
		}

		if !entry.Type().IsRegular() {
			return nil
		}

		rel, err := filepath.Rel(p.root, path)
		if err != nil {
			return err
		}

		parts := strings.SplitN(filepath.ToSlash(rel), "/", 2)
		u := url.URL{Scheme: EncryptedFSScheme, Host: parts[0]}
		if len(parts) > 1 {
			u.Path = "/" + parts[1]
		}

		ref := u.String()
		if normalized, err := encryptedFSRelativePath(ref); err == nil && normalized == filepath.ToSlash(rel) {
			refs = append(refs, ref)
		}

		return nil
	})

	return refs, err
}
