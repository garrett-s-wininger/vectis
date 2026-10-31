package httpsecurity

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalStaticFilesEnforceRootAndVisibility(t *testing.T) {
	t.Parallel()
	root, outside := t.TempDir(), t.TempDir()
	for _, dir := range []string{"assets", "guide", ".git"} {
		if err := os.Mkdir(filepath.Join(root, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}

	for name, body := range map[string]string{"index.html": "home", "assets/app.js": "public script", "guide/index.html": "guide", ".env": "PRIVATE", ".git/config": "PRIVATE"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("PRIVATE"), 0600); err != nil {
		t.Fatal(err)
	}

	fileLinkErr := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(root, "leak.txt"))
	dirLinkErr := os.Symlink(outside, filepath.Join(root, "alias"))
	h := http.FileServer(LocalStaticFileSystem(root))
	for _, tc := range []struct {
		path   string
		status int
		body   string
	}{
		{"/", 200, "home"}, {"/assets/app.js", 200, "public script"}, {"/guide/", 200, "guide"},
		{"/.env", 404, ""}, {"/.git/config", 404, ""}, {"/assets/", 404, ""},
		{"/leak.txt", 404, ""}, {"/alias/secret.txt", 404, ""}, {"/missing", 404, ""},
	} {
		t.Run(tc.path, func(t *testing.T) {
			if tc.path == "/leak.txt" && fileLinkErr != nil {
				t.Skipf("symlink unavailable: %v", fileLinkErr)
			}

			if tc.path == "/alias/secret.txt" && dirLinkErr != nil {
				t.Skipf("symlink unavailable: %v", dirLinkErr)
			}

			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if w.Code != tc.status || (tc.body != "" && w.Body.String() != tc.body) || strings.Contains(w.Body.String(), "PRIVATE") {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}

	file, err := LocalStaticFileSystem(root).Open("/guide")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	if _, err := file.Readdir(-1); err == nil {
		t.Fatal("directory enumeration allowed")
	}

	if _, err := io.ReadAll(file); err == nil {
		t.Fatal("directory contents read as regular file")
	}
}
