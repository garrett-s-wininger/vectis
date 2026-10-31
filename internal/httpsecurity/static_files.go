package httpsecurity

import (
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"
)

func StaticFileSystem(base http.FileSystem) http.FileSystem { return staticFiles{base: base} }

func LocalStaticFileSystem(root string) http.FileSystem {
	return StaticFileSystem(localStaticFiles{root: root})
}

type localStaticFiles struct{ root string }

func (f localStaticFiles) Open(name string) (http.File, error) {
	clean, ok := cleanStaticPath(name)
	if !ok {
		return nil, fs.ErrNotExist
	}

	relative := strings.TrimPrefix(clean, "/")
	if relative == "" {
		relative = "."
	}

	file, err := os.OpenInRoot(f.root, relative)
	if err != nil {
		return nil, fs.ErrNotExist
	}

	return file, nil
}

type staticFiles struct{ base http.FileSystem }
type noListingFile struct{ http.File }

func (f noListingFile) Readdir(int) ([]fs.FileInfo, error) { return nil, fs.ErrPermission }
func (f staticFiles) Open(name string) (http.File, error) {
	clean, ok := cleanStaticPath(name)
	if !ok {
		return nil, fs.ErrNotExist
	}

	file, err := f.base.Open(clean)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	if !info.IsDir() {
		return file, nil
	}

	index, err := f.base.Open(path.Join(clean, "index.html"))
	if err != nil {
		_ = file.Close()
		return nil, fs.ErrNotExist
	}

	indexInfo, err := index.Stat()
	_ = index.Close()
	if err != nil || !indexInfo.Mode().IsRegular() {
		_ = file.Close()
		return nil, fs.ErrNotExist
	}

	return noListingFile{File: file}, nil
}

func cleanStaticPath(name string) (string, bool) {
	if strings.ContainsAny(name, "\\\x00") {
		return "", false
	}

	for _, segment := range strings.Split(name, "/") {
		if strings.HasPrefix(segment, ".") {
			return "", false
		}
	}

	return path.Clean("/" + name), true
}
