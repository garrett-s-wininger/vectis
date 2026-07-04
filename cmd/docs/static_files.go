package main

import (
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type docsStaticFileSystem struct {
	base http.FileSystem
}

type docsNoListingFile struct {
	http.File
}

type docsLocalFileSystem struct {
	root string
}

func docsStaticFileServer(fsys http.FileSystem) http.Handler {
	return http.FileServer(docsStaticFileSystem{base: fsys})
}

func newDocsLocalFileSystem(root string) (http.FileSystem, error) {
	clean, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}

	clean, err = filepath.EvalSymlinks(clean)
	if err != nil {
		return nil, err
	}

	return docsStaticFileSystem{base: docsLocalFileSystem{root: clean}}, nil
}

func (fsys docsStaticFileSystem) Open(name string) (http.File, error) {
	clean, ok := cleanDocsStaticPath(name)
	if !ok {
		return nil, fs.ErrNotExist
	}

	file, err := fsys.base.Open(clean)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	if info.IsDir() {
		if !fsys.hasDirectoryIndex(clean) {
			_ = file.Close()
			return nil, fs.ErrNotExist
		}

		return docsNoListingFile{File: file}, nil
	}

	return file, nil
}

func (fsys docsStaticFileSystem) hasDirectoryIndex(dir string) bool {
	index, err := fsys.base.Open(path.Join(dir, "index.html"))
	if err != nil {
		return false
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(index)

	info, err := index.Stat()
	return err == nil && !info.IsDir()
}

func (f docsNoListingFile) Readdir(int) ([]fs.FileInfo, error) {
	return nil, fs.ErrPermission
}

func (fsys docsLocalFileSystem) Open(name string) (http.File, error) {
	clean, ok := cleanDocsStaticPath(name)
	if !ok {
		return nil, fs.ErrNotExist
	}

	resolved, err := fsys.resolve(clean)
	if err != nil {
		return nil, err
	}

	return os.Open(resolved)
}

func (fsys docsLocalFileSystem) resolve(name string) (string, error) {
	rel := strings.TrimPrefix(name, "/")
	candidate := filepath.Join(fsys.root, filepath.FromSlash(rel))
	if !docsPathWithinRoot(fsys.root, candidate) {
		return "", fs.ErrNotExist
	}

	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", err
	}

	if !docsPathWithinRoot(fsys.root, resolved) {
		return "", fs.ErrNotExist
	}

	return resolved, nil
}

func cleanDocsStaticPath(name string) (string, bool) {
	if strings.Contains(name, "\\") {
		return "", false
	}

	clean := path.Clean("/" + name)
	for _, segment := range strings.Split(strings.Trim(clean, "/"), "/") {
		if segment == "" {
			continue
		}

		if strings.HasPrefix(segment, ".") {
			return "", false
		}
	}

	return clean, true
}

func docsPathWithinRoot(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	if err != nil {
		return false
	}

	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}
