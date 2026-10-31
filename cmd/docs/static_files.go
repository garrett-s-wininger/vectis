package main

import (
	"net/http"
	"path/filepath"
	"vectis/internal/httpsecurity"
)

func docsStaticFileServer(fsys http.FileSystem) http.Handler {
	return http.FileServer(httpsecurity.StaticFileSystem(fsys))
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

	return httpsecurity.LocalStaticFileSystem(clean), nil
}
