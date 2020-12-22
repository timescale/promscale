// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package migrations

import (
	"net/http"
	"os"
	"time"
)

// modTimeFS is an http.FileSystem wrapper that modifies
// underlying fs such that all of its file mod times are set to zero.
type modTimeFS struct {
	fs http.FileSystem
}

func (fs modTimeFS) Open(name string) (http.File, error) {
	f, err := fs.fs.Open(name)
	if err != nil {
		return nil, err
	}
	return modTimeFile{f}, nil
}

type modTimeFile struct {
	http.File
}

func (f modTimeFile) Stat() (os.FileInfo, error) {
	fi, err := f.File.Stat()
	if err != nil {
		return nil, err
	}
	return modTimeFileInfo{fi}, nil
}

type modTimeFileInfo struct {
	os.FileInfo
}

func (modTimeFileInfo) ModTime() time.Time {
	return time.Time{}
}

func NewModTimeFs(sub http.FileSystem) http.FileSystem {
	return modTimeFS{
		fs: sub,
	}
}
