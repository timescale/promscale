// +build ignore
// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

// This file is a binary that generates migration_files_generated.go
// it is not built by default, but rather invoked by the go:generate command
// defined in migrations.go
package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/shurcooL/vfsgen"
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

var Assets http.FileSystem = modTimeFS{
	fs: http.Dir("sql"),
}

func main() {
	err := vfsgen.Generate(Assets, vfsgen.Options{
		Filename:     "migration_files_generated.go",
		PackageName:  "migrations",
		VariableName: "MigrationFiles",
	})
	if err != nil {
		log.Fatalln(err)
	}
}
