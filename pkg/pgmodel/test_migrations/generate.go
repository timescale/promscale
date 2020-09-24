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

	"github.com/shurcooL/vfsgen"
	"github.com/timescale/promscale/pkg/pgmodel/migrations"
)

var Assets http.FileSystem = migrations.NewModTimeFs(http.Dir("sql"))

func main() {
	err := vfsgen.Generate(Assets, vfsgen.Options{
		Filename:     "migration_files_generated.go",
		PackageName:  "test_migrations",
		VariableName: "MigrationFiles",
	})
	if err != nil {
		log.Fatalln(err)
	}
}
