// +build ignore
// This file is a binary that generates migration_files_generated.go
// it is not built by default, but rather invoked by the go:generate command
// defined in migrations.go

package main

import (
	"log"
	"net/http"

	"github.com/shurcooL/vfsgen"
)

var Assets http.FileSystem = http.Dir("sql")

func main() {
	err := vfsgen.Generate(Assets, vfsgen.Options{
		Filename:     "migration_files_generated.go",
		PackageName:  "migrations",
		VariableName: "SqlFiles",
	})
	if err != nil {
		log.Fatalln(err)
	}
}
