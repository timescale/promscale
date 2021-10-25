// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package migrations

// This is a stub to define the go:generate command to create the go file
// that embeds the sql files into a go variable to make the sql files part
// of the binary

//go:generate go run -tags=dev generate.go

// pin vfsgen version in go mod. It's used in generate.go but that isn't picked up
// because it uses "ignore" tag. See https://github.com/shurcooL/vfsgen/issues/83
import _ "github.com/shurcooL/vfsgen"
