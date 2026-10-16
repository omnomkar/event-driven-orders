// Package migrations embeds the payment service's SQL schema migrations.
package migrations

import "embed"

// FS holds the goose migration files, compiled into the binary.
//
//go:embed *.sql
var FS embed.FS
