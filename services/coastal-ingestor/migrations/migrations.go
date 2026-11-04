// Package migrations embeds the coastal-ingestor schema migrations.
package migrations

import "embed"

// Dir is the directory inside FS that goose reads.
const Dir = "sql"

//go:embed sql/*.sql
var FS embed.FS
