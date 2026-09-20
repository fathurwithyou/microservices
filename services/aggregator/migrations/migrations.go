package migrations

import "embed"

const Dir = "sql"

//go:embed sql/*.sql
var FS embed.FS
