package migrations

import "embed"

// FS contains the Auth Service schema applied during service startup.
//
//go:embed 001_init.sql
var FS embed.FS
