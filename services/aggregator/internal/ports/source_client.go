package ports

import (
	"context"
	"encoding/json"
)

type SourceClient interface {
	Get(context.Context, string, string) ([]map[string]json.RawMessage, error)
}
