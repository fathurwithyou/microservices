// Package ports declares the outbound boundaries the ingestor consumes.
package ports

import (
	"context"
	"errors"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

// Sentinel errors adapters return so application code can branch without
// inspecting transport details.
var (
	// ErrSourceUnavailable marks a feed that could not be reached or
	// returned a non-success status.
	ErrSourceUnavailable = errors.New("source unavailable")
	// ErrSourceUnauthorized marks rejected credentials.
	ErrSourceUnauthorized = errors.New("source unauthorized")
	// ErrSourcePayload marks a response that could not be decoded.
	ErrSourcePayload = errors.New("source payload invalid")
)

// Source fetches raw readings from an upstream feed.
type Source interface {
	Fetch(ctx context.Context) ([]domain.Reading, error)
}

// CursorSource exposes the upstream paging cursor so the ingestor can persist
// it and resume after a restart without replaying the whole stream.
type CursorSource interface {
	Cursor() string
	SetCursor(string)
}
