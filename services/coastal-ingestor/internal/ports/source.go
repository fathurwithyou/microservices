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
//
// Fetch may return readings together with an error: the readings of every
// page that was read completely before a later page failed, and the complete
// items salvaged from a damaged page. The caller processes them (a critical
// reading must not wait for the feed to recover) and still counts the error
// as a source failure.
type Source interface {
	Fetch(ctx context.Context) ([]domain.Reading, error)
}

// FeedStatusSource is implemented by sources that can describe the feed as a
// whole (its clock and how old its newest data is).
type FeedStatusSource interface {
	FeedStatus() domain.FeedStatus
}

// CursorSource exposes the upstream paging cursor so the ingestor can persist
// it and resume after a restart without replaying the whole stream.
type CursorSource interface {
	Cursor() string
	SetCursor(string)
}
