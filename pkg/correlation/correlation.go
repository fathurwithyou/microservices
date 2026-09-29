// Package correlation provides the single correlation ID strategy shared by
// all services: UUID generation, HTTP header names, gRPC metadata keys, and
// context propagation helpers.
package correlation

import (
	"context"

	"github.com/google/uuid"
)

const (
	// Header is the HTTP header used to propagate correlation IDs.
	Header = "X-Correlation-ID"
	// GinKey is the gin context key under which the correlation ID is stored.
	GinKey = "correlation_id"
	// GRPCKey is the canonical gRPC metadata key for correlation IDs.
	GRPCKey = "correlation-id"
	// GRPCKeyAlt is the alternative gRPC metadata key accepted on inbound calls.
	GRPCKeyAlt = "x-correlation-id"
)

// New generates a new correlation ID.
func New() string {
	return uuid.NewString()
}

type contextKey struct{}

// WithContext returns a copy of ctx carrying the correlation ID.
func WithContext(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

// FromContext returns the correlation ID stored in ctx, or an empty string
// when none is present.
func FromContext(ctx context.Context) string {
	id, _ := ctx.Value(contextKey{}).(string)
	return id
}
