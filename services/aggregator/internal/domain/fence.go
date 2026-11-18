package domain

import (
	"context"
	"errors"
)

// ErrFenced reports a write refused because the writer's lease term is no
// longer the current one (expired, or taken over with a newer fence).
var ErrFenced = errors.New("write fenced: lease term is no longer current")

// WriteFence identifies the lease term a write is made under. A store that
// supports fencing checks it inside the write transaction, so a process that
// was frozen after its last lease check cannot persist once it wakes up.
type WriteFence struct {
	Lease  string
	Holder string
	Fence  int64
}

type writeFenceKey struct{}

// WithWriteFence attaches the lease term to ctx.
func WithWriteFence(ctx context.Context, fence WriteFence) context.Context {
	return context.WithValue(ctx, writeFenceKey{}, fence)
}

// WriteFenceFrom returns the lease term attached to ctx, if any.
func WriteFenceFrom(ctx context.Context) (WriteFence, bool) {
	fence, ok := ctx.Value(writeFenceKey{}).(WriteFence)
	return fence, ok && fence.Lease != "" && fence.Holder != ""
}
