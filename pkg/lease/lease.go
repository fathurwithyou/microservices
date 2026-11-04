// Package lease provides database-time leases with fencing tokens for
// single-writer coordination. The Store is the source of truth and must be
// implemented by the caller using the database clock; Coordinator never reads
// the local clock.
package lease

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Token identifies one successful acquisition. Fence increases by exactly one
// on every acquisition, so consumers can reject writes from stale holders.
type Token struct {
	Holder string
	Fence  int64
	Expiry time.Time
}

// Store persists the single active lease. Implementations must use the
// database clock for expiry and must make Acquire atomic, so that only one
// caller wins when several race.
type Store interface {
	Acquire(ctx context.Context, holder string, ttl time.Duration) (Token, bool, error)
	Renew(ctx context.Context, token Token, ttl time.Duration) (Token, bool, error)
	Release(ctx context.Context, token Token) error
}

// Coordinator tracks the token this process currently holds and refuses to
// act on tokens that were superseded.
type Coordinator struct {
	store Store

	mu    sync.Mutex
	token Token
	held  bool
}

// New returns a Coordinator backed by store.
func New(store Store) *Coordinator {
	return &Coordinator{store: store}
}

// Acquire attempts to take the lease for holder. It reports false when the
// lease is held by someone else and still unexpired.
func (c *Coordinator) Acquire(ctx context.Context, holder string, ttl time.Duration) (Token, bool, error) {
	token, ok, err := c.store.Acquire(ctx, holder, ttl)
	if err != nil {
		return Token{}, false, fmt.Errorf("acquire lease: %w", err)
	}
	if !ok {
		return Token{}, false, nil
	}
	c.set(token)
	return token, true, nil
}

// Renew extends token's lease. A token that is not the one this coordinator
// holds, or that the store no longer recognises, is rejected.
func (c *Coordinator) Renew(ctx context.Context, token Token, ttl time.Duration) (Token, bool, error) {
	if !c.current(token) {
		return Token{}, false, nil
	}
	renewed, ok, err := c.store.Renew(ctx, token, ttl)
	if err != nil {
		return Token{}, false, fmt.Errorf("renew lease: %w", err)
	}
	if !ok {
		c.clear(token)
		return Token{}, false, nil
	}
	c.set(renewed)
	return renewed, true, nil
}

// Release drops the lease if token is still the current one. A stale token is
// ignored so it can never release a newer holder's lease.
func (c *Coordinator) Release(ctx context.Context, token Token) error {
	if !c.current(token) {
		return nil
	}
	if err := c.store.Release(ctx, token); err != nil {
		return fmt.Errorf("release lease: %w", err)
	}
	c.clear(token)
	return nil
}

// Fence returns the token this coordinator currently holds, or the zero Token
// when it holds none.
func (c *Coordinator) Fence() Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.held {
		return Token{}
	}
	return c.token
}

func (c *Coordinator) set(token Token) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = token
	c.held = true
}

func (c *Coordinator) current(token Token) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.held && c.token.Holder == token.Holder && c.token.Fence == token.Fence
}

func (c *Coordinator) clear(token Token) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.held && c.token.Holder == token.Holder && c.token.Fence == token.Fence {
		c.token = Token{}
		c.held = false
	}
}
