package grpcadapter

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
	"golang.org/x/sync/singleflight"
)

// tokenCacheTTL bounds how long a positive validation is reused. Without it
// every API request costs an Auth RPC plus a PostgreSQL lookup, which caps
// throughput no matter how many Aggregator instances run (M2 P2.7). The cost
// is that a revoked token keeps working for at most this long.
const tokenCacheTTL = 15 * time.Second

// tokenCacheMax caps memory; on overflow expired entries are dropped first,
// then the whole cache is reset.
const tokenCacheMax = 50000

// tokenValidateTimeout bounds one shared validation, detached from the first
// caller's context so its cancellation does not fail the others.
const tokenValidateTimeout = 5 * time.Second

type cachedToken struct {
	token ports.ValidatedToken
	until time.Time
}

// cachingAuth reuses successful token validations until the earlier of the
// token's own expiry and tokenCacheTTL. Invalid tokens are never cached.
//
// Misses for the same token are coalesced: when a cached entry expires under
// load, thousands of concurrent requests carrying that token used to call
// Auth at once (a thundering herd every tokenCacheTTL); now one call per
// token is in flight and every waiter shares its answer.
type cachingAuth struct {
	next    *authClient
	now     func() time.Time
	mu      sync.Mutex
	entries map[[sha256.Size]byte]cachedToken
	flight  singleflight.Group
}

type validation struct {
	token     *ports.ValidatedToken
	expiresAt time.Time
}

func newCachingAuth(next *authClient) *cachingAuth {
	return &cachingAuth{next: next, now: time.Now, entries: make(map[[sha256.Size]byte]cachedToken)}
}

func (c *cachingAuth) Issue(ctx context.Context, clientID, secret string) (*ports.TokenPair, error) {
	return c.next.Issue(ctx, clientID, secret)
}

func (c *cachingAuth) Refresh(ctx context.Context, refreshToken string) (*ports.TokenPair, error) {
	return c.next.Refresh(ctx, refreshToken)
}

func (c *cachingAuth) Validate(ctx context.Context, accessToken string) (*ports.ValidatedToken, error) {
	key := sha256.Sum256([]byte(accessToken))
	now := c.now()
	c.mu.Lock()
	entry, ok := c.entries[key]
	c.mu.Unlock()
	if ok && now.Before(entry.until) {
		token := entry.token
		token.Scopes = append([]string(nil), entry.token.Scopes...)
		return &token, nil
	}

	answer := c.flight.DoChan(string(key[:]), func() (any, error) {
		shared, cancel := context.WithTimeout(context.WithoutCancel(ctx), tokenValidateTimeout)
		defer cancel()
		token, expiresAt, err := c.next.validate(shared, accessToken)
		return validation{token: token, expiresAt: expiresAt}, err
	})
	var result singleflight.Result
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result = <-answer:
	}
	shared, _ := result.Val.(validation)
	if result.Err != nil || shared.token == nil || !shared.token.Valid {
		return copyToken(shared.token), result.Err
	}
	validated, expiresAt := copyToken(shared.token), shared.expiresAt
	until := now.Add(tokenCacheTTL)
	if !expiresAt.IsZero() && expiresAt.Before(until) {
		until = expiresAt
	}
	c.mu.Lock()
	if len(c.entries) >= tokenCacheMax {
		for k, v := range c.entries {
			if !now.Before(v.until) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= tokenCacheMax {
			c.entries = make(map[[sha256.Size]byte]cachedToken)
		}
	}
	c.entries[key] = cachedToken{token: *validated, until: until}
	c.mu.Unlock()
	return validated, nil
}

// copyToken gives each caller its own value; coalesced callers share one.
func copyToken(token *ports.ValidatedToken) *ports.ValidatedToken {
	if token == nil {
		return nil
	}
	copied := *token
	copied.Scopes = append([]string(nil), token.Scopes...)
	return &copied
}
