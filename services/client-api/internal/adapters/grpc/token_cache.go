package grpcadapter

import (
	"context"
	"crypto/sha256"
	"sync"
	"time"

	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
)

// tokenCacheTTL bounds how long a positive validation is reused. Without it
// every API request costs an Auth RPC plus a PostgreSQL lookup, which caps
// throughput no matter how many Aggregator instances run (M2 P2.7). The cost
// is that a revoked token keeps working for at most this long.
const tokenCacheTTL = 15 * time.Second

// tokenCacheMax caps memory; on overflow expired entries are dropped first,
// then the whole cache is reset.
const tokenCacheMax = 50000

type cachedToken struct {
	token ports.ValidatedToken
	until time.Time
}

// cachingAuth reuses successful token validations until the earlier of the
// token's own expiry and tokenCacheTTL. Invalid tokens are never cached.
type cachingAuth struct {
	next    *authClient
	now     func() time.Time
	mu      sync.Mutex
	entries map[[sha256.Size]byte]cachedToken
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

	validated, expiresAt, err := c.next.validate(ctx, accessToken)
	if err != nil || !validated.Valid {
		return validated, err
	}
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
