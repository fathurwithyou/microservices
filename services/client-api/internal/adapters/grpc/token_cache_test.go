package grpcadapter

import (
	"context"
	"testing"
	"time"

	authpb "github.com/fathurwithyou/microservices/contracts/gen/go/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type countingAuthServer struct {
	calls     int
	expiresAt time.Time
	valid     bool
}

func (s *countingAuthServer) ValidateToken(_ context.Context, request *authpb.ValidateTokenRequest, _ ...grpc.CallOption) (*authpb.ValidatedToken, error) {
	s.calls++
	if !s.valid {
		return nil, status.Error(codes.Unauthenticated, "invalid")
	}
	return &authpb.ValidatedToken{Valid: true, ClientId: "field-team-client", Scopes: []string{"hazards:read"}, ExpiresAt: s.expiresAt.Format(time.RFC3339Nano)}, nil
}

func (s *countingAuthServer) IssueToken(context.Context, *authpb.IssueTokenRequest, ...grpc.CallOption) (*authpb.TokenPair, error) {
	return nil, nil
}
func (s *countingAuthServer) RefreshToken(context.Context, *authpb.RefreshTokenRequest, ...grpc.CallOption) (*authpb.TokenPair, error) {
	return nil, nil
}
func (s *countingAuthServer) HealthCheck(context.Context, *authpb.HealthCheckRequest, ...grpc.CallOption) (*authpb.HealthCheckResponse, error) {
	return nil, nil
}

func TestCachingAuthReusesValidTokenUntilEarlierOfExpiryAndTTL(t *testing.T) {
	now := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	server := &countingAuthServer{valid: true, expiresAt: now.Add(5 * time.Second)}
	cache := newCachingAuth(&authClient{client: server})
	cache.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		if token, err := cache.Validate(context.Background(), "t-1"); err != nil || !token.Valid {
			t.Fatalf("validate: %+v %v", token, err)
		}
	}
	if server.calls != 1 {
		t.Fatalf("auth calls = %d, want 1", server.calls)
	}
	// Past the token's own expiry the cache must ask Auth again.
	cache.now = func() time.Time { return now.Add(6 * time.Second) }
	_, _ = cache.Validate(context.Background(), "t-1")
	if server.calls != 2 {
		t.Fatalf("auth calls after expiry = %d, want 2", server.calls)
	}
	// A long-lived token is still re-validated after tokenCacheTTL.
	server.expiresAt = now.Add(time.Hour)
	cache.now = func() time.Time { return now.Add(7 * time.Second) }
	_, _ = cache.Validate(context.Background(), "t-2")
	cache.now = func() time.Time { return now.Add(7*time.Second + tokenCacheTTL) }
	_, _ = cache.Validate(context.Background(), "t-2")
	if server.calls != 4 {
		t.Fatalf("auth calls after cache TTL = %d, want 4", server.calls)
	}
}

func TestCachingAuthNeverCachesRejections(t *testing.T) {
	server := &countingAuthServer{valid: false}
	cache := newCachingAuth(&authClient{client: server})
	for i := 0; i < 2; i++ {
		if _, err := cache.Validate(context.Background(), "bad"); err == nil {
			t.Fatal("invalid token accepted")
		}
	}
	if server.calls != 2 {
		t.Fatalf("auth calls = %d, want 2", server.calls)
	}
}
