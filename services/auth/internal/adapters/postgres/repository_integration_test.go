package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/auth/internal/domain"
	"github.com/fathurwithyou/microservices/services/auth/internal/ports"
)

func TestStorePersistsCredentialsAndRotatesRefreshToken(t *testing.T) {
	databaseURL := os.Getenv("AUTH_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set AUTH_DATABASE_URL to run the PostgreSQL persistence test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := Connect(ctx, databaseURL, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}

	store := NewRepository(pool)
	clientID := fmt.Sprintf("auth-test-%d", time.Now().UnixNano())
	client := domain.Client{ID: clientID, Secret: "test-secret", Scopes: []string{"hazard:read:raw"}}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM refresh_tokens WHERE client_id = $1`, clientID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM access_tokens WHERE client_id = $1`, clientID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM auth_clients WHERE client_id = $1`, clientID)
	})

	if err := store.EnsureClients(ctx, map[string]domain.Client{clientID: client}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateClient(ctx, clientID, "wrong-secret"); !errors.Is(err, ports.ErrInvalidCredentials) {
		t.Fatalf("wrong secret error = %v, want invalid credentials", err)
	}
	if _, err := store.AuthenticateClient(ctx, clientID, client.Secret); err != nil {
		t.Fatalf("stored bcrypt credential rejected: %v", err)
	}

	now := time.Now().UTC()
	access := domain.AccessToken{
		Hash:      []byte(fmt.Sprintf("access-%d", now.UnixNano())),
		ClientID:  clientID,
		Scopes:    client.Scopes,
		ExpiresAt: now.Add(time.Minute),
	}
	refresh := domain.RefreshToken{Hash: []byte(fmt.Sprintf("refresh-%d", now.UnixNano())), ClientID: clientID, Scopes: client.Scopes}
	if err := store.PersistTokenPair(ctx, access, refresh); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ValidateAccessToken(ctx, access.Hash, now); err != nil {
		t.Fatalf("persisted access token rejected: %v", err)
	}

	rotatedAccess := domain.AccessToken{Hash: []byte(fmt.Sprintf("access-rotated-%d", now.UnixNano())), ExpiresAt: now.Add(time.Minute)}
	rotatedRefresh := domain.RefreshToken{Hash: []byte(fmt.Sprintf("refresh-rotated-%d", now.UnixNano()))}
	owner, err := store.RotateRefreshToken(ctx, refresh.Hash, rotatedAccess, rotatedRefresh)
	if err != nil {
		t.Fatal(err)
	}
	if owner.ClientID != clientID || len(owner.Scopes) != 1 || owner.Scopes[0] != "hazard:read:raw" {
		t.Fatalf("unexpected rotated owner: %+v", owner)
	}
	if _, err := store.RotateRefreshToken(ctx, refresh.Hash, rotatedAccess, rotatedRefresh); !errors.Is(err, ports.ErrInvalidToken) {
		t.Fatalf("old refresh token error = %v, want invalid token", err)
	}
}
