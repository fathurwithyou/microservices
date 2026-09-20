package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/auth/internal/domain"
	"github.com/fathurwithyou/microservices/services/auth/internal/ports"
)

func TestIssuePersistsHashedTokenMaterialAndScopes(t *testing.T) {
	store := newMemoryTokenStore()
	manager := NewTokenManager(time.Minute, store)
	seedClient(t, manager, "field-team-client", "secret", "hazard:read:raw")

	access, refresh, _, scopes, err := manager.Issue(context.Background(), "field-team-client", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if access == "" || refresh == "" {
		t.Fatal("token pair is empty")
	}
	if string(store.access[hashKey(hash(access))].Hash) == access {
		t.Fatal("access token was stored in plaintext")
	}
	if _, ok := store.refresh[hashKey(hash(refresh))]; !ok {
		t.Fatal("refresh token was not persisted")
	}
	if len(scopes) != 1 || scopes[0] != "hazard:read:raw" {
		t.Fatalf("unexpected scopes: %v", scopes)
	}
}

func TestIssueRejectsInvalidCredentials(t *testing.T) {
	store := newMemoryTokenStore()
	manager := NewTokenManager(time.Minute, store)
	seedClient(t, manager, "media-client", "secret", "hazard:read:summary")

	if _, _, _, _, err := manager.Issue(context.Background(), "media-client", "wrong"); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("invalid credentials error = %v, want ErrInvalidCredentials", err)
	}
}

func TestRefreshRotationRejectsOldToken(t *testing.T) {
	store := newMemoryTokenStore()
	manager := NewTokenManager(time.Minute, store)
	seedClient(t, manager, "field-team-client", "secret", "hazard:read:raw")
	_, refresh, _, _, err := manager.Issue(context.Background(), "field-team-client", "secret")
	if err != nil {
		t.Fatal(err)
	}

	_, rotated, _, clientID, scopes, err := manager.Refresh(context.Background(), refresh)
	if err != nil {
		t.Fatal(err)
	}
	if rotated == refresh {
		t.Fatal("refresh token was not rotated")
	}
	if clientID != "field-team-client" || len(scopes) != 1 || scopes[0] != "hazard:read:raw" {
		t.Fatalf("unexpected rotated owner: %s %v", clientID, scopes)
	}
	if _, _, _, _, _, err := manager.Refresh(context.Background(), refresh); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("old refresh token accepted: %v", err)
	}
}

func TestValidateAccessTokenExpires(t *testing.T) {
	store := newMemoryTokenStore()
	manager := NewTokenManager(time.Minute, store)
	seedClient(t, manager, "media-client", "secret", "hazard:read:summary")
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	manager.now = func() time.Time { return now }

	access, _, _, _, err := manager.Issue(context.Background(), "media-client", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := manager.Validate(context.Background(), access); err != nil {
		t.Fatalf("valid access token rejected: %v", err)
	}
	manager.now = func() time.Time { return now.Add(time.Minute) }
	if _, _, _, err := manager.Validate(context.Background(), access); !errors.Is(err, ErrInvalidAccessToken) {
		t.Fatalf("expired access token accepted: %v", err)
	}
}

func seedClient(t *testing.T, manager *TokenManager, clientID, secret, scope string) {
	t.Helper()
	if err := manager.SeedClients(context.Background(), map[string]domain.Client{
		clientID: {Secret: secret, Scopes: []string{scope}},
	}); err != nil {
		t.Fatal(err)
	}
}

type memoryTokenStore struct {
	clients map[string]domain.Client
	access  map[string]domain.AccessToken
	refresh map[string]memoryRefresh
}

type memoryRefresh struct {
	owner   domain.TokenOwner
	expires *time.Time
	revoked bool
}

func newMemoryTokenStore() *memoryTokenStore {
	return &memoryTokenStore{
		clients: make(map[string]domain.Client),
		access:  make(map[string]domain.AccessToken),
		refresh: make(map[string]memoryRefresh),
	}
}

func (s *memoryTokenStore) Ping(context.Context) error { return nil }

func (s *memoryTokenStore) EnsureClients(_ context.Context, clients map[string]domain.Client) error {
	for clientID, client := range clients {
		client.ID = clientID
		s.clients[clientID] = client
	}
	return nil
}

func (s *memoryTokenStore) AuthenticateClient(_ context.Context, clientID, secret string) (domain.Client, error) {
	client, ok := s.clients[clientID]
	if !ok || client.Secret != secret {
		return domain.Client{}, ports.ErrInvalidCredentials
	}
	return client, nil
}

func (s *memoryTokenStore) PersistTokenPair(_ context.Context, access domain.AccessToken, refresh domain.RefreshToken) error {
	s.access[hashKey(access.Hash)] = access
	s.refresh[hashKey(refresh.Hash)] = memoryRefresh{
		owner:   domain.TokenOwner{ClientID: refresh.ClientID, Scopes: append([]string(nil), refresh.Scopes...)},
		expires: refresh.ExpiresAt,
	}
	return nil
}

func (s *memoryTokenStore) ValidateAccessToken(_ context.Context, tokenHash []byte, now time.Time) (domain.AccessToken, error) {
	token, ok := s.access[hashKey(tokenHash)]
	if !ok || !token.ExpiresAt.After(now) {
		return domain.AccessToken{}, ports.ErrInvalidToken
	}
	return token, nil
}

func (s *memoryTokenStore) RotateRefreshToken(_ context.Context, oldHash []byte, access domain.AccessToken, refresh domain.RefreshToken) (domain.TokenOwner, error) {
	old, ok := s.refresh[hashKey(oldHash)]
	if !ok || old.revoked || old.expires != nil && !old.expires.After(time.Now()) {
		return domain.TokenOwner{}, ports.ErrInvalidToken
	}
	old.revoked = true
	s.refresh[hashKey(oldHash)] = old
	access.ClientID = old.owner.ClientID
	access.Scopes = append([]string(nil), old.owner.Scopes...)
	refresh.ClientID = old.owner.ClientID
	refresh.Scopes = append([]string(nil), old.owner.Scopes...)
	if err := s.PersistTokenPair(context.Background(), access, refresh); err != nil {
		return domain.TokenOwner{}, err
	}
	return old.owner, nil
}

func hashKey(value []byte) string { return string(value) }
