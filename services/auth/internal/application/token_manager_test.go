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
	manager := newTestManager(t, store, nil)
	seedClient(t, manager, "field-team-client", "secret", "hazard:read:raw")

	pair, err := manager.Issue(context.Background(), "field-team-client", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if pair.AccessToken == "" || pair.RefreshToken == "" {
		t.Fatal("token pair is empty")
	}
	if pair.ClientID != "field-team-client" {
		t.Fatalf("token pair client = %q, want field-team-client", pair.ClientID)
	}
	if string(store.access[hashKey(hash(pair.AccessToken))].Hash) == pair.AccessToken {
		t.Fatal("access token was stored in plaintext")
	}
	if _, ok := store.refresh[hashKey(hash(pair.RefreshToken))]; !ok {
		t.Fatal("refresh token was not persisted")
	}
	if len(pair.Scopes) != 1 || pair.Scopes[0] != "hazard:read:raw" {
		t.Fatalf("unexpected scopes: %v", pair.Scopes)
	}
}

func TestIssueRejectsInvalidCredentials(t *testing.T) {
	store := newMemoryTokenStore()
	manager := newTestManager(t, store, nil)
	seedClient(t, manager, "media-client", "secret", "hazard:read:summary")

	if _, err := manager.Issue(context.Background(), "media-client", "wrong"); !errors.Is(err, ports.ErrInvalidCredentials) {
		t.Fatalf("invalid credentials error = %v, want ports.ErrInvalidCredentials", err)
	}
}

func TestRefreshRotationRejectsOldToken(t *testing.T) {
	store := newMemoryTokenStore()
	manager := newTestManager(t, store, nil)
	seedClient(t, manager, "field-team-client", "secret", "hazard:read:raw")
	issued, err := manager.Issue(context.Background(), "field-team-client", "secret")
	if err != nil {
		t.Fatal(err)
	}

	rotated, err := manager.Refresh(context.Background(), issued.RefreshToken)
	if err != nil {
		t.Fatal(err)
	}
	if rotated.RefreshToken == issued.RefreshToken {
		t.Fatal("refresh token was not rotated")
	}
	if rotated.ClientID != "field-team-client" || len(rotated.Scopes) != 1 || rotated.Scopes[0] != "hazard:read:raw" {
		t.Fatalf("unexpected rotated owner: %s %v", rotated.ClientID, rotated.Scopes)
	}
	if _, err := manager.Refresh(context.Background(), issued.RefreshToken); !errors.Is(err, ErrInvalidRefreshToken) {
		t.Fatalf("old refresh token accepted: %v", err)
	}
}

func TestValidateAccessTokenExpires(t *testing.T) {
	store := newMemoryTokenStore()
	current := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	manager := newTestManager(t, store, func() time.Time { return current })
	seedClient(t, manager, "media-client", "secret", "hazard:read:summary")

	pair, err := manager.Issue(context.Background(), "media-client", "secret")
	if err != nil {
		t.Fatal(err)
	}
	clientID, scopes, expiresAt, err := manager.Validate(context.Background(), pair.AccessToken)
	if err != nil {
		t.Fatalf("valid access token rejected: %v", err)
	}
	if clientID != "media-client" || len(scopes) != 1 || scopes[0] != "hazard:read:summary" {
		t.Fatalf("unexpected validated session: %s %v", clientID, scopes)
	}
	if want := current.Add(time.Minute); !expiresAt.Equal(want) {
		t.Fatalf("expiresAt = %s, want %s", expiresAt, want)
	}

	current = current.Add(time.Minute)
	if _, _, _, err := manager.Validate(context.Background(), pair.AccessToken); !errors.Is(err, ErrInvalidAccessToken) {
		t.Fatalf("expired access token accepted: %v", err)
	}
}

func TestNewTokenManagerValidatesConfig(t *testing.T) {
	if _, err := NewTokenManager(TokenManagerConfig{AccessTokenTTL: time.Minute}); err == nil {
		t.Fatal("nil repository accepted")
	}
	if _, err := NewTokenManager(TokenManagerConfig{Repository: newMemoryTokenStore()}); err == nil {
		t.Fatal("zero AccessTokenTTL accepted")
	}
}

func TestIssueUsesInjectedClockAndTokenGenerator(t *testing.T) {
	store := newMemoryTokenStore()
	fixed := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	tokens := []string{"access-token-1", "refresh-token-1"}
	next := 0
	manager, err := NewTokenManager(TokenManagerConfig{
		Repository:     store,
		AccessTokenTTL: time.Minute,
		Now:            func() time.Time { return fixed },
		GenerateToken: func() (string, error) {
			value := tokens[next]
			next++
			return value, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	seedClient(t, manager, "media-client", "secret", "hazard:read:summary")

	pair, err := manager.Issue(context.Background(), "media-client", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if pair.AccessToken != "access-token-1" || pair.RefreshToken != "refresh-token-1" {
		t.Fatalf("unexpected token pair: %+v", pair)
	}
	if !pair.ExpiresAt.Equal(fixed.Add(time.Minute)) {
		t.Fatalf("expiresAt = %s, want %s", pair.ExpiresAt, fixed.Add(time.Minute))
	}
}

func TestDefaultTokenGeneratorProducesHex(t *testing.T) {
	token, err := randomToken()
	if err != nil {
		t.Fatal(err)
	}
	if len(token) != tokenBytes*2 {
		t.Fatalf("token length = %d, want %d", len(token), tokenBytes*2)
	}
}

func newTestManager(t *testing.T, store ports.TokenRepository, now func() time.Time) *TokenManager {
	t.Helper()
	manager, err := NewTokenManager(TokenManagerConfig{Repository: store, AccessTokenTTL: time.Minute, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	return manager
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

func (s *memoryTokenStore) RotateRefreshToken(_ context.Context, oldHash []byte, access domain.AccessToken, refresh domain.RefreshToken, now time.Time) (domain.TokenOwner, error) {
	old, ok := s.refresh[hashKey(oldHash)]
	if !ok || old.revoked || (old.expires != nil && !old.expires.After(now)) {
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
