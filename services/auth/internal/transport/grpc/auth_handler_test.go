package grpctransport

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	authpb "github.com/fathurwithyou/microservices/contracts/gen/go/auth"
	"github.com/fathurwithyou/microservices/services/auth/internal/application"
	"github.com/fathurwithyou/microservices/services/auth/internal/domain"
	"github.com/fathurwithyou/microservices/services/auth/internal/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var handlerNow = time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

func TestIssueTokenMapsTokenPair(t *testing.T) {
	handler, _ := newTestHandler(t, nil)

	response, err := handler.IssueToken(context.Background(), &authpb.IssueTokenRequest{
		ClientId:     "media-client",
		ClientSecret: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetAccessToken() != "access-token-1" || response.GetRefreshToken() != "refresh-token-1" {
		t.Fatalf("unexpected token pair: %+v", response)
	}
	if want := handlerNow.Add(time.Minute).Format(time.RFC3339Nano); response.GetExpiresAt() != want {
		t.Fatalf("expiresAt = %q, want %q", response.GetExpiresAt(), want)
	}
	if response.GetClientId() != "media-client" {
		t.Fatalf("clientId = %q, want media-client", response.GetClientId())
	}
	if len(response.GetScopes()) != 1 || response.GetScopes()[0] != "hazard:read:summary" {
		t.Fatalf("unexpected scopes: %v", response.GetScopes())
	}
}

func TestIssueTokenRejectsInvalidCredentials(t *testing.T) {
	handler, _ := newTestHandler(t, nil)

	_, err := handler.IssueToken(context.Background(), &authpb.IssueTokenRequest{
		ClientId:     "media-client",
		ClientSecret: "wrong",
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("invalid credentials code = %s, want Unauthenticated", status.Code(err))
	}
}

func TestIssueTokenMapsStorageFailureToInternal(t *testing.T) {
	var logs bytes.Buffer
	handler, _ := newTestHandlerWithLogger(t, errors.New("connection refused"), &logs)

	_, err := handler.IssueToken(context.Background(), &authpb.IssueTokenRequest{
		ClientId:     "media-client",
		ClientSecret: "secret",
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("storage failure code = %s, want Internal", status.Code(err))
	}
	if status.Convert(err).Message() != "auth storage failure" {
		t.Fatalf("storage failure message = %q", status.Convert(err).Message())
	}
	if !strings.Contains(logs.String(), "auth storage failure") || !strings.Contains(logs.String(), "connection refused") {
		t.Fatalf("swallowed storage error was not logged: %s", logs.String())
	}
}

func TestValidateTokenValidAndInvalidPaths(t *testing.T) {
	handler, _ := newTestHandler(t, nil)
	ctx := context.Background()

	issued, err := handler.IssueToken(ctx, &authpb.IssueTokenRequest{ClientId: "media-client", ClientSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}

	valid, err := handler.ValidateToken(ctx, &authpb.ValidateTokenRequest{AccessToken: issued.GetAccessToken()})
	if err != nil {
		t.Fatal(err)
	}
	if !valid.GetValid() {
		t.Fatalf("valid token rejected: %+v", valid)
	}
	if valid.GetClientId() != "media-client" || len(valid.GetScopes()) != 1 || valid.GetScopes()[0] != "hazard:read:summary" {
		t.Fatalf("unexpected validated token: %+v", valid)
	}
	if want := handlerNow.Add(time.Minute).Format(time.RFC3339Nano); valid.GetExpiresAt() != want {
		t.Fatalf("expiresAt = %q, want %q", valid.GetExpiresAt(), want)
	}

	invalid, err := handler.ValidateToken(ctx, &authpb.ValidateTokenRequest{AccessToken: "unknown-token"})
	if err != nil {
		t.Fatal(err)
	}
	if invalid.GetValid() || invalid.GetError() == "" {
		t.Fatalf("unknown token accepted: %+v", invalid)
	}
}

// newTestHandler builds an authHandler backed by a real TokenManager with an
// injected clock and deterministic token generator over a fake repository.
func newTestHandler(t *testing.T, persistErr error) (*authHandler, *fakeTokenRepository) {
	t.Helper()
	var logs bytes.Buffer
	return newTestHandlerWithLogger(t, persistErr, &logs)
}

func newTestHandlerWithLogger(t *testing.T, persistErr error, logs *bytes.Buffer) (*authHandler, *fakeTokenRepository) {
	t.Helper()
	repository := &fakeTokenRepository{
		persistErr: persistErr,
		clients: map[string]domain.Client{
			"media-client": {ID: "media-client", Secret: "secret", Scopes: []string{"hazard:read:summary"}},
		},
		access: make(map[string]domain.AccessToken),
	}
	tokens := []string{"access-token-1", "refresh-token-1", "access-token-2", "refresh-token-2"}
	next := 0
	manager, err := application.NewTokenManager(application.TokenManagerConfig{
		Repository:     repository,
		AccessTokenTTL: time.Minute,
		Now:            func() time.Time { return handlerNow },
		GenerateToken: func() (string, error) {
			value := tokens[next%len(tokens)]
			next++
			return value, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	return newAuthHandler(manager, logger), repository
}

type fakeTokenRepository struct {
	persistErr error
	clients    map[string]domain.Client
	access     map[string]domain.AccessToken
}

func (r *fakeTokenRepository) Ping(context.Context) error { return nil }

func (r *fakeTokenRepository) EnsureClients(_ context.Context, clients map[string]domain.Client) error {
	for clientID, client := range clients {
		client.ID = clientID
		r.clients[clientID] = client
	}
	return nil
}

func (r *fakeTokenRepository) AuthenticateClient(_ context.Context, clientID, secret string) (domain.Client, error) {
	client, ok := r.clients[clientID]
	if !ok || client.Secret != secret {
		return domain.Client{}, ports.ErrInvalidCredentials
	}
	return client, nil
}

func (r *fakeTokenRepository) PersistTokenPair(_ context.Context, access domain.AccessToken, refresh domain.RefreshToken) error {
	if r.persistErr != nil {
		return r.persistErr
	}
	r.access[string(access.Hash)] = access
	return nil
}

func (r *fakeTokenRepository) ValidateAccessToken(_ context.Context, tokenHash []byte, now time.Time) (domain.AccessToken, error) {
	token, ok := r.access[string(tokenHash)]
	if !ok || !token.ExpiresAt.After(now) {
		return domain.AccessToken{}, ports.ErrInvalidToken
	}
	return token, nil
}

func (r *fakeTokenRepository) RotateRefreshToken(context.Context, []byte, domain.AccessToken, domain.RefreshToken, time.Time) (domain.TokenOwner, error) {
	return domain.TokenOwner{}, ports.ErrInvalidToken
}
