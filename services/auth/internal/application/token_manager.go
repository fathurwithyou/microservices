package application

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/services/auth/internal/domain"
	"github.com/fathurwithyou/microservices/services/auth/internal/ports"
)

var (
	ErrInvalidCredentials  = errors.New("invalid client credentials")
	ErrInvalidRefreshToken = errors.New("invalid refresh token")
	ErrInvalidAccessToken  = errors.New("invalid access token")
)

type TokenManager struct {
	repository ports.TokenRepository
	accessTTL  time.Duration
	now        func() time.Time
}

func NewTokenManager(accessTTL time.Duration, repository ports.TokenRepository) *TokenManager {
	return &TokenManager{repository: repository, accessTTL: accessTTL, now: time.Now}
}

func (m *TokenManager) SeedClients(ctx context.Context, clients map[string]domain.Client) error {
	return m.repository.EnsureClients(ctx, clients)
}

func (m *TokenManager) Issue(ctx context.Context, clientID, secret string) (string, string, time.Time, []string, error) {
	client, err := m.repository.AuthenticateClient(ctx, clientID, secret)
	if errors.Is(err, ports.ErrInvalidCredentials) {
		return "", "", time.Time{}, nil, ErrInvalidCredentials
	}
	if err != nil {
		return "", "", time.Time{}, nil, fmt.Errorf("authenticate client: %w", err)
	}

	access, refresh, accessValue, refreshValue, err := m.newTokenPair(client.ID, client.Scopes)
	if err != nil {
		return "", "", time.Time{}, nil, err
	}
	if err := m.repository.PersistTokenPair(ctx, access, refresh); err != nil {
		return "", "", time.Time{}, nil, fmt.Errorf("persist issued token pair: %w", err)
	}
	return accessValue, refreshValue, access.ExpiresAt, append([]string(nil), client.Scopes...), nil
}

func (m *TokenManager) Refresh(ctx context.Context, value string) (string, string, time.Time, string, []string, error) {
	access, refresh, accessValue, refreshValue, err := m.newTokenPair("", nil)
	if err != nil {
		return "", "", time.Time{}, "", nil, err
	}
	owner, err := m.repository.RotateRefreshToken(ctx, hash(value), access, refresh)
	if errors.Is(err, ports.ErrInvalidToken) {
		return "", "", time.Time{}, "", nil, ErrInvalidRefreshToken
	}
	if err != nil {
		return "", "", time.Time{}, "", nil, fmt.Errorf("rotate refresh token: %w", err)
	}
	return accessValue, refreshValue, access.ExpiresAt, owner.ClientID, append([]string(nil), owner.Scopes...), nil
}

func (m *TokenManager) Validate(ctx context.Context, value string) (string, []string, time.Time, error) {
	session, err := m.repository.ValidateAccessToken(ctx, hash(value), m.now())
	if errors.Is(err, ports.ErrInvalidToken) {
		return "", nil, time.Time{}, ErrInvalidAccessToken
	}
	if err != nil {
		return "", nil, time.Time{}, fmt.Errorf("validate access token: %w", err)
	}
	return session.ClientID, append([]string(nil), session.Scopes...), session.ExpiresAt, nil
}

func (m *TokenManager) Health(ctx context.Context) error {
	return m.repository.Ping(ctx)
}

func (m *TokenManager) newTokenPair(clientID string, scopes []string) (domain.AccessToken, domain.RefreshToken, string, string, error) {
	accessValue, err := randomToken()
	if err != nil {
		return domain.AccessToken{}, domain.RefreshToken{}, "", "", err
	}
	refreshValue, err := randomToken()
	if err != nil {
		return domain.AccessToken{}, domain.RefreshToken{}, "", "", err
	}
	now := m.now()
	return domain.AccessToken{
			Hash:      hash(accessValue),
			ClientID:  clientID,
			Scopes:    append([]string(nil), scopes...),
			ExpiresAt: now.Add(m.accessTTL),
		}, domain.RefreshToken{
			Hash:      hash(refreshValue),
			ClientID:  clientID,
			Scopes:    append([]string(nil), scopes...),
			ExpiresAt: nil,
		}, accessValue, refreshValue, nil
}

func randomToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func hash(value string) []byte {
	digest := sha256.Sum256([]byte(value))
	return digest[:]
}
