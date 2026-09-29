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

// tokenBytes is the entropy size of generated tokens. Tokens are
// hex-encoded, so their wire form is twice this length.
const tokenBytes = 32

var (
	// ErrInvalidRefreshToken reports a refresh token that is unknown,
	// expired, or already rotated. The gRPC handler needs it to
	// distinguish refresh failures from other invalid-token errors;
	// it always matches ports.ErrInvalidToken semantics.
	ErrInvalidRefreshToken = errors.New("invalid refresh token")
	// ErrInvalidAccessToken reports an access token that is unknown,
	// expired, or revoked. Kept for the same handler-side distinction.
	ErrInvalidAccessToken = errors.New("invalid access token")
)

// TokenPair is the plaintext result of issuing or refreshing tokens.
// ExpiresAt refers to the access token.
type TokenPair struct {
	AccessToken  string
	RefreshToken string
	ExpiresAt    time.Time
	ClientID     string
	Scopes       []string
}

// TokenManagerConfig holds the TokenManager dependencies. Now and
// GenerateToken are injectable seams for deterministic tests; both have
// production defaults.
type TokenManagerConfig struct {
	// Repository persists clients and token material. Required.
	Repository ports.TokenRepository
	// AccessTokenTTL is how long issued access tokens stay valid. Required.
	AccessTokenTTL time.Duration
	// Now returns the current time. Defaults to time.Now().UTC().
	Now func() time.Time
	// GenerateToken produces a random token value. Defaults to
	// crypto/rand-backed hex encoding of tokenBytes bytes.
	GenerateToken func() (string, error)
}

type TokenManager struct {
	repository    ports.TokenRepository
	accessTTL     time.Duration
	now           func() time.Time
	generateToken func() (string, error)
}

func NewTokenManager(cfg TokenManagerConfig) (*TokenManager, error) {
	if cfg.Repository == nil {
		return nil, errors.New("token manager requires a repository")
	}
	if cfg.AccessTokenTTL <= 0 {
		return nil, fmt.Errorf("token manager requires a positive access token TTL, got %s", cfg.AccessTokenTTL)
	}
	now := cfg.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	generateToken := cfg.GenerateToken
	if generateToken == nil {
		generateToken = randomToken
	}
	return &TokenManager{
		repository:    cfg.Repository,
		accessTTL:     cfg.AccessTokenTTL,
		now:           now,
		generateToken: generateToken,
	}, nil
}

func (m *TokenManager) SeedClients(ctx context.Context, clients map[string]domain.Client) error {
	return m.repository.EnsureClients(ctx, clients)
}

func (m *TokenManager) Issue(ctx context.Context, clientID, secret string) (TokenPair, error) {
	client, err := m.repository.AuthenticateClient(ctx, clientID, secret)
	if errors.Is(err, ports.ErrInvalidCredentials) {
		return TokenPair{}, ports.ErrInvalidCredentials
	}
	if err != nil {
		return TokenPair{}, fmt.Errorf("authenticate client: %w", err)
	}

	access, refresh, pair, err := m.newTokenPair(client.ID, client.Scopes)
	if err != nil {
		return TokenPair{}, err
	}
	if err := m.repository.PersistTokenPair(ctx, access, refresh); err != nil {
		return TokenPair{}, fmt.Errorf("persist issued token pair: %w", err)
	}
	return pair, nil
}

func (m *TokenManager) Refresh(ctx context.Context, refreshToken string) (TokenPair, error) {
	access, refresh, pair, err := m.newTokenPair("", nil)
	if err != nil {
		return TokenPair{}, err
	}
	owner, err := m.repository.RotateRefreshToken(ctx, hash(refreshToken), access, refresh, m.now())
	if errors.Is(err, ports.ErrInvalidToken) {
		return TokenPair{}, ErrInvalidRefreshToken
	}
	if err != nil {
		return TokenPair{}, fmt.Errorf("rotate refresh token: %w", err)
	}
	pair.ClientID = owner.ClientID
	pair.Scopes = append([]string(nil), owner.Scopes...)
	return pair, nil
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

// newTokenPair builds the hashed domain tokens plus the plaintext TokenPair
// the caller receives. The returned pair's ClientID and Scopes reflect the
// arguments; Refresh overwrites them from the stored owner after rotation.
func (m *TokenManager) newTokenPair(clientID string, scopes []string) (domain.AccessToken, domain.RefreshToken, TokenPair, error) {
	accessValue, err := m.generateToken()
	if err != nil {
		return domain.AccessToken{}, domain.RefreshToken{}, TokenPair{}, fmt.Errorf("generate access token: %w", err)
	}
	refreshValue, err := m.generateToken()
	if err != nil {
		return domain.AccessToken{}, domain.RefreshToken{}, TokenPair{}, fmt.Errorf("generate refresh token: %w", err)
	}
	expiresAt := m.now().Add(m.accessTTL)
	access := domain.AccessToken{
		Hash:      hash(accessValue),
		ClientID:  clientID,
		Scopes:    append([]string(nil), scopes...),
		ExpiresAt: expiresAt,
	}
	refresh := domain.RefreshToken{
		Hash:      hash(refreshValue),
		ClientID:  clientID,
		Scopes:    append([]string(nil), scopes...),
		ExpiresAt: nil,
	}
	pair := TokenPair{
		AccessToken:  accessValue,
		RefreshToken: refreshValue,
		ExpiresAt:    expiresAt,
		ClientID:     clientID,
		Scopes:       append([]string(nil), scopes...),
	}
	return access, refresh, pair, nil
}

func randomToken() (string, error) {
	bytes := make([]byte, tokenBytes)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func hash(value string) []byte {
	digest := sha256.Sum256([]byte(value))
	return digest[:]
}
