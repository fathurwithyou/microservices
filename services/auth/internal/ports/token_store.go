package ports

import (
	"context"
	"errors"
	"time"

	"github.com/fathurwithyou/microservices/services/auth/internal/domain"
)

var (
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrInvalidToken       = errors.New("invalid token")
)

type TokenStore interface {
	Ping(context.Context) error
	EnsureClients(context.Context, map[string]domain.Client) error
	AuthenticateClient(context.Context, string, string) (domain.Client, error)
	PersistTokenPair(context.Context, domain.AccessToken, domain.RefreshToken) error
	ValidateAccessToken(context.Context, []byte, time.Time) (domain.AccessToken, error)
	RotateRefreshToken(context.Context, []byte, domain.AccessToken, domain.RefreshToken) (domain.TokenOwner, error)
}
