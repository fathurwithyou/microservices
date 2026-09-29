package grpctransport

import (
	"context"
	"errors"
	"log/slog"
	"time"

	authpb "github.com/fathurwithyou/microservices/contracts/gen/go/auth"
	"github.com/fathurwithyou/microservices/services/auth/internal/application"
	"github.com/fathurwithyou/microservices/services/auth/internal/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	storageFailureMessage     = "auth storage failure"
	storageUnavailableMessage = "auth storage unavailable"
)

// TokenService is the narrow slice of the application layer the gRPC
// handlers depend on.
type TokenService interface {
	Issue(ctx context.Context, clientID, secret string) (application.TokenPair, error)
	Refresh(ctx context.Context, refreshToken string) (application.TokenPair, error)
	Validate(ctx context.Context, accessToken string) (string, []string, time.Time, error)
	Health(ctx context.Context) error
}

type authHandler struct {
	authpb.UnimplementedAuthServer
	tokens TokenService
	logger *slog.Logger
}

func newAuthHandler(tokens TokenService, logger *slog.Logger) *authHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &authHandler{tokens: tokens, logger: logger}
}

func (h *authHandler) IssueToken(ctx context.Context, request *authpb.IssueTokenRequest) (*authpb.TokenPair, error) {
	pair, err := h.tokens.Issue(ctx, request.GetClientId(), request.GetClientSecret())
	if err != nil {
		return nil, h.tokenError(ctx, "IssueToken", err)
	}
	return toProtoTokenPair(pair), nil
}

func (h *authHandler) RefreshToken(ctx context.Context, request *authpb.RefreshTokenRequest) (*authpb.TokenPair, error) {
	pair, err := h.tokens.Refresh(ctx, request.GetRefreshToken())
	if err != nil {
		return nil, h.tokenError(ctx, "RefreshToken", err)
	}
	return toProtoTokenPair(pair), nil
}

func (h *authHandler) ValidateToken(ctx context.Context, request *authpb.ValidateTokenRequest) (*authpb.ValidatedToken, error) {
	clientID, scopes, expiresAt, err := h.tokens.Validate(ctx, request.GetAccessToken())
	if err != nil {
		if !errors.Is(err, application.ErrInvalidAccessToken) {
			return nil, h.internalError(ctx, "ValidateToken", err, storageFailureMessage)
		}
		return &authpb.ValidatedToken{Valid: false, Error: err.Error()}, nil
	}
	return &authpb.ValidatedToken{Valid: true, ClientId: clientID, Scopes: scopes, ExpiresAt: expiresAt.Format(time.RFC3339Nano)}, nil
}

func (h *authHandler) HealthCheck(ctx context.Context, _ *authpb.HealthCheckRequest) (*authpb.HealthCheckResponse, error) {
	if err := h.tokens.Health(ctx); err != nil {
		h.logSwallowed(ctx, "HealthCheck", err, storageUnavailableMessage)
		return nil, status.Error(codes.Unavailable, storageUnavailableMessage)
	}
	return &authpb.HealthCheckResponse{Status: "ok"}, nil
}

// tokenError maps application errors onto gRPC status codes. Credential and
// refresh-token failures are authentication problems; anything else is a
// storage failure and must not leak internals to the caller.
func (h *authHandler) tokenError(ctx context.Context, operation string, err error) error {
	switch {
	case errors.Is(err, ports.ErrInvalidCredentials):
		return status.Error(codes.Unauthenticated, "invalid client credentials")
	case errors.Is(err, application.ErrInvalidRefreshToken):
		return status.Error(codes.Unauthenticated, err.Error())
	default:
		return h.internalError(ctx, operation, err, storageFailureMessage)
	}
}

// internalError logs the real cause and returns an opaque Internal status.
func (h *authHandler) internalError(ctx context.Context, operation string, err error, message string) error {
	h.logSwallowed(ctx, operation, err, message)
	return status.Error(codes.Internal, message)
}

func (h *authHandler) logSwallowed(ctx context.Context, operation string, err error, message string) {
	h.logger.ErrorContext(ctx, message,
		"service", "auth",
		"component", "grpc-handler",
		"operation", operation,
		"error", err,
	)
}

func toProtoTokenPair(pair application.TokenPair) *authpb.TokenPair {
	return &authpb.TokenPair{
		AccessToken:  pair.AccessToken,
		RefreshToken: pair.RefreshToken,
		ExpiresAt:    pair.ExpiresAt.Format(time.RFC3339Nano),
		ClientId:     pair.ClientID,
		Scopes:       pair.Scopes,
	}
}
