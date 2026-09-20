package grpctransport

import (
	"context"
	"errors"

	authpb "github.com/fathurwithyou/microservices/contracts/gen/go/auth"
	"github.com/fathurwithyou/microservices/services/auth/internal/application"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const timeFormat = "2006-01-02T15:04:05.999999999Z07:00"

type authHandler struct {
	authpb.UnimplementedAuthServer
	manager *application.TokenManager
}

func newAuthHandler(manager *application.TokenManager) *authHandler {
	return &authHandler{manager: manager}
}

func (h *authHandler) IssueToken(ctx context.Context, request *authpb.IssueTokenRequest) (*authpb.TokenPair, error) {
	access, refresh, expiresAt, scopes, err := h.manager.Issue(ctx, request.GetClientId(), request.GetClientSecret())
	if err != nil {
		return nil, tokenError(err)
	}
	return tokenPair(access, refresh, expiresAt.Format(timeFormat), request.GetClientId(), scopes), nil
}

func (h *authHandler) RefreshToken(ctx context.Context, request *authpb.RefreshTokenRequest) (*authpb.TokenPair, error) {
	access, refresh, expiresAt, clientID, scopes, err := h.manager.Refresh(ctx, request.GetRefreshToken())
	if err != nil {
		return nil, tokenError(err)
	}
	return tokenPair(access, refresh, expiresAt.Format(timeFormat), clientID, scopes), nil
}

func (h *authHandler) ValidateToken(ctx context.Context, request *authpb.ValidateTokenRequest) (*authpb.ValidatedToken, error) {
	clientID, scopes, expiresAt, err := h.manager.Validate(ctx, request.GetAccessToken())
	if err != nil {
		if !errors.Is(err, application.ErrInvalidAccessToken) {
			return nil, status.Error(codes.Internal, "auth storage failure")
		}
		return &authpb.ValidatedToken{Valid: false, Error: err.Error()}, nil
	}
	return &authpb.ValidatedToken{Valid: true, ClientId: clientID, Scopes: scopes, ExpiresAt: expiresAt.Format(timeFormat)}, nil
}

func tokenError(err error) error {
	if errors.Is(err, application.ErrInvalidCredentials) || errors.Is(err, application.ErrInvalidRefreshToken) {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	return status.Error(codes.Internal, "auth storage failure")
}

func (h *authHandler) HealthCheck(ctx context.Context, _ *authpb.HealthCheckRequest) (*authpb.HealthCheckResponse, error) {
	if err := h.manager.Health(ctx); err != nil {
		return nil, status.Error(codes.Unavailable, "auth storage unavailable")
	}
	return &authpb.HealthCheckResponse{Status: "ok"}, nil
}

func tokenPair(access, refresh, expiresAt, clientID string, scopes []string) *authpb.TokenPair {
	return &authpb.TokenPair{AccessToken: access, RefreshToken: refresh, ExpiresAt: expiresAt, ClientId: clientID, Scopes: scopes}
}
