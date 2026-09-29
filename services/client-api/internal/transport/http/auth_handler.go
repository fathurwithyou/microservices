package httptransport

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/fathurwithyou/microservices/services/client-api/internal/application"
	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
	"github.com/gin-gonic/gin"
)

type tokenRequest struct {
	ClientID     string `json:"client_id" binding:"required"`
	ClientSecret string `json:"client_secret" binding:"required"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

func (h handler) issueToken(c *gin.Context) {
	request := tokenRequest{}
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "client_id and client_secret are required"})
		return
	}
	ctx, cancel := h.contextWithCorrelation(c)
	defer cancel()
	pair, err := h.api.IssueToken(ctx, request.ClientID, request.ClientSecret)
	if err != nil {
		writeAPIError(c, err)
		return
	}
	c.JSON(http.StatusOK, pair)
}

func (h handler) refreshToken(c *gin.Context) {
	request := refreshRequest{}
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "refresh_token is required"})
		return
	}
	ctx, cancel := h.contextWithCorrelation(c)
	defer cancel()
	pair, err := h.api.RefreshToken(ctx, request.RefreshToken)
	if err != nil {
		writeAPIError(c, err)
		return
	}
	c.JSON(http.StatusOK, pair)
}

func (h handler) authenticate(c *gin.Context) {
	accessToken, ok := bearerToken(c.GetHeader("Authorization"))
	if !ok {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "bearer token required"})
		return
	}
	ctx, cancel := h.contextWithCorrelation(c)
	defer cancel()
	principal, err := h.api.Authenticate(ctx, accessToken)
	if err != nil {
		status, message := apiErrorStatus(err)
		c.AbortWithStatusJSON(status, gin.H{"error": message})
		return
	}
	c.Set(principalKey, principal)
	c.Next()
}

// requestPrincipal returns the Principal stored by the authenticate
// middleware. It is only valid on protected routes.
func requestPrincipal(c *gin.Context) (application.Principal, bool) {
	value, ok := c.Get(principalKey)
	if !ok {
		return application.Principal{}, false
	}
	principal, ok := value.(application.Principal)
	return principal, ok
}

func bearerToken(header string) (string, bool) {
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok || token == "" {
		return "", false
	}
	return token, true
}

// apiErrorStatus maps application and ports errors to HTTP status codes.
// Upstream outages are never reported as client credential problems: an
// unreachable auth service yields 503/502, not 401.
func apiErrorStatus(err error) (int, string) {
	switch {
	case errors.Is(err, ports.ErrInvalidCredentials):
		return http.StatusUnauthorized, "invalid client credentials"
	case errors.Is(err, ports.ErrInvalidToken):
		return http.StatusUnauthorized, "invalid or expired access token"
	case errors.Is(err, application.ErrForbidden):
		return http.StatusForbidden, "required scope is missing"
	case errors.Is(err, ports.ErrNotFound):
		return http.StatusNotFound, "hazard not found"
	case errors.Is(err, ports.ErrUnavailable):
		return http.StatusServiceUnavailable, "upstream unavailable"
	default:
		return http.StatusBadGateway, "upstream request failed"
	}
}

func writeAPIError(c *gin.Context, err error) {
	status, message := apiErrorStatus(err)
	c.JSON(status, gin.H{"error": message})
}

func (h handler) contextWithCorrelation(c *gin.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := h.requestContext(c)
	return correlation.WithContext(ctx, c.GetString(correlation.GinKey)), cancel
}
