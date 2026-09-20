package httptransport

import (
	"net/http"
	"strings"

	grpcadapter "github.com/fathurwithyou/microservices/services/client-api/internal/adapters/grpc"
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
	ctx, cancel := h.requestContext(c)
	defer cancel()
	pair, err := h.api.IssueToken(grpcadapter.WithCorrelationID(ctx, c.GetString("correlation_id")), request.ClientID, request.ClientSecret)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid client credentials"})
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
	ctx, cancel := h.requestContext(c)
	defer cancel()
	pair, err := h.api.RefreshToken(grpcadapter.WithCorrelationID(ctx, c.GetString("correlation_id")), request.RefreshToken)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid refresh token"})
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
	ctx, cancel := h.requestContext(c)
	defer cancel()
	validated, err := h.api.ValidateToken(grpcadapter.WithCorrelationID(ctx, c.GetString("correlation_id")), accessToken)
	if err != nil || !validated.GetValid() {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired access token"})
		return
	}
	c.Set("scopes", validated.GetScopes())
	c.Next()
}

func bearerToken(header string) (string, bool) {
	if !strings.HasPrefix(header, "Bearer ") {
		return "", false
	}
	return strings.TrimPrefix(header, "Bearer "), true
}
