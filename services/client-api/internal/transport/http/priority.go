package httptransport

import (
	"net/http"

	"github.com/fathurwithyou/microservices/contracts/scopes"
	"github.com/fathurwithyou/microservices/services/client-api/internal/application"
	"github.com/gin-gonic/gin"
)

type classLimiter struct {
	field chan struct{}
	media chan struct{}
}

func newClassLimiter(total int) *classLimiter {
	if total < 2 {
		total = 2
	}
	fieldCapacity := total * 4 / 5
	if fieldCapacity < 1 {
		fieldCapacity = 1
	}
	mediaCapacity := total - fieldCapacity
	if mediaCapacity < 1 {
		mediaCapacity = 1
	}
	return &classLimiter{field: make(chan struct{}, fieldCapacity), media: make(chan struct{}, mediaCapacity)}
}

func (l *classLimiter) acquire(principal application.Principal) (func(), bool) {
	capacity := l.media
	if hasAnyScope(principal, scopes.FieldReportsWrite, scopes.HazardReadRaw) {
		capacity = l.field
	}
	select {
	case capacity <- struct{}{}:
		return func() { <-capacity }, true
	default:
		return nil, false
	}
}

func (h handler) priorityLimit(c *gin.Context) {
	principal, ok := requestPrincipal(c)
	if !ok {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	release, allowed := h.limiter.acquire(principal)
	if !allowed {
		c.Header("Retry-After", "1")
		c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "request capacity exceeded"})
		return
	}
	defer release()
	c.Next()
}

func hasAnyScope(principal application.Principal, required ...string) bool {
	for _, scope := range principal.Scopes {
		for _, want := range required {
			if scope == want {
				return true
			}
		}
	}
	return false
}
