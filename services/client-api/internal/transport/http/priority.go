package httptransport

import (
	"net/http"

	"github.com/fathurwithyou/microservices/contracts/scopes"
	"github.com/fathurwithyou/microservices/services/client-api/internal/application"
	"github.com/gin-gonic/gin"
)

// classLimiter is the admission control for authenticated requests. Every
// request takes a slot of the shared capacity; Media requests also need one
// of the Media slots, at most a fifth of the capacity. Field Team requests can
// therefore always use at least four fifths, and all of it when Media is
// quiet, while a Media flood is rejected with 429 before it can crowd them
// out (M2 P5.5).
type classLimiter struct {
	total chan struct{}
	media chan struct{}
}

func newClassLimiter(total int) *classLimiter {
	if total < 2 {
		total = 2
	}
	mediaCapacity := total / 5
	if mediaCapacity < 1 {
		mediaCapacity = 1
	}
	return &classLimiter{total: make(chan struct{}, total), media: make(chan struct{}, mediaCapacity)}
}

func (l *classLimiter) acquire(principal application.Principal) (func(), bool) {
	field := hasAnyScope(principal, scopes.FieldReportsWrite, scopes.HazardReadRaw)
	if !field {
		select {
		case l.media <- struct{}{}:
		default:
			return nil, false
		}
	}
	select {
	case l.total <- struct{}{}:
	default:
		if !field {
			<-l.media
		}
		return nil, false
	}
	return func() {
		<-l.total
		if !field {
			<-l.media
		}
	}, true
}

func (h handler) priorityLimit(c *gin.Context) {
	principal, ok := requestPrincipal(c)
	if !ok {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}
	if hasAnyScope(principal, scopes.FieldReportsWrite, scopes.HazardReadRaw) {
		// Lets the overload gate recognise this credential while it is valid.
		h.priority.mark(c.GetHeader("Authorization"))
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
