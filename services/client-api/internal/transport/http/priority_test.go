package httptransport

import (
	"testing"

	"github.com/fathurwithyou/microservices/contracts/scopes"
	"github.com/fathurwithyou/microservices/services/client-api/internal/application"
)

func TestClassLimiterSeparatesPriorityClasses(t *testing.T) {
	limiter := newClassLimiter(5) // 4 field slots, 1 media slot
	media := application.Principal{Scopes: []string{scopes.HazardReadSummary}}
	field := application.Principal{Scopes: []string{scopes.HazardReadSummary, scopes.HazardReadRaw}}

	releaseMedia, ok := limiter.acquire(media)
	if !ok {
		t.Fatal("first media request must be admitted")
	}
	if _, ok := limiter.acquire(media); ok {
		t.Fatal("second media request must be rejected")
	}
	if _, ok := limiter.acquire(field); !ok {
		t.Fatal("field request must still be admitted")
	}
	releaseMedia()
	if _, ok := limiter.acquire(media); !ok {
		t.Fatal("media slot must be released")
	}
}
