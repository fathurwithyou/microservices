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

func TestFieldTeamUsesIdleCapacityAndIsNeverCrowdedOutByMedia(t *testing.T) {
	limiter := newClassLimiter(10) // 10 shared slots, at most 2 for Media
	media := application.Principal{Scopes: []string{scopes.HazardReadSummary}}
	field := application.Principal{Scopes: []string{scopes.HazardReadSummary, scopes.HazardReadRaw}}

	// A Media flood gets at most a fifth of the capacity.
	admitted := 0
	for i := 0; i < 50; i++ {
		if _, ok := limiter.acquire(media); ok {
			admitted++
		}
	}
	if admitted != 2 {
		t.Fatalf("media admitted %d, want 2", admitted)
	}
	// Field Team still gets every remaining slot.
	for i := 0; i < 8; i++ {
		if _, ok := limiter.acquire(field); !ok {
			t.Fatalf("field request %d rejected while capacity was free", i+1)
		}
	}
	if _, ok := limiter.acquire(field); ok {
		t.Fatal("field request admitted beyond total capacity")
	}
}

func TestFieldTeamMayUseAllCapacityWhenMediaIsQuiet(t *testing.T) {
	limiter := newClassLimiter(10)
	field := application.Principal{Scopes: []string{scopes.FieldReportsWrite}}
	for i := 0; i < 10; i++ {
		if _, ok := limiter.acquire(field); !ok {
			t.Fatalf("field request %d rejected with idle capacity", i+1)
		}
	}
	media := application.Principal{Scopes: []string{scopes.HazardReadSummary}}
	if _, ok := limiter.acquire(media); ok {
		t.Fatal("media admitted while the shared capacity is full")
	}
	// The rejected Media request must not leak its Media slot.
	if got := len(limiter.media); got != 0 {
		t.Fatalf("media slots held = %d, want 0", got)
	}
}
