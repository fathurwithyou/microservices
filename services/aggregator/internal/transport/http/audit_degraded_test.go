package httptransport

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/audit"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

var errStoreDown = errors.New("remote wall time is too far ahead (4m59s) to be trustworthy (SQLSTATE XXUUU)")

// outageRepository is auditRepository with a store that can stop answering:
// down fails every data query, healthDown fails the completeness lookup (on
// the real store that lookup keeps its last answer, so the two differ).
type outageRepository struct {
	auditRepository
	down       atomic.Bool
	healthDown atomic.Bool
}

func (r *outageRepository) ListAuditEvents(ctx context.Context, filter audit.Filter) ([]domain.HazardEvent, error) {
	if r.down.Load() {
		return nil, errStoreDown
	}
	return r.auditRepository.ListAuditEvents(ctx, filter)
}

func (r *outageRepository) SourceRefIDs(ctx context.Context, source domain.Source) ([]string, error) {
	if r.down.Load() {
		return nil, errStoreDown
	}
	return r.auditRepository.SourceRefIDs(ctx, source)
}

func (r *outageRepository) HazardExists(ctx context.Context, id string) (bool, error) {
	if r.down.Load() {
		return false, errStoreDown
	}
	return r.auditRepository.HazardExists(ctx, id)
}

func (r *outageRepository) HazardDistribution(ctx context.Context) (audit.Distribution, error) {
	if r.down.Load() {
		return audit.Distribution{}, errStoreDown
	}
	return r.auditRepository.HazardDistribution(ctx)
}

func (r *outageRepository) ListIngestFailures(ctx context.Context, filter domain.FailureFilter) ([]domain.IngestFailure, error) {
	if r.down.Load() {
		return nil, errStoreDown
	}
	return r.auditRepository.ListIngestFailures(ctx, filter)
}

func (r *outageRepository) UnavailableUnits(context.Context) ([]string, error) {
	if r.healthDown.Load() {
		return nil, errStoreDown
	}
	return nil, nil
}

type auditAnswer struct {
	Checksum         string   `json:"checksum"`
	Count            int      `json:"count"`
	Ranges           int      `json:"ranges"`
	AsOf             string   `json:"as_of"`
	Partial          bool     `json:"partial"`
	UnavailableUnits []string `json:"unavailable_units"`
	Error            struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func auditGet(t *testing.T, server *httptest.Server, path string) (int, auditAnswer) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
	request.Header.Set("Authorization", "Bearer secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var answer auditAnswer
	if err := json.NewDecoder(response.Body).Decode(&answer); err != nil {
		t.Fatalf("%s: body is not JSON: %v", path, err)
	}
	return response.StatusCode, answer
}

func outageServer(t *testing.T) (*httptest.Server, *outageRepository) {
	t.Helper()
	repository := &outageRepository{}
	server := httptest.NewServer(NewServer("0", audit.NewQueries(repository), "secret", "", nil).Handler)
	t.Cleanup(server.Close)
	return server, repository
}

// With nothing answered before there is nothing to fall back on: the answer
// is 503 with the standard error body, never 500, and a caller's own mistake
// stays 400.
func TestAuditAnswersUnavailableWhenTheStoreIsDownAndNothingWasAnsweredBefore(t *testing.T) {
	server, repository := outageServer(t)
	repository.down.Store(true)
	for _, path := range []string{
		"/internal/audit/checksum?source=SATELIT",
		"/internal/audit/distribution",
		"/internal/audit/events?source=SATELIT",
		"/internal/audit/location?hazard_id=h-1",
		"/internal/audit/unprocessed",
	} {
		status, answer := auditGet(t, server, path)
		if status != http.StatusServiceUnavailable || answer.Error.Code != "unavailable" || answer.Error.Message == "" {
			t.Errorf("%s: status = %d error = %+v, want 503 with code unavailable", path, status, answer.Error)
		}
	}
	for _, path := range []string{"/internal/audit/events?cursor=not-a-cursor", "/internal/audit/unprocessed?cursor=not-a-cursor"} {
		if status, answer := auditGet(t, server, path); status != http.StatusBadRequest || answer.Error.Code != "bad_request" {
			t.Errorf("%s: status = %d error = %+v, want 400 bad_request", path, status, answer.Error)
		}
	}
}

// The 18 Nov outage seen through the Audit API: checksum and distribution
// were answered a moment ago, then the store refuses every statement. Both
// keep answering 200 with the last figures, partial: true, a non-empty
// unavailable_units and the as_of of the answer they are repeating.
func TestAuditChecksumAndDistributionStayAnsweredWhileTheStoreIsDown(t *testing.T) {
	t.Parallel()
	server, repository := outageServer(t)
	paths := []string{"/internal/audit/checksum?source=SATELIT", "/internal/audit/distribution"}
	before := map[string]auditAnswer{}
	for _, path := range paths {
		status, answer := auditGet(t, server, path)
		if status != http.StatusOK || answer.Partial || len(answer.UnavailableUnits) != 0 {
			t.Fatalf("%s before the outage: status = %d answer = %+v", path, status, answer)
		}
		before[path] = answer
	}
	repository.down.Store(true)
	// Both caches reuse an answer for 2 s without asking the store; the
	// outage is first noticed by the refresh after that.
	time.Sleep(2100 * time.Millisecond)
	for _, path := range paths {
		var answer auditAnswer
		for wait := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			var status int
			status, answer = auditGet(t, server, path)
			if status != http.StatusOK {
				t.Fatalf("%s during the outage: status = %d error = %+v, want 200", path, status, answer.Error)
			}
			if answer.Partial {
				break
			}
			if time.Now().After(wait) {
				t.Fatalf("%s: the answer was never marked partial during the outage", path)
			}
		}
		if len(answer.UnavailableUnits) != 1 || answer.UnavailableUnits[0] != "store" {
			t.Errorf("%s: unavailable_units = %v, want [store]", path, answer.UnavailableUnits)
		}
		if answer.Checksum != before[path].Checksum || answer.Count != before[path].Count || answer.Ranges != before[path].Ranges {
			t.Errorf("%s: answer = %+v, want the figures given before the outage %+v", path, answer, before[path])
		}
		// as_of is the time the repeated answer was read, not now: within
		// the second it was first given, two seconds or more ago.
		given, _ := time.Parse(time.RFC3339, before[path].AsOf)
		asOf, err := time.Parse(time.RFC3339, answer.AsOf)
		if err != nil || asOf.After(given) || given.Sub(asOf) > time.Second {
			t.Errorf("%s: as_of = %s, want the time the repeated answer was read (%s)", path, answer.AsOf, before[path].AsOf)
		}
	}
}

// The completeness lookup is a store query too. Its failure must not hide
// behind partial: false (as it did while the error was ignored), and must
// not fail a request whose data is there.
func TestAuditMarksAnswersPartialWhenCompletenessIsUnknown(t *testing.T) {
	server, repository := outageServer(t)
	repository.healthDown.Store(true)
	for _, path := range []string{"/internal/audit/checksum?source=SATELIT", "/internal/audit/distribution", "/internal/audit/events?source=SATELIT"} {
		status, answer := auditGet(t, server, path)
		if status != http.StatusOK || !answer.Partial || len(answer.UnavailableUnits) != 1 || answer.UnavailableUnits[0] != "store" {
			t.Errorf("%s: status = %d partial = %v units = %v, want 200 partial with [store]", path, status, answer.Partial, answer.UnavailableUnits)
		}
	}
}
