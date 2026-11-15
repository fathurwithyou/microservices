package httptransport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/audit"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/failures"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type auditRepository struct{}

func (auditRepository) ListAuditEvents(context.Context, audit.Filter) ([]domain.HazardEvent, error) {
	return []domain.HazardEvent{{HazardID: "h-1", Source: domain.SourceSatellite, SourceRefID: "det-1", HazardType: domain.HazardWildfire, Severity: domain.SeveritySiaga, OccurredAt: time.Now().UTC(), IngestedAt: time.Now().UTC()}}, nil
}
func (auditRepository) SourceRefIDs(context.Context, domain.Source) ([]string, error) {
	return []string{"det-1"}, nil
}
func (auditRepository) HazardExists(context.Context, string) (bool, error) { return true, nil }

func (auditRepository) HazardLocations(context.Context, string) ([]audit.Location, error) {
	return []audit.Location{{Node: "node-b", Unit: "shard-2", Role: "primary"}}, nil
}

func (auditRepository) UnavailableUnits(context.Context) ([]string, error) { return nil, nil }
func (auditRepository) ListIngestFailures(context.Context, domain.Source, int, time.Time, string) ([]domain.IngestFailure, error) {
	return nil, nil
}

func auditTestServer(token string) *httptest.Server {
	server := NewServer("0", audit.NewQueries(auditRepository{}), token, "", nil)
	return httptest.NewServer(server.Handler)
}

type replayRepository struct{}

func (replayRepository) ListIngestFailures(context.Context, domain.Source, int, time.Time, string) ([]domain.IngestFailure, error) {
	return nil, nil
}
func (replayRepository) MarkIngestFailureRecovered(context.Context, string) error { return nil }

type replaySubmitter struct{}

func (replaySubmitter) Submit(context.Context, domain.HazardEvent, string) (string, error) {
	return "", nil
}

func replayTestServer(t *testing.T, auditToken, opsToken string) *httptest.Server {
	t.Helper()
	replayer, err := failures.New(failures.Config{Repository: replayRepository{}, Submitter: replaySubmitter{}})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer("0", audit.NewQueries(auditRepository{}), auditToken, opsToken, replayer)
	return httptest.NewServer(server.Handler)
}

func postReplay(t *testing.T, server *httptest.Server, authorization string) int {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/internal/ops/replay?source=PVMBG", nil)
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

func TestReplayRequiresOpsToken(t *testing.T) {
	server := replayTestServer(t, "audit-secret", "ops-secret")
	defer server.Close()

	cases := []struct {
		name          string
		authorization string
		want          int
	}{
		{"no token", "", http.StatusUnauthorized},
		{"audit token is rejected", "Bearer audit-secret", http.StatusUnauthorized},
		{"wrong token", "Bearer nope", http.StatusUnauthorized},
		{"ops token", "Bearer ops-secret", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := postReplay(t, server, tc.authorization); got != tc.want {
				t.Fatalf("status = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestReplayDisabledWithoutOpsToken(t *testing.T) {
	server := replayTestServer(t, "audit-secret", "")
	defer server.Close()
	// Even a valid audit token must not unlock replay when no ops token is set.
	if got := postReplay(t, server, "Bearer audit-secret"); got != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", got)
	}
}

func TestOpsTokenDoesNotUnlockAudit(t *testing.T) {
	server := replayTestServer(t, "audit-secret", "ops-secret")
	defer server.Close()
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/internal/audit/events", nil)
	request.Header.Set("Authorization", "Bearer ops-secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.StatusCode)
	}
}

func TestAuditRequiresToken(t *testing.T) {
	server := auditTestServer("secret")
	defer server.Close()
	response, err := http.Get(server.URL + "/internal/audit/events")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.StatusCode)
	}
}

func TestAuditEventsAndChecksum(t *testing.T) {
	server := auditTestServer("secret")
	defer server.Close()
	for _, path := range []string{"/internal/audit/events?source=SATELIT", "/internal/audit/checksum?source=SATELIT"} {
		request, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		request.Header.Set("Authorization", "Bearer secret")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d", path, response.StatusCode)
		}
		var payload map[string]any
		if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
			t.Fatalf("%s decode: %v", path, err)
		}
		response.Body.Close()
		if path == "/internal/audit/events?source=SATELIT" {
			if _, ok := payload["items"]; !ok {
				t.Fatalf("events payload = %#v", payload)
			}
		} else if payload["algorithm"] != "sha256" {
			t.Fatalf("checksum payload = %#v", payload)
		}
	}
}

func TestAuditChecksumRequiresSource(t *testing.T) {
	server := auditTestServer("secret")
	defer server.Close()

	request, _ := http.NewRequest(http.MethodGet, server.URL+"/internal/audit/checksum", nil)
	request.Header.Set("Authorization", "Bearer secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.StatusCode)
	}
}
