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
func (auditRepository) ListIngestFailures(context.Context, domain.FailureFilter) ([]domain.IngestFailure, error) {
	return []domain.IngestFailure{{MessageID: "msg-1", Source: domain.SourcePVMBG, Reason: "schema_incompatible", Status: domain.FailureParked, PayloadBytes: 16}}, nil
}
func (auditRepository) GetIngestFailure(_ context.Context, id string) (domain.IngestFailure, error) {
	if id != "msg-1" {
		return domain.IngestFailure{}, domain.ErrNotFound
	}
	return domain.IngestFailure{MessageID: "msg-1", Source: domain.SourcePVMBG, Status: domain.FailureParked, Payload: []byte("bad\xff"), PayloadBytes: 4}, nil
}

func auditTestServer(token string) *httptest.Server {
	server := NewServer("0", audit.NewQueries(auditRepository{}), token, "", nil)
	return httptest.NewServer(server.Handler)
}

type replayRepository struct{}

func (replayRepository) ListIngestFailures(context.Context, domain.FailureFilter) ([]domain.IngestFailure, error) {
	return nil, nil
}
func (replayRepository) GetIngestFailure(context.Context, string) (domain.IngestFailure, error) {
	return domain.IngestFailure{}, domain.ErrNotFound
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

func TestOpsFailureInspectionRequiresOpsTokenAndReturnsPayload(t *testing.T) {
	server := replayTestServer(t, "audit-secret", "ops-secret")
	defer server.Close()
	get := func(path, authorization string) (int, map[string]any) {
		request, _ := http.NewRequest(http.MethodGet, server.URL+path, nil)
		if authorization != "" {
			request.Header.Set("Authorization", authorization)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body := map[string]any{}
		_ = json.NewDecoder(response.Body).Decode(&body)
		return response.StatusCode, body
	}
	if status, _ := get("/internal/ops/failures", "Bearer audit-secret"); status != http.StatusUnauthorized {
		t.Fatalf("audit token status = %d, want 401", status)
	}
	status, body := get("/internal/ops/failures", "Bearer ops-secret")
	if status != http.StatusOK {
		t.Fatalf("ops list status = %d", status)
	}
	items, _ := body["items"].([]any)
	// Listings carry the size only; bytes come from the single-message endpoint.
	if len(items) != 1 || items[0].(map[string]any)["payload_bytes"] != float64(16) {
		t.Fatalf("ops list body = %v", body)
	}
	if _, listed := items[0].(map[string]any)["payload"]; listed {
		t.Fatal("ops list included payload bytes")
	}
	status, body = get("/internal/ops/failures/msg-1", "Bearer ops-secret")
	if status != http.StatusOK || body["payload_base64"] != "YmFk/w==" || body["payload_bytes"] != float64(4) {
		t.Fatalf("ops get status=%d body=%v", status, body)
	}
	if status, _ := get("/internal/ops/failures/nope", "Bearer ops-secret"); status != http.StatusNotFound {
		t.Fatalf("missing failure status = %d, want 404", status)
	}
	// The audit listing must never leak raw payloads.
	_, body = get("/internal/audit/unprocessed", "Bearer audit-secret")
	auditItems, _ := body["items"].([]any)
	if len(auditItems) != 1 {
		t.Fatalf("audit unprocessed = %v", body)
	}
	if _, leaked := auditItems[0].(map[string]any)["payload"]; leaked {
		t.Fatal("audit unprocessed exposed payload")
	}
}
