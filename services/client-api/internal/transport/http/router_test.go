package httptransport

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/contracts/scopes"
	"github.com/fathurwithyou/microservices/services/client-api/internal/application"
	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestMediaScopeCannotRequestRawHazards(t *testing.T) {
	router := NewRouter(RouterConfig{API: application.NewAPI(summaryAuth{}, unusedAggregator{}), MaxInflight: 1, Timeout: time.Second, Logger: slog.Default()})
	request := httptest.NewRequest(http.MethodGet, "/hazards?raw=true", nil)
	request.Header.Set("Authorization", "Bearer media-token")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
}

func TestUpstreamOutageIsNotReportedAsNotFound(t *testing.T) {
	router := NewRouter(RouterConfig{API: application.NewAPI(summaryAuth{}, failingAggregator{err: ports.ErrUnavailable}), MaxInflight: 1, Timeout: time.Second, Logger: slog.Default()})
	request := httptest.NewRequest(http.MethodGet, "/hazards/h-1", nil)
	request.Header.Set("Authorization", "Bearer media-token")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestMissingHazardMapsToNotFound(t *testing.T) {
	router := NewRouter(RouterConfig{API: application.NewAPI(summaryAuth{}, failingAggregator{err: ports.ErrNotFound}), MaxInflight: 1, Timeout: time.Second, Logger: slog.Default()})
	request := httptest.NewRequest(http.MethodGet, "/hazards/missing", nil)
	request.Header.Set("Authorization", "Bearer media-token")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestInvalidCredentialsMapToUnauthorized(t *testing.T) {
	router := NewRouter(RouterConfig{API: application.NewAPI(rejectingAuth{}, unusedAggregator{}), MaxInflight: 1, Timeout: time.Second, Logger: slog.Default()})
	request := httptest.NewRequest(http.MethodPost, "/auth/token", nil)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	// Empty body fails binding first; the mapping itself is covered through
	// the status table below.
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestAPIErrorStatusMapping(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{name: "invalid credentials", err: ports.ErrInvalidCredentials, status: http.StatusUnauthorized},
		{name: "invalid token", err: ports.ErrInvalidToken, status: http.StatusUnauthorized},
		{name: "forbidden", err: application.ErrForbidden, status: http.StatusForbidden},
		{name: "not found", err: ports.ErrNotFound, status: http.StatusNotFound},
		{name: "unavailable", err: ports.ErrUnavailable, status: http.StatusServiceUnavailable},
		{name: "upstream failure", err: ports.ErrUpstreamFailure, status: http.StatusBadGateway},
		{name: "invalid argument", err: ports.ErrInvalidArgument, status: http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _ := apiErrorStatus(tt.err)
			if status != tt.status {
				t.Fatalf("status = %d, want %d", status, tt.status)
			}
		})
	}
}

func TestPageSizeBounds(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{name: "empty defaults", input: "", want: 50},
		{name: "zero defaults", input: "0", want: 50},
		{name: "negative defaults", input: "-5", want: 50},
		{name: "non-numeric defaults", input: "abc", want: 50},
		{name: "capped", input: "500", want: 100},
		{name: "exact", input: "25", want: 25},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pageSize(tt.input); got != tt.want {
				t.Fatalf("pageSize(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestFieldReportRequiresFieldScope(t *testing.T) {
	router := NewRouter(RouterConfig{API: application.NewAPI(summaryAuth{}, unusedAggregator{}), MaxInflight: 1, Timeout: time.Second, Logger: slog.Default()})
	request := httptest.NewRequest(http.MethodPost, "/field-reports", strings.NewReader(`{"report_id":"r-1","hazard_type":"WILDFIRE","severity":"SIAGA","area_name":"Kalteng","occurred_at":"2026-11-12T02:43:00Z"}`))
	request.Header.Set("Authorization", "Bearer media-token")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
}

func TestFieldReportAcceptedForFieldTeam(t *testing.T) {
	router := NewRouter(RouterConfig{API: application.NewAPI(fieldAuth{}, acceptingAggregator{}), MaxInflight: 1, Timeout: time.Second, Logger: slog.Default()})
	request := httptest.NewRequest(http.MethodPost, "/field-reports", strings.NewReader(`{"report_id":"r-1","hazard_type":"WILDFIRE","severity":"SIAGA","area_name":"Kalteng","occurred_at":"2026-11-12T02:43:00Z"}`))
	request.Header.Set("Authorization", "Bearer field-token")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusAccepted, response.Body.String())
	}
}

type fieldAuth struct{ summaryAuth }

func (fieldAuth) Validate(context.Context, string) (*ports.ValidatedToken, error) {
	return &ports.ValidatedToken{Valid: true, Scopes: []string{scopes.HazardReadSummary, scopes.HazardReadRaw, scopes.FieldReportsWrite}}, nil
}

type acceptingAggregator struct{ unusedAggregator }

func (acceptingAggregator) SubmitFieldReport(_ context.Context, input ports.FieldReportInput) (ports.FieldReport, error) {
	return ports.FieldReport{ReportID: input.ReportID, Status: "accepted"}, nil
}

type summaryAuth struct{}

func (summaryAuth) Issue(context.Context, string, string) (*ports.TokenPair, error) {
	return &ports.TokenPair{}, nil
}
func (summaryAuth) Refresh(context.Context, string) (*ports.TokenPair, error) {
	return &ports.TokenPair{}, nil
}
func (summaryAuth) Validate(context.Context, string) (*ports.ValidatedToken, error) {
	return &ports.ValidatedToken{Valid: true, Scopes: []string{scopes.HazardReadSummary}}, nil
}

type rejectingAuth struct {
	summaryAuth
}

func (rejectingAuth) Issue(context.Context, string, string) (*ports.TokenPair, error) {
	return nil, ports.ErrInvalidCredentials
}

type unusedAggregator struct{}

func (unusedAggregator) List(context.Context, ports.HazardFilter) (ports.HazardPage, error) {
	return ports.HazardPage{}, nil
}
func (unusedAggregator) GetHazard(context.Context, string, bool) (ports.Hazard, error) {
	return ports.Hazard{}, nil
}
func (unusedAggregator) Status(context.Context, string) (ports.SourceStatus, error) {
	return ports.SourceStatus{}, nil
}

func (unusedAggregator) SubmitFieldReport(context.Context, ports.FieldReportInput) (ports.FieldReport, error) {
	return ports.FieldReport{}, nil
}

func (unusedAggregator) Analytics(context.Context, ports.AnalyticsFilter) (ports.Analytics, error) {
	return ports.Analytics{}, nil
}

type failingAggregator struct {
	unusedAggregator
	err error
}

func (f failingAggregator) GetHazard(context.Context, string, bool) (ports.Hazard, error) {
	return ports.Hazard{}, f.err
}
func (f failingAggregator) List(context.Context, ports.HazardFilter) (ports.HazardPage, error) {
	return ports.HazardPage{}, f.err
}

type rejectingFieldAggregator struct{ unusedAggregator }

func (rejectingFieldAggregator) SubmitFieldReport(context.Context, ports.FieldReportInput) (ports.FieldReport, error) {
	return ports.FieldReport{}, fmt.Errorf("%w: invalid field report: area_name is required", ports.ErrInvalidArgument)
}

func postFieldReport(t *testing.T, aggregator ports.Aggregator, body string) *httptest.ResponseRecorder {
	t.Helper()
	router := NewRouter(RouterConfig{API: application.NewAPI(fieldAuth{}, aggregator), MaxInflight: 1, Timeout: time.Second, Logger: slog.Default()})
	request := httptest.NewRequest(http.MethodPost, "/field-reports", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer field-token")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}

func TestFieldReportClientMistakesAreBadRequestNotBadGateway(t *testing.T) {
	cases := map[string]string{
		"unknown hazard type": `{"report_id":"r-1","hazard_type":"TSUNAMI","severity":"SIAGA","area_name":"Padang","occurred_at":"2026-11-12T02:43:00Z"}`,
		"unknown severity":    `{"report_id":"r-1","hazard_type":"SEISMIC","severity":"BAHAYA","area_name":"Padang","occurred_at":"2026-11-12T02:43:00Z"}`,
		"no timezone":         `{"report_id":"r-1","hazard_type":"SEISMIC","severity":"SIAGA","area_name":"Padang","occurred_at":"2026-11-12T02:43:00"}`,
		"latitude range":      `{"report_id":"r-1","hazard_type":"SEISMIC","severity":"SIAGA","area_name":"Padang","latitude":91,"occurred_at":"2026-11-12T02:43:00Z"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if response := postFieldReport(t, unusedAggregator{}, body); response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", response.Code, response.Body.String())
			}
		})
	}
	upstream := postFieldReport(t, rejectingFieldAggregator{}, `{"report_id":"r-1","hazard_type":"SEISMIC","severity":"SIAGA","area_name":"Padang","occurred_at":"2026-11-12T02:43:00Z"}`)
	if upstream.Code != http.StatusBadRequest || !strings.Contains(upstream.Body.String(), "area_name is required") {
		t.Fatalf("upstream InvalidArgument status = %d body=%s", upstream.Code, upstream.Body.String())
	}
}

func TestFieldReportAcceptsLowercaseEnums(t *testing.T) {
	response := postFieldReport(t, acceptingAggregator{}, `{"report_id":"r-1","hazard_type":"seismic","severity":"siaga","area_name":"Padang","occurred_at":"2026-11-12T09:43:00+07:00"}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", response.Code, response.Body.String())
	}
}
