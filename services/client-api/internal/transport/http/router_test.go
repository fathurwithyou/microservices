package httptransport

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	authpb "github.com/fathurwithyou/microservices/contracts/gen/go/auth"
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/services/client-api/internal/application"
)

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

type summaryAuth struct{}

func (summaryAuth) Issue(context.Context, string, string) (*authpb.TokenPair, error) { return nil, nil }
func (summaryAuth) Refresh(context.Context, string) (*authpb.TokenPair, error)       { return nil, nil }
func (summaryAuth) Validate(context.Context, string) (*authpb.ValidatedToken, error) {
	return &authpb.ValidatedToken{Valid: true, Scopes: []string{"hazard:read:summary"}}, nil
}

type unusedAggregator struct{}

func (unusedAggregator) List(context.Context, *aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error) {
	return nil, nil
}
func (unusedAggregator) GetHazard(context.Context, *aggregatorpb.GetHazardRequest) (*eventspb.HazardEvent, error) {
	return nil, nil
}
func (unusedAggregator) Status(context.Context, *aggregatorpb.GetSourceStatusRequest) (*eventspb.SourceStatus, error) {
	return nil, nil
}
