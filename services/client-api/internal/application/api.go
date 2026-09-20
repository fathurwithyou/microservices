package application

import (
	"context"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	authpb "github.com/fathurwithyou/microservices/contracts/gen/go/auth"
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
)

type API struct {
	auth       ports.Auth
	aggregator ports.Aggregator
}

func NewAPI(auth ports.Auth, aggregator ports.Aggregator) *API {
	return &API{auth: auth, aggregator: aggregator}
}

func (a *API) IssueToken(ctx context.Context, clientID, secret string) (*authpb.TokenPair, error) {
	return a.auth.Issue(ctx, clientID, secret)
}

func (a *API) RefreshToken(ctx context.Context, refreshToken string) (*authpb.TokenPair, error) {
	return a.auth.Refresh(ctx, refreshToken)
}

func (a *API) ValidateToken(ctx context.Context, accessToken string) (*authpb.ValidatedToken, error) {
	return a.auth.Validate(ctx, accessToken)
}

func (a *API) ListHazards(ctx context.Context, request *aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error) {
	return a.aggregator.List(ctx, request)
}

func (a *API) GetHazard(ctx context.Context, request *aggregatorpb.GetHazardRequest) (*eventspb.HazardEvent, error) {
	return a.aggregator.GetHazard(ctx, request)
}

func (a *API) SourceStatus(ctx context.Context, request *aggregatorpb.GetSourceStatusRequest) (*eventspb.SourceStatus, error) {
	return a.aggregator.Status(ctx, request)
}
