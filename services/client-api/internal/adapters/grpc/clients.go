package grpcadapter

import (
	"context"
	"fmt"
	"time"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	authpb "github.com/fathurwithyou/microservices/contracts/gen/go/auth"
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Clients struct {
	Auth       ports.Auth
	Aggregator ports.Aggregator
	close      func()
}

func Dial(aggregatorAddress, authAddress, internalServiceToken string) (*Clients, error) {
	aggregatorConnection, err := grpc.Dial(aggregatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial aggregator: %w", err)
	}
	authConnection, err := grpc.Dial(authAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = aggregatorConnection.Close()
		return nil, fmt.Errorf("dial auth: %w", err)
	}
	return &Clients{
		Auth:       &authClient{client: authpb.NewAuthClient(authConnection), internalServiceToken: internalServiceToken},
		Aggregator: &aggregatorClient{client: aggregatorpb.NewAggregatorClient(aggregatorConnection), internalServiceToken: internalServiceToken},
		close: func() {
			_ = aggregatorConnection.Close()
			_ = authConnection.Close()
		},
	}, nil
}

func (c *Clients) Close() { c.close() }

type authClient struct {
	client               authpb.AuthClient
	internalServiceToken string
}

func (c *authClient) Issue(ctx context.Context, clientID, secret string) (*authpb.TokenPair, error) {
	return c.client.IssueToken(withInternalMetadata(ctx, c.internalServiceToken), &authpb.IssueTokenRequest{ClientId: clientID, ClientSecret: secret})
}

func (c *authClient) Refresh(ctx context.Context, refreshToken string) (*authpb.TokenPair, error) {
	return c.client.RefreshToken(withInternalMetadata(ctx, c.internalServiceToken), &authpb.RefreshTokenRequest{RefreshToken: refreshToken})
}

func (c *authClient) Validate(ctx context.Context, accessToken string) (*authpb.ValidatedToken, error) {
	return c.client.ValidateToken(withInternalMetadata(ctx, c.internalServiceToken), &authpb.ValidateTokenRequest{AccessToken: accessToken})
}

type aggregatorClient struct {
	client               aggregatorpb.AggregatorClient
	internalServiceToken string
}

func (c *aggregatorClient) List(ctx context.Context, request *aggregatorpb.ListHazardsRequest) (*aggregatorpb.ListHazardsResponse, error) {
	return c.client.ListHazards(withInternalMetadata(ctx, c.internalServiceToken), request)
}

func (c *aggregatorClient) GetHazard(ctx context.Context, request *aggregatorpb.GetHazardRequest) (*eventspb.HazardEvent, error) {
	return c.client.GetHazard(withInternalMetadata(ctx, c.internalServiceToken), request)
}

func (c *aggregatorClient) Status(ctx context.Context, request *aggregatorpb.GetSourceStatusRequest) (*eventspb.SourceStatus, error) {
	return c.client.GetSourceStatus(withInternalMetadata(ctx, c.internalServiceToken), request)
}

type correlationIDKey struct{}

func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, correlationID)
}

func withInternalMetadata(ctx context.Context, internalServiceToken string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+internalServiceToken, "x-correlation-id", correlationID(ctx))
}

func correlationID(ctx context.Context) string {
	if value, ok := ctx.Value(correlationIDKey{}).(string); ok && value != "" {
		return value
	}
	return "client-api-" + fmt.Sprint(time.Now().UnixNano())
}
