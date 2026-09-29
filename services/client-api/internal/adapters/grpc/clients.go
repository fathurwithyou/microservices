// Package grpcadapter implements the outbound ports against the auth and
// aggregator gRPC services. Protobuf types are confined to this package:
// callers only see client-api-owned DTOs from internal/ports. gRPC status
// codes are translated to ports sentinel errors so the application layer can
// classify failures without importing gRPC.
package grpcadapter

import (
	"context"
	"fmt"
	"time"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	authpb "github.com/fathurwithyou/microservices/contracts/gen/go/auth"
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Clients bundles the outbound service clients.
type Clients struct {
	Auth       ports.Auth
	Aggregator ports.Aggregator
	close      func()
}

// Dial opens gRPC connections to the aggregator and auth services.
func Dial(aggregatorAddress, authAddress, internalServiceToken string) (*Clients, error) {
	aggregatorConnection, err := grpc.NewClient(aggregatorAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial aggregator: %w", err)
	}
	authConnection, err := grpc.NewClient(authAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

// Close releases the underlying connections.
func (c *Clients) Close() { c.close() }

type authClient struct {
	client               authpb.AuthClient
	internalServiceToken string
}

func (c *authClient) Issue(ctx context.Context, clientID, secret string) (*ports.TokenPair, error) {
	pair, err := c.client.IssueToken(withInternalMetadata(ctx, c.internalServiceToken), &authpb.IssueTokenRequest{ClientId: clientID, ClientSecret: secret})
	if err != nil {
		if status.Code(err) == codes.Unauthenticated {
			return nil, ports.ErrInvalidCredentials
		}
		return nil, grpcError(err)
	}
	return toTokenPair(pair), nil
}

func (c *authClient) Refresh(ctx context.Context, refreshToken string) (*ports.TokenPair, error) {
	pair, err := c.client.RefreshToken(withInternalMetadata(ctx, c.internalServiceToken), &authpb.RefreshTokenRequest{RefreshToken: refreshToken})
	if err != nil {
		if status.Code(err) == codes.Unauthenticated {
			return nil, ports.ErrInvalidToken
		}
		return nil, grpcError(err)
	}
	return toTokenPair(pair), nil
}

func (c *authClient) Validate(ctx context.Context, accessToken string) (*ports.ValidatedToken, error) {
	validated, err := c.client.ValidateToken(withInternalMetadata(ctx, c.internalServiceToken), &authpb.ValidateTokenRequest{AccessToken: accessToken})
	if err != nil {
		if status.Code(err) == codes.Unauthenticated {
			return nil, ports.ErrInvalidToken
		}
		return nil, grpcError(err)
	}
	return &ports.ValidatedToken{Valid: validated.GetValid(), ClientID: validated.GetClientId(), Scopes: validated.GetScopes()}, nil
}

func toTokenPair(pair *authpb.TokenPair) *ports.TokenPair {
	expiresAt, _ := time.Parse(time.RFC3339Nano, pair.GetExpiresAt())
	return &ports.TokenPair{AccessToken: pair.GetAccessToken(), RefreshToken: pair.GetRefreshToken(), ExpiresAt: expiresAt, ClientID: pair.GetClientId(), Scopes: pair.GetScopes()}
}

type aggregatorClient struct {
	client               aggregatorpb.AggregatorClient
	internalServiceToken string
}

func (c *aggregatorClient) List(ctx context.Context, filter ports.HazardFilter) (ports.HazardPage, error) {
	response, err := c.client.ListHazards(withInternalMetadata(ctx, c.internalServiceToken), &aggregatorpb.ListHazardsRequest{
		Source:     sourceProto(filter.Source),
		HazardType: hazardTypeProto(filter.HazardType),
		Severity:   severityProto(filter.Severity),
		PageSize:   filter.PageSize,
		PageToken:  filter.PageToken,
		Raw:        filter.Raw,
	})
	if err != nil {
		return ports.HazardPage{}, grpcError(err)
	}
	hazards := make([]ports.Hazard, 0, len(response.GetHazards()))
	for _, event := range response.GetHazards() {
		hazards = append(hazards, toHazard(event))
	}
	return ports.HazardPage{Hazards: hazards, NextPageToken: response.GetNextPageToken()}, nil
}

func (c *aggregatorClient) GetHazard(ctx context.Context, hazardID string, raw bool) (ports.Hazard, error) {
	event, err := c.client.GetHazard(withInternalMetadata(ctx, c.internalServiceToken), &aggregatorpb.GetHazardRequest{HazardId: hazardID, Raw: raw})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return ports.Hazard{}, ports.ErrNotFound
		}
		return ports.Hazard{}, grpcError(err)
	}
	return toHazard(event), nil
}

func (c *aggregatorClient) Status(ctx context.Context, source string) (ports.SourceStatus, error) {
	response, err := c.client.GetSourceStatus(withInternalMetadata(ctx, c.internalServiceToken), &aggregatorpb.GetSourceStatusRequest{Source: sourceProto(source)})
	if err != nil {
		return ports.SourceStatus{}, grpcError(err)
	}
	return ports.SourceStatus{Source: source, Status: response.GetStatus(), LastSuccessAt: response.GetLastSuccessAt(), StaleSince: response.GetStaleSince(), LastError: response.GetLastError()}, nil
}

func toHazard(event *eventspb.HazardEvent) ports.Hazard {
	attributes := make(map[string]string, len(event.GetAttributes()))
	for key, value := range event.GetAttributes() {
		attributes[key] = value
	}
	return ports.Hazard{
		HazardID:    event.GetHazardId(),
		Source:      event.GetSource().String(),
		SourceRefID: event.GetSourceRefId(),
		HazardType:  event.GetHazardType().String(),
		Severity:    event.GetSeverity().String(),
		AreaName:    event.GetAreaName(),
		Latitude:    event.GetLatitude(),
		Longitude:   event.GetLongitude(),
		OccurredAt:  event.GetOccurredAt(),
		IngestedAt:  event.GetIngestedAt(),
		Attributes:  attributes,
	}
}

func sourceProto(value string) eventspb.Source {
	switch value {
	case "BMKG":
		return eventspb.Source_SOURCE_BMKG
	case "PVMBG":
		return eventspb.Source_SOURCE_PVMBG
	default:
		return eventspb.Source_SOURCE_UNSPECIFIED
	}
}

func hazardTypeProto(value string) eventspb.HazardType {
	switch value {
	case "SEISMIC":
		return eventspb.HazardType_HAZARD_TYPE_SEISMIC
	case "VOLCANIC":
		return eventspb.HazardType_HAZARD_TYPE_VOLCANIC
	default:
		return eventspb.HazardType_HAZARD_TYPE_UNSPECIFIED
	}
}

func severityProto(value string) eventspb.Severity {
	switch value {
	case "NORMAL":
		return eventspb.Severity_SEVERITY_NORMAL
	case "WASPADA":
		return eventspb.Severity_SEVERITY_WASPADA
	case "SIAGA":
		return eventspb.Severity_SEVERITY_SIAGA
	case "AWAS":
		return eventspb.Severity_SEVERITY_AWAS
	default:
		return eventspb.Severity_SEVERITY_UNSPECIFIED
	}
}

// grpcError translates transport-level gRPC failures to ports sentinels,
// preserving the original error for logs via errors.Is chains.
func grpcError(err error) error {
	switch status.Code(err) {
	case codes.Unavailable:
		return fmt.Errorf("%w: %v", ports.ErrUnavailable, err)
	case codes.NotFound:
		return fmt.Errorf("%w: %v", ports.ErrNotFound, err)
	default:
		return fmt.Errorf("%w: %v", ports.ErrUpstreamFailure, err)
	}
}

func withInternalMetadata(ctx context.Context, internalServiceToken string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+internalServiceToken, correlation.GRPCKeyAlt, requestCorrelationID(ctx))
}

// requestCorrelationID reads the correlation ID the HTTP middleware stored
// in the request context, generating one when absent.
func requestCorrelationID(ctx context.Context) string {
	if id := correlation.FromContext(ctx); id != "" {
		return id
	}
	return correlation.New()
}
