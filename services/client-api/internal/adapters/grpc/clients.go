// Package grpcadapter implements the outbound ports against the auth and
// aggregator gRPC services. Protobuf types are confined to this package:
// callers only see client-api-owned DTOs from internal/ports. gRPC status
// codes are translated to ports sentinel errors so the application layer can
// classify failures without importing gRPC.
package grpcadapter

import (
	"context"
	"fmt"
	"strings"
	"time"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	authpb "github.com/fathurwithyou/microservices/contracts/gen/go/auth"
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/fathurwithyou/microservices/services/client-api/internal/ports"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Clients bundles the outbound service clients.
type Clients struct {
	Auth       ports.Auth
	Aggregator ports.Aggregator
	close      func()
}

// aggregatorServiceConfig balances every call over all resolved Aggregator
// instances (use a dns:/// target on a headless Service) and retries reads
// that failed with UNAVAILABLE, e.g. while an instance drains during a
// rolling update. Writes are not retried here.
const aggregatorServiceConfig = `{
  "loadBalancingConfig": [{"round_robin": {}}],
  "methodConfig": [{
    "name": [
      {"service": "bnpb.aggregator.Aggregator", "method": "ListHazards"},
      {"service": "bnpb.aggregator.Aggregator", "method": "GetHazard"},
      {"service": "bnpb.aggregator.Aggregator", "method": "GetSourceStatus"},
      {"service": "bnpb.aggregator.Aggregator", "method": "GetAnalytics"},
      {"service": "bnpb.aggregator.Aggregator", "method": "HealthCheck"}
    ],
    "retryPolicy": {"maxAttempts": 3, "initialBackoff": "0.05s", "maxBackoff": "0.5s", "backoffMultiplier": 2, "retryableStatusCodes": ["UNAVAILABLE"]}
  }]
}`

// readAttemptTimeout bounds one attempt of an idempotent read. An instance
// whose node was powered off never answers and never resets the connection,
// and round_robin keeps handing it calls until the keepalive pings notice
// (~13 s). Capping each attempt turns that silence into DEADLINE_EXCEEDED so
// the retry lands on the next instance, and the HTTP request still answers
// within its own timeout instead of 502. A variable so tests can shorten it.
var readAttemptTimeout = 2 * time.Second

// retryRead runs an idempotent read: the first attempt is capped at
// readAttemptTimeout and, when it only timed out, a second attempt gets the
// rest of the caller's deadline. The cap exists to escape a silent peer, so
// it is not applied twice: a slow but live instance still gets the remaining
// time on the retry. Any other failure is returned as is.
func retryRead[T any](ctx context.Context, call func(context.Context) (T, error)) (T, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, readAttemptTimeout)
	result, err := call(attemptCtx)
	cancel()
	if err == nil || status.Code(err) != codes.DeadlineExceeded || ctx.Err() != nil {
		return result, err
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < readAttemptTimeout/2 {
		return result, err
	}
	return call(ctx)
}

// authServiceConfig retries the idempotent token validation once the
// transport reports UNAVAILABLE, e.g. right after the keepalive pings have
// found the Auth pod gone and the connection is being re-established.
const authServiceConfig = `{
  "methodConfig": [{
    "name": [{"service": "bnpb.auth.Auth", "method": "ValidateToken"}],
    "retryPolicy": {"maxAttempts": 3, "initialBackoff": "0.05s", "maxBackoff": "0.5s", "backoffMultiplier": 2, "retryableStatusCodes": ["UNAVAILABLE"]}
  }]
}`

// Dial opens gRPC connections to the aggregator and auth services.
func Dial(aggregatorAddress, authAddress, internalServiceToken string) (*Clients, error) {
	aggregatorConnection, err := grpc.NewClient(aggregatorAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDefaultServiceConfig(aggregatorServiceConfig),
		// A powered-off instance never sends a RST; pings find it in ~13 s
		// and round_robin stops picking it.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 3 * time.Second, PermitWithoutStream: true}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial aggregator: %w", err)
	}
	// Without keepalive a powered-off Auth node left this connection "ready"
	// for the TCP retransmit timeout (~15 min): every token validation hung
	// until the request deadline and the gateway answered 502 for all of that
	// time, even after Auth was rescheduled (P1.4 run, 17 Nov).
	authConnection, err := grpc.NewClient(authAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDefaultServiceConfig(authServiceConfig),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 3 * time.Second, PermitWithoutStream: true}),
	)
	if err != nil {
		_ = aggregatorConnection.Close()
		return nil, fmt.Errorf("dial auth: %w", err)
	}
	return &Clients{
		Auth:       newCachingAuth(&authClient{client: authpb.NewAuthClient(authConnection), internalServiceToken: internalServiceToken}),
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
	validated, _, err := c.validate(ctx, accessToken)
	return validated, err
}

// validate also returns the token's expiry so callers can cache the result.
// It goes through retryRead: validation is idempotent, and a silent Auth peer
// must cost one capped attempt, not the whole request deadline.
func (c *authClient) validate(ctx context.Context, accessToken string) (*ports.ValidatedToken, time.Time, error) {
	validated, err := retryRead(ctx, func(ctx context.Context) (*authpb.ValidatedToken, error) {
		return c.client.ValidateToken(withInternalMetadata(ctx, c.internalServiceToken), &authpb.ValidateTokenRequest{AccessToken: accessToken})
	})
	if err != nil {
		if status.Code(err) == codes.Unauthenticated {
			return nil, time.Time{}, ports.ErrInvalidToken
		}
		return nil, time.Time{}, grpcError(err)
	}
	expiresAt, _ := time.Parse(time.RFC3339Nano, validated.GetExpiresAt())
	return &ports.ValidatedToken{Valid: validated.GetValid(), ClientID: validated.GetClientId(), Scopes: validated.GetScopes()}, expiresAt, nil
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
	var header metadata.MD
	response, err := retryRead(ctx, func(ctx context.Context) (*aggregatorpb.ListHazardsResponse, error) {
		return c.client.ListHazards(withInternalMetadata(ctx, c.internalServiceToken), &aggregatorpb.ListHazardsRequest{
			Source:     sourceProto(filter.Source),
			HazardType: hazardTypeProto(filter.HazardType),
			Severity:   severityProto(filter.Severity),
			PageSize:   filter.PageSize,
			PageToken:  filter.PageToken,
			Raw:        filter.Raw,
		}, grpc.Header(&header))
	})
	if err != nil {
		return ports.HazardPage{}, grpcError(err)
	}
	hazards := make([]ports.Hazard, 0, len(response.GetHazards()))
	for _, event := range response.GetHazards() {
		hazards = append(hazards, toHazard(event))
	}
	return ports.HazardPage{
		Hazards: hazards, NextPageToken: response.GetNextPageToken(),
		Partial: response.GetPartial(), UnavailableUnits: response.GetUnavailableUnits(),
		StaleSince: staleSince(header),
	}, nil
}

func (c *aggregatorClient) GetHazard(ctx context.Context, hazardID string, raw bool) (ports.Hazard, error) {
	event, err := retryRead(ctx, func(ctx context.Context) (*eventspb.HazardEvent, error) {
		return c.client.GetHazard(withInternalMetadata(ctx, c.internalServiceToken), &aggregatorpb.GetHazardRequest{HazardId: hazardID, Raw: raw})
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return ports.Hazard{}, ports.ErrNotFound
		}
		return ports.Hazard{}, grpcError(err)
	}
	return toHazard(event), nil
}

func (c *aggregatorClient) Status(ctx context.Context, source string) (ports.SourceStatus, error) {
	response, err := retryRead(ctx, func(ctx context.Context) (*eventspb.SourceStatus, error) {
		return c.client.GetSourceStatus(withInternalMetadata(ctx, c.internalServiceToken), &aggregatorpb.GetSourceStatusRequest{Source: sourceProto(source)})
	})
	if err != nil {
		return ports.SourceStatus{}, grpcError(err)
	}
	return ports.SourceStatus{Source: source, Status: response.GetStatus(), LastSuccessAt: response.GetLastSuccessAt(), StaleSince: response.GetStaleSince(), LastError: response.GetLastError()}, nil
}

func (c *aggregatorClient) SubmitFieldReport(ctx context.Context, input ports.FieldReportInput) (ports.FieldReport, error) {
	response, err := c.client.SubmitFieldReport(withInternalMetadata(ctx, c.internalServiceToken), &aggregatorpb.SubmitFieldReportRequest{
		ReportId:    input.ReportID,
		HazardType:  hazardTypeProto(input.HazardType),
		Severity:    severityProto(input.Severity),
		AreaName:    input.AreaName,
		Latitude:    input.Latitude,
		Longitude:   input.Longitude,
		OccurredAt:  input.OccurredAt,
		Description: input.Description,
	})
	if err != nil {
		return ports.FieldReport{}, grpcError(err)
	}
	return ports.FieldReport{ReportID: response.GetReportId(), Status: response.GetStatus()}, nil
}

func (c *aggregatorClient) Analytics(ctx context.Context, filter ports.AnalyticsFilter) (ports.Analytics, error) {
	var header metadata.MD
	response, err := retryRead(ctx, func(ctx context.Context) (*aggregatorpb.AnalyticsResponse, error) {
		return c.client.GetAnalytics(withInternalMetadata(ctx, c.internalServiceToken), &aggregatorpb.AnalyticsRequest{Since: filter.Since, Until: filter.Until, Bucket: filter.Bucket}, grpc.Header(&header))
	})
	if err != nil {
		return ports.Analytics{}, grpcError(err)
	}
	result := ports.Analytics{AsOf: response.GetAsOf(), Partial: response.GetPartial(), UnavailableUnits: response.GetUnavailableUnits(), StaleSince: staleSince(header)}
	for _, item := range response.GetHazardTypes() {
		result.HazardTypes = append(result.HazardTypes, ports.AnalyticsTypeCount{HazardType: analyticsCategory(item.GetCategory(), item.GetHazardType()), Count: item.GetCount()})
	}
	for _, item := range response.GetSeverities() {
		result.Severities = append(result.Severities, ports.AnalyticsSeverity{Severity: enumName(item.GetSeverity().String()), Count: item.GetCount()})
	}
	for _, item := range response.GetAreas() {
		result.Areas = append(result.Areas, ports.AnalyticsArea{AreaName: item.GetAreaName(), Count: item.GetCount()})
	}
	for _, item := range response.GetTimeBuckets() {
		result.Buckets = append(result.Buckets, ports.AnalyticsBucket{BucketStart: item.GetBucketStart(), HazardType: analyticsCategory(item.GetCategory(), item.GetHazardType()), Count: item.GetCount()})
	}
	return result, nil
}

// staleSinceHeader is set by the Aggregator on an answer it served from
// memory because its store did not answer: the time (RFC3339) that data was
// read. Such an answer is also marked partial in the message itself; the
// header only adds its age, without a change to the contract.
const staleSinceHeader = "x-stale-since"

func staleSince(header metadata.MD) string {
	if values := header.Get(staleSinceHeader); len(values) > 0 {
		return values[0]
	}
	return ""
}

// analyticsCategory prefers the aggregator's dashboard category (which adds
// TSUNAMI) and falls back to the canonical enum for older aggregators.
func analyticsCategory(category string, hazardType eventspb.HazardType) string {
	if category != "" {
		return category
	}
	return enumName(hazardType.String())
}

func enumName(value string) string {
	if index := strings.LastIndexByte(value, '_'); index >= 0 {
		return value[index+1:]
	}
	return value
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
	case "SATELIT":
		return eventspb.Source_SOURCE_SATELIT
	case "SENSOR_PANTAI":
		return eventspb.Source_SOURCE_SENSOR_PANTAI
	default:
		return eventspb.Source_SOURCE_UNSPECIFIED
	}
}

func hazardTypeProto(value string) eventspb.HazardType {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "SEISMIC":
		return eventspb.HazardType_HAZARD_TYPE_SEISMIC
	case "VOLCANIC":
		return eventspb.HazardType_HAZARD_TYPE_VOLCANIC
	case "WILDFIRE":
		return eventspb.HazardType_HAZARD_TYPE_WILDFIRE
	case "COASTAL":
		return eventspb.HazardType_HAZARD_TYPE_COASTAL
	default:
		return eventspb.HazardType_HAZARD_TYPE_UNSPECIFIED
	}
}

func severityProto(value string) eventspb.Severity {
	switch strings.ToUpper(strings.TrimSpace(value)) {
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
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %s", ports.ErrInvalidArgument, status.Convert(err).Message())
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
