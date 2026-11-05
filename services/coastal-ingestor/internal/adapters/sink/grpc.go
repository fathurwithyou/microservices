package sink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/fathurwithyou/microservices/pkg/grpckit"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// GRPC submits accepted coastal readings to the aggregator.
type GRPC struct {
	connection *grpc.ClientConn
	client     aggregatorpb.AggregatorClient
	token      string
}

// NewGRPC dials the aggregator with the internal service token.
func NewGRPC(target, token string) (*GRPC, error) {
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial aggregator: %w", err)
	}
	return &GRPC{connection: connection, client: aggregatorpb.NewAggregatorClient(connection), token: token}, nil
}

// Close releases the aggregator connection.
func (s *GRPC) Close() error { return s.connection.Close() }

func (s *GRPC) Submit(ctx context.Context, readings []domain.Reading) error {
	for _, reading := range readings {
		request := &aggregatorpb.SubmitHazardRequest{Event: readingEvent(reading), CorrelationId: correlation.FromContext(ctx)}
		if _, err := s.client.SubmitHazard(metadata.NewOutgoingContext(ctx, grpckit.Authorization(s.token)), request); err != nil {
			return fmt.Errorf("submit reading %s: %w", reading.ID, err)
		}
	}
	return nil
}

func readingEvent(reading domain.Reading) *eventspb.HazardEvent {
	attributes := map[string]string{}
	put := func(key string, value any) {
		encoded, _ := json.Marshal(value)
		attributes[key] = string(encoded)
	}
	put("sensor_id", reading.SensorID)
	put("quality", reading.Quality)
	put("critical", reading.Critical)
	put("status", "active")

	sum := sha256.Sum256([]byte("SENSOR_PANTAI:" + reading.ID))
	return &eventspb.HazardEvent{
		HazardId:    "hz-" + hex.EncodeToString(sum[:16]),
		Source:      eventspb.Source_SOURCE_SENSOR_PANTAI,
		SourceRefId: reading.ID,
		HazardType:  eventspb.HazardType_HAZARD_TYPE_COASTAL,
		Severity:    coastalSeverity(reading.Severity),
		AreaName:    reading.AreaName,
		Latitude:    reading.Latitude,
		Longitude:   reading.Longitude,
		OccurredAt:  reading.OccurredAt.UTC().Format(time.RFC3339),
		IngestedAt:  time.Now().UTC().Format(time.RFC3339),
		Attributes:  attributes,
	}
}

func coastalSeverity(value domain.Severity) eventspb.Severity {
	switch value {
	case domain.SeverityAwas:
		return eventspb.Severity_SEVERITY_AWAS
	case domain.SeveritySiaga:
		return eventspb.Severity_SEVERITY_SIAGA
	case domain.SeverityWaspada:
		return eventspb.Severity_SEVERITY_WASPADA
	default:
		return eventspb.Severity_SEVERITY_NORMAL
	}
}
