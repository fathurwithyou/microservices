package sink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
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

// submitTimeout bounds one SubmitHazard call. A call normally takes well
// under a second; without a bound a hung Aggregator connection would hold the
// critical lane (and the whole poll loop) indefinitely.
const submitTimeout = 5 * time.Second

// Submit stores each reading in the Aggregator. It is safe for concurrent
// use; the ingestor calls it with one reading per call from several workers.
func (s *GRPC) Submit(ctx context.Context, readings []domain.Reading) error {
	for _, reading := range readings {
		if err := s.submitOne(ctx, reading); err != nil {
			return err
		}
	}
	return nil
}

func (s *GRPC) submitOne(ctx context.Context, reading domain.Reading) error {
	ctx, cancel := context.WithTimeout(ctx, submitTimeout)
	defer cancel()
	request := &aggregatorpb.SubmitHazardRequest{Event: readingEvent(reading), CorrelationId: correlation.FromContext(ctx)}
	if _, err := s.client.SubmitHazard(metadata.NewOutgoingContext(ctx, grpckit.Authorization(s.token)), request); err != nil {
		return fmt.Errorf("submit reading %s: %w", reading.ID, err)
	}
	return nil
}

// readingEvent maps a reading to the canonical event.
//
// The Aggregator treats an event whose attributes changed as a changed event
// and announces it again, so attributes must be a pure function of the
// observation. The measurements, quality and validation note are. The two
// timing attributes (received_at, observation_generated_at) are not, so they
// are attached to critical readings only, where the latency measurement needs
// them, and the source adapter keeps them stable per observation (first
// receipt). Ordinary readings stay byte-identical on a repeated read of the
// stream and remain a no-op in the Aggregator.
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
	// JSON has no NaN or infinity; a non-finite value would encode as an
	// empty attribute, so it is left out and the note says why.
	if reading.HasMeasurements && finite(reading.WaveHeightM) && finite(reading.SeaLevelAnomalyM) {
		put("wave_height_m", reading.WaveHeightM)
		put("sea_level_anomaly_m", reading.SeaLevelAnomalyM)
	}
	if reading.Note != "" {
		put("validation_note", reading.Note)
	}
	if reading.Critical {
		// received_at: ingestor wall clock when the page carrying this
		// reading finished arriving; the notifier measures delivery latency
		// from it. Nanosecond resolution, unlike ingested_at.
		if !reading.ReceivedAt.IsZero() {
			put("received_at", reading.ReceivedAt.UTC().Format(time.RFC3339Nano))
		}
		// observation_generated_at: the sensor's own clock for that page.
		if !reading.GeneratedAt.IsZero() {
			put("observation_generated_at", reading.GeneratedAt.UTC().Format(time.RFC3339))
		}
	}

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

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

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
