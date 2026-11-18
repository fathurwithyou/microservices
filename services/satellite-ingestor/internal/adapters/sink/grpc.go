package sink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/fathurwithyou/microservices/pkg/grpckit"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// GRPC submits clustered fires to the aggregator canonical store.
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

// submitConcurrency bounds parallel SubmitHazard calls. A satellite pass
// changes hundreds of fires at once; sent one by one they would hold the
// offset back for minutes.
const submitConcurrency = 16

// Submit sends every fire and returns the first error. A batch carries each
// fire identifier at most once, so the calls do not depend on each other.
func (s *GRPC) Submit(ctx context.Context, fires []domain.Fire) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wait     sync.WaitGroup
		once     sync.Once
		firstErr error
		slots    = make(chan struct{}, submitConcurrency)
	)
	for _, fire := range fires {
		if ctx.Err() != nil {
			break
		}
		slots <- struct{}{}
		wait.Add(1)
		go func(fire domain.Fire) {
			defer wait.Done()
			defer func() { <-slots }()
			callCtx, callCancel := context.WithTimeout(s.context(ctx), submitTimeout)
			defer callCancel()
			if _, err := s.client.SubmitHazard(callCtx, &aggregatorpb.SubmitHazardRequest{
				Event:         fireEvent(fire),
				CorrelationId: correlation.FromContext(ctx),
			}); err != nil {
				once.Do(func() {
					firstErr = fmt.Errorf("submit fire %s: %w", fire.SourceRefID, err)
					cancel()
				})
			}
		}(fire)
	}
	wait.Wait()
	return firstErr
}

// submitTimeout bounds one call so a stuck Aggregator instance fails the
// batch (which is then replayed) instead of stalling the stream.
const submitTimeout = 15 * time.Second

func (s *GRPC) context(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, grpckit.Authorization(s.token))
}

func fireEvent(fire domain.Fire) *eventspb.HazardEvent {
	attributes := map[string]string{}
	put := func(key string, value any) {
		encoded, _ := json.Marshal(value)
		attributes[key] = string(encoded)
	}
	put("detection_count", fire.DetectionCount)
	put("satellites", fire.Satellites)
	put("max_confidence", string(fire.MaxConfidence))
	put("total_frp_mw", fire.TotalFRP)
	put("detection_keys", fire.DetectionKeys)
	status := fire.Status
	if status == "" {
		status = domain.StatusActive
	}
	put("status", status)

	sum := sha256.Sum256([]byte("SATELIT:" + fire.SourceRefID))
	return &eventspb.HazardEvent{
		HazardId:    "hz-" + hex.EncodeToString(sum[:16]),
		Source:      eventspb.Source_SOURCE_SATELIT,
		SourceRefId: fire.SourceRefID,
		HazardType:  eventspb.HazardType_HAZARD_TYPE_WILDFIRE,
		Severity:    wildfireSeverity(fire.Severity),
		AreaName:    fire.AreaName,
		Latitude:    fire.Latitude,
		Longitude:   fire.Longitude,
		OccurredAt:  fire.OccurredAt.UTC().Format(time.RFC3339),
		IngestedAt:  time.Now().UTC().Format(time.RFC3339),
		Attributes:  attributes,
	}
}

func wildfireSeverity(value domain.Severity) eventspb.Severity {
	switch value {
	case domain.SeverityAwas:
		return eventspb.Severity_SEVERITY_AWAS
	case domain.SeveritySiaga:
		return eventspb.Severity_SEVERITY_SIAGA
	default:
		return eventspb.Severity_SEVERITY_WASPADA
	}
}
