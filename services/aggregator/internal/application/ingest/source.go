package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/ports"
)

// Source is the contract every external hazard feed must satisfy. A Source
// owns exactly two concerns: which endpoints to call and how to map raw
// payloads into domain events. Scheduling, cursors, source status,
// persistence, and outbox enqueueing are owned by the Poller, so adding a
// new feed means implementing this interface and wiring it in main.
type Source interface {
	// Name identifies the feed for poll cursors, source status, and logs.
	Name() domain.Source
	// Fetch retrieves payloads observed since the given cursor and maps
	// them to domain events. Transport failures are returned as plain
	// errors; payloads that fail mapping are skipped (and reported through
	// a *MappingError) while the successfully mapped events are still
	// returned.
	Fetch(ctx context.Context, since time.Time, correlationID string) ([]domain.HazardEvent, error)
}

// MappingError reports source payloads that could not be mapped to domain
// events. It is returned together with the events that did map, allowing
// the poller to advance past poison payloads instead of retrying them
// forever. Rejected payloads are retained for inspection and replay.
type MappingError struct {
	Source   domain.Source
	Err      error
	Rejected []RejectedPayload
	// Transport marks a whole response that could not be decoded. Its
	// records were never seen individually, so the poller must not advance
	// the cursor past them: the next poll re-requests the same window.
	Transport bool
}

// RejectedPayload is one raw source record that failed mapping.
type RejectedPayload struct {
	Source domain.Source
	Raw    map[string]json.RawMessage
	// Payload overrides Raw for a body that could not be decoded into a map.
	Payload []byte
	Err     error
}

func (e *MappingError) Error() string {
	return fmt.Sprintf("map %s payloads: %v", e.Source, e.Err)
}

func (e *MappingError) Unwrap() error { return e.Err }

// malformedPayload converts a decode failure into a parked mapping failure.
func malformedPayload(source domain.Source, err error) (*MappingError, bool) {
	var bad *domain.MalformedPayloadError
	if !errors.As(err, &bad) {
		return nil, false
	}
	return &MappingError{
		Source:    source,
		Err:       bad,
		Transport: true,
		Rejected: []RejectedPayload{{
			Source:  source,
			Payload: bad.Body,
			// Keep the decode reason while classifying it as a permanent
			// payload violation: these exact bytes never decode.
			Err: fmt.Errorf("%w: %s", domain.ErrInvalidSourcePayload, bad.Error()),
		}},
	}, true
}

// fetchRecords performs one feed request and sorts decode failures. A body
// that cannot be split into records is returned as a transport
// *MappingError (the window is re-polled). Individual undecodable records are
// returned as permanently rejected payloads next to the records that decoded,
// so the caller maps the good ones and the cursor can move past the bad one.
func fetchRecords(ctx context.Context, client ports.SourceClient, source domain.Source, path, correlationID string) ([]map[string]json.RawMessage, []RejectedPayload, error) {
	records, err := client.Get(ctx, path, correlationID)
	var partial *domain.MalformedRecordsError
	if errors.As(err, &partial) {
		rejected := make([]RejectedPayload, 0, len(partial.Records))
		for _, record := range partial.Records {
			rejected = append(rejected, RejectedPayload{
				Source:  source,
				Payload: record.Body,
				Err:     fmt.Errorf("%w: %s", domain.ErrInvalidSourcePayload, record.Err.Error()),
			})
		}
		return records, rejected, nil
	}
	if err != nil {
		if malformed, ok := malformedPayload(source, err); ok {
			return nil, nil, malformed
		}
		return nil, nil, err
	}
	return records, nil, nil
}

// rejectionErrors lists the reasons of rejected payloads for a MappingError.
func rejectionErrors(rejected []RejectedPayload) []error {
	errs := make([]error, 0, len(rejected))
	for _, item := range rejected {
		errs = append(errs, item.Err)
	}
	return errs
}
