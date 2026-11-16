package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type fakeRepository struct {
	events []domain.HazardEvent
	ids    []string
}

func (f *fakeRepository) ListAuditEvents(_ context.Context, filter Filter) ([]domain.HazardEvent, error) {
	result := make([]domain.HazardEvent, 0)
	for _, event := range f.events {
		if filter.Source != "" && event.Source != filter.Source {
			continue
		}
		if event.IngestedAt.Before(filter.Since) {
			continue
		}
		if filter.Cursor != "" && (event.IngestedAt.Before(filter.Since) || (event.IngestedAt.Equal(filter.Since) && event.HazardID <= filter.Cursor)) {
			continue
		}
		result = append(result, event)
	}
	if len(result) > filter.Limit {
		result = result[:filter.Limit]
	}
	return result, nil
}

func (f *fakeRepository) SourceRefIDs(context.Context, domain.Source) ([]string, error) {
	return f.ids, nil
}

func (f *fakeRepository) HazardLocations(context.Context, string) ([]Location, error) {
	return nil, nil
}

func (f *fakeRepository) HazardDistribution(context.Context) (Distribution, error) {
	return Distribution{}, nil
}

func (f *fakeRepository) FieldReportsByID(context.Context, []string) ([]domain.FieldReport, error) {
	return nil, nil
}

func (f *fakeRepository) HazardExists(context.Context, string) (bool, error) { return true, nil }
func (f *fakeRepository) UnavailableUnits(context.Context) ([]string, error) { return nil, nil }

func (f *fakeRepository) ListIngestFailures(context.Context, domain.FailureFilter) ([]domain.IngestFailure, error) {
	return nil, nil
}

func (f *fakeRepository) GetIngestFailure(context.Context, string) (domain.IngestFailure, error) {
	return domain.IngestFailure{}, domain.ErrNotFound
}

func auditEvents() []domain.HazardEvent {
	at := time.Date(2026, 11, 12, 3, 0, 0, 0, time.UTC)
	return []domain.HazardEvent{
		{HazardID: "h-1", Source: domain.SourceSatellite, SourceRefID: "a", IngestedAt: at},
		{HazardID: "h-2", Source: domain.SourceSatellite, SourceRefID: "b", IngestedAt: at},
		{HazardID: "h-3", Source: domain.SourceSatellite, SourceRefID: "c", IngestedAt: at.Add(time.Second)},
	}
}

func TestListPaginatesWithCursor(t *testing.T) {
	queries := NewQueries(&fakeRepository{events: auditEvents()})
	first, next, err := queries.List(context.Background(), Filter{Source: domain.SourceSatellite, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || next == "" {
		t.Fatalf("first page = %d events, next=%q", len(first), next)
	}
	second, next, err := queries.List(context.Background(), Filter{Source: domain.SourceSatellite, Limit: 2, Cursor: next})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].HazardID != "h-3" {
		t.Fatalf("second page = %#v", second)
	}
	if next != "" {
		t.Fatalf("last page next = %q, want empty", next)
	}
}

func TestListRejectsInvalidCursor(t *testing.T) {
	queries := NewQueries(&fakeRepository{events: auditEvents()})
	if _, _, err := queries.List(context.Background(), Filter{Cursor: "not-base64"}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("error = %v, want ErrInvalidCursor", err)
	}
}

func TestChecksumUsesSortedSourceRefIDs(t *testing.T) {
	repository := &fakeRepository{ids: []string{"a", "b", "c"}}
	queries := NewQueries(repository)
	count, checksum, err := queries.Checksum(context.Background(), domain.SourceSatellite)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("count = %d", count)
	}
	want := sha256.Sum256([]byte("a\nb\nc"))
	if checksum != hex.EncodeToString(want[:]) {
		t.Fatalf("checksum = %s", checksum)
	}
}

func TestFailureCursorRoundTrip(t *testing.T) {
	want := failureCursor{ReceivedAt: time.Date(2026, 11, 12, 2, 40, 11, 0, time.UTC), MessageID: "msg-31c9"}
	encoded, err := encodeFailureCursor(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeFailureCursor(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !got.ReceivedAt.Equal(want.ReceivedAt) || got.MessageID != want.MessageID {
		t.Fatalf("decoded = %+v, want %+v", got, want)
	}
}

func TestUnprocessedRejectsInvalidCursor(t *testing.T) {
	queries := NewQueries(&fakeRepository{})
	if _, _, err := queries.Unprocessed(context.Background(), "", 10, "not-a-cursor"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("err = %v, want ErrInvalidCursor", err)
	}
}
