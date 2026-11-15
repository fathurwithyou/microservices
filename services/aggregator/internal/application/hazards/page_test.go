package hazards

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

func TestPageTokenRoundTrip(t *testing.T) {
	event := domain.HazardEvent{HazardID: "abc", OccurredAt: time.Date(2026, 11, 16, 1, 2, 3, 456789000, time.UTC)}
	cursor, ok, err := DecodePageToken(EncodePageToken(event))
	if err != nil || !ok {
		t.Fatalf("decode: ok=%v err=%v", ok, err)
	}
	if !cursor.OccurredAt.Equal(event.OccurredAt) || cursor.HazardID != event.HazardID {
		t.Fatalf("cursor = %+v", cursor)
	}
}

func TestDecodePageTokenRejectsForeignTokens(t *testing.T) {
	for _, token := range []string{"h-1", "!!!", "djJ8eHx5", "djF8bm90LWEtdGltZXxpZA"} {
		if _, _, err := DecodePageToken(token); !errors.Is(err, ErrInvalidPageToken) {
			t.Fatalf("token %q: err = %v", token, err)
		}
	}
	if _, ok, err := DecodePageToken(""); ok || err != nil {
		t.Fatalf("empty token: ok=%v err=%v", ok, err)
	}
}

// sliceRepo applies the same ordering and cursor rule the stores use.
type sliceRepo struct{ events []domain.HazardEvent }

func (r sliceRepo) ListHazards(_ context.Context, filter ListFilter) ([]domain.HazardEvent, error) {
	items := append([]domain.HazardEvent(nil), r.events...)
	sort.Slice(items, func(i, j int) bool {
		if !items[i].OccurredAt.Equal(items[j].OccurredAt) {
			return items[i].OccurredAt.After(items[j].OccurredAt)
		}
		return items[i].HazardID > items[j].HazardID
	})
	cursor, after, err := DecodePageToken(filter.PageToken)
	if err != nil {
		return nil, err
	}
	out := make([]domain.HazardEvent, 0, filter.PageSize)
	for _, item := range items {
		if after && !cursor.Before(item) {
			continue
		}
		if len(out) == filter.PageSize {
			break
		}
		out = append(out, item)
	}
	return out, nil
}

func (sliceRepo) GetHazard(context.Context, string) (domain.HazardEvent, error) {
	return domain.HazardEvent{}, nil
}

func (sliceRepo) GetSourceStatus(context.Context, domain.Source) (domain.SourceStatus, error) {
	return domain.SourceStatus{}, nil
}

func TestListWalksEveryRowOnceAcrossTies(t *testing.T) {
	base := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	events := make([]domain.HazardEvent, 0, 137)
	for i := 0; i < 137; i++ {
		// Groups of seven share one occurred_at, so ordering needs the tiebreaker.
		events = append(events, domain.HazardEvent{HazardID: fmt.Sprintf("h-%03d", i), OccurredAt: base.Add(-time.Duration(i/7) * time.Minute)})
	}
	queries := NewQueries(sliceRepo{events: events})

	seen := map[string]bool{}
	var previous *domain.HazardEvent
	token := ""
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
		items, next, err := queries.List(context.Background(), ListFilter{PageSize: 50, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for index := range items {
			item := items[index]
			if seen[item.HazardID] {
				t.Fatalf("duplicate %s", item.HazardID)
			}
			seen[item.HazardID] = true
			if previous != nil && !(Cursor{OccurredAt: previous.OccurredAt, HazardID: previous.HazardID}).Before(item) {
				t.Fatalf("order broken: %s then %s", previous.HazardID, item.HazardID)
			}
			previous = &items[index]
		}
		if next == "" {
			break
		}
		token = next
	}
	if len(seen) != len(events) {
		t.Fatalf("saw %d of %d rows", len(seen), len(events))
	}
}

func TestListRejectsInvalidToken(t *testing.T) {
	_, _, err := NewQueries(sliceRepo{}).List(context.Background(), ListFilter{PageToken: "h-1"})
	if !errors.Is(err, ErrInvalidPageToken) {
		t.Fatalf("err = %v", err)
	}
}
