package postgres

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/analytics"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/google/uuid"
)

// TestRepositoryAgainstCockroach exercises the M2 queries on a real
// CockroachDB. It skips unless AGGREGATOR_TEST_DATABASE_URL points at a
// database the test may write to, for example
// postgresql://aggregator@127.0.0.1:26257/aggregator_db?sslmode=disable.
func TestRepositoryAgainstCockroach(t *testing.T) {
	databaseURL := os.Getenv("AGGREGATOR_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("AGGREGATOR_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	repository, err := NewRepositoryWithWait(ctx, databaseURL, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()

	run := uuid.NewString()[:8]
	at := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(time.Now().Unix()%3600) * time.Second)
	events := []domain.HazardEvent{
		{HazardID: run + "-q", Source: domain.SourceBMKG, SourceRefID: run + "-q", HazardType: domain.HazardSeismic, Severity: domain.SeverityNormal, AreaName: "A", OccurredAt: at, IngestedAt: at, Attributes: map[string]any{"potential_tsunami": false}},
		{HazardID: run + "-t1", Source: domain.SourceBMKG, SourceRefID: run + "-t1", HazardType: domain.HazardSeismic, Severity: domain.SeveritySiaga, AreaName: "A", OccurredAt: at, IngestedAt: at, Attributes: map[string]any{"potential_tsunami": true}},
		{HazardID: run + "-t2", Source: domain.SourceBMKG, SourceRefID: run + "-t2", HazardType: domain.HazardSeismic, Severity: domain.SeverityAwas, AreaName: "A", OccurredAt: at, IngestedAt: at, Attributes: map[string]any{"potential_tsunami": false, "warning_id": "w-" + run}},
	}
	for _, event := range events {
		message := domain.OutboxMessage{HazardID: event.HazardID, CorrelationID: run, RoutingKey: "hazard.event", Payload: []byte("{}")}
		if err := repository.PersistHazardAndEnqueue(ctx, event, message); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("write fence", func(t *testing.T) {
		store := repository.LeaseStore("fence-test-" + run)
		token, ok, err := store.Acquire(ctx, "pod-a", 2*time.Second)
		if err != nil || !ok {
			t.Fatalf("acquire: ok=%v err=%v", ok, err)
		}
		write := func(suffix string, fence domain.WriteFence) error {
			event := domain.HazardEvent{HazardID: run + "-f" + suffix, Source: domain.SourcePVMBG, SourceRefID: run + "-f" + suffix, HazardType: domain.HazardVolcanic, Severity: domain.SeverityNormal, AreaName: "A", OccurredAt: at, IngestedAt: at, Attributes: map[string]any{}}
			message := domain.OutboxMessage{HazardID: event.HazardID, CorrelationID: run, RoutingKey: "hazard.event", Payload: []byte("{}")}
			return repository.PersistHazardAndEnqueue(domain.WithWriteFence(ctx, fence), event, message)
		}
		current := domain.WriteFence{Lease: "fence-test-" + run, Holder: token.Holder, Fence: token.Fence}
		if err := write("1", current); err != nil {
			t.Fatalf("current term refused: %v", err)
		}
		stale := current
		stale.Fence--
		if err := write("2", stale); !errors.Is(err, domain.ErrFenced) {
			t.Fatalf("stale fence err = %v, want ErrFenced", err)
		}
		// Expiry by the database clock ends the term even for its holder,
		// and a takeover gets a newer fence.
		time.Sleep(2100 * time.Millisecond)
		if err := write("3", current); !errors.Is(err, domain.ErrFenced) {
			t.Fatalf("expired term err = %v, want ErrFenced", err)
		}
		next, ok, err := store.Acquire(ctx, "pod-b", time.Minute)
		if err != nil || !ok || next.Fence <= token.Fence {
			t.Fatalf("takeover: token=%+v ok=%v err=%v", next, ok, err)
		}
		if err := write("4", current); !errors.Is(err, domain.ErrFenced) {
			t.Fatalf("old holder after takeover err = %v, want ErrFenced", err)
		}
		if exists, _ := repository.HazardExists(ctx, run+"-f4"); exists {
			t.Fatal("fenced write was persisted")
		}
	})

	t.Run("tsunami category", func(t *testing.T) {
		// The events were written moments ago: read at present time so the
		// historical snapshot does not hide them.
		repository.AnalyticsStaleness = 0
		result, err := analytics.NewQueries(repository).Get(ctx, analytics.Filter{Since: at, Until: at.Add(time.Second), Bucket: analytics.BucketDay})
		if err != nil {
			t.Fatal(err)
		}
		got := map[domain.HazardType]int64{}
		for _, item := range result.HazardTypes {
			got[item.HazardType] = item.Count
		}
		if got[domain.HazardSeismic] != 1 || got[analytics.CategoryTsunami] != 2 {
			t.Fatalf("counts = %v", got)
		}
		if result.AsOf.IsZero() {
			t.Fatal("as_of missing")
		}
		hourly, err := analytics.NewQueries(repository).Get(ctx, analytics.Filter{Since: at, Until: at.Add(time.Second), Bucket: analytics.BucketHour})
		if err != nil {
			t.Fatal(err)
		}
		var tsunami int64
		for _, bucket := range hourly.Buckets {
			if bucket.HazardType == analytics.CategoryTsunami {
				tsunami += bucket.Count
			}
		}
		if tsunami != 2 {
			t.Fatalf("tsunami bucket count = %d", tsunami)
		}
	})

	t.Run("failure attempts and inspection", func(t *testing.T) {
		id := "msg-" + run
		if err := repository.SaveIngestFailure(ctx, domain.IngestFailure{MessageID: id, Source: domain.SourcePVMBG, ReceivedAt: at, Reason: "unknown volcano_id", Retryable: true, Status: domain.FailureRetrying, Payload: []byte("bad\xff")}); err != nil {
			t.Fatal(err)
		}
		if err := repository.RecordIngestFailureAttempt(ctx, id, "still missing", true, false); err != nil {
			t.Fatal(err)
		}
		if err := repository.RecordIngestFailureAttempt(ctx, id, "exhausted", true, true); err != nil {
			t.Fatal(err)
		}
		// A parked message is outside the retry queue; further attempts are no-ops.
		if err := repository.RecordIngestFailureAttempt(ctx, id, "ignored", false, false); err != nil {
			t.Fatal(err)
		}
		item, err := repository.GetIngestFailure(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		// The original reason survives retries; the latest outcome is separate.
		if item.Attempts != 2 || item.Status != domain.FailureParked || !item.Retryable || item.Reason != "unknown volcano_id" || item.LastError != "exhausted" || string(item.Payload) != "bad\xff" || item.PayloadBytes != 4 {
			t.Fatalf("failure = %+v", item)
		}
		parked, err := repository.ListIngestFailures(ctx, domain.FailureFilter{Source: domain.SourcePVMBG, Status: domain.FailureParked, Limit: 1000})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, listed := range parked {
			if listed.Status != domain.FailureParked {
				t.Fatalf("status filter leaked %+v", listed)
			}
			if listed.MessageID == id {
				found = true
				if listed.Payload != nil || listed.PayloadBytes != 4 || listed.LastError != "exhausted" {
					t.Fatalf("listed = %+v, want metadata only", listed)
				}
			}
		}
		if !found {
			t.Fatal("parked failure missing from status-filtered listing")
		}
		retrying, err := repository.ListIngestFailures(ctx, domain.FailureFilter{Status: domain.FailureRetrying, Limit: 1000})
		if err != nil {
			t.Fatal(err)
		}
		for _, listed := range retrying {
			if listed.MessageID == id {
				t.Fatal("parked failure listed as retrying")
			}
		}
		if _, err := repository.GetIngestFailure(ctx, "missing-"+run); err != domain.ErrNotFound {
			t.Fatalf("missing failure err = %v, want ErrNotFound", err)
		}
	})

	t.Run("cluster metadata", func(t *testing.T) {
		if _, err := repository.UnavailableUnits(ctx); err != nil {
			t.Fatalf("UnavailableUnits: %v", err)
		}
		distribution, err := repository.HazardDistribution(ctx)
		if err != nil || distribution.Ranges == 0 || len(distribution.Nodes) == 0 {
			t.Fatalf("distribution = %+v err = %v", distribution, err)
		}
		locations, err := repository.HazardLocations(ctx, events[0].HazardID)
		if err != nil || len(locations) == 0 || locations[0].Role != "primary" {
			t.Fatalf("locations = %+v err = %v", locations, err)
		}
	})
}
