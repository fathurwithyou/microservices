package memory

import (
	"context"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

func TestStorePreventsDuplicateSourceRecord(t *testing.T) {
	store := New()
	event := domain.HazardEvent{HazardID: "h-1", Source: domain.SourceBMKG, SourceRefID: "source-1", HazardType: domain.HazardSeismic, Severity: domain.SeverityNormal, OccurredAt: time.Now(), Attributes: map[string]any{"magnitude": 5.1}}
	message := domain.OutboxMessage{HazardID: event.HazardID, CorrelationID: "test", RoutingKey: "hazard.event", Payload: []byte("event")}
	if err := store.PersistHazardAndEnqueue(context.Background(), event, message); err != nil {
		t.Fatal(err)
	}
	event.HazardID = "h-2"
	event.Attributes["magnitude"] = 5.5
	message.HazardID = event.HazardID
	if err := store.PersistHazardAndEnqueue(context.Background(), event, message); err != nil {
		t.Fatal(err)
	}
	items, err := store.ListHazards(context.Background(), hazards.ListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].HazardID != "h-1" {
		t.Fatalf("duplicate source record changed its canonical identity: %#v", items)
	}
}

func TestStoreAdvancesPollCursorMonotonically(t *testing.T) {
	store := New()
	first := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	second := first.Add(time.Minute)

	if err := store.AdvancePollCursor(context.Background(), domain.SourcePVMBG, second); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvancePollCursor(context.Background(), domain.SourcePVMBG, first); err != nil {
		t.Fatal(err)
	}
	cursor, err := store.GetPollCursor(context.Background(), domain.SourcePVMBG)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Equal(second) {
		t.Fatalf("cursor moved backwards: got %s, want %s", cursor, second)
	}
}

func TestStoreLoadsVolcanoReferenceCache(t *testing.T) {
	store := New()
	references, err := store.LoadVolcanoReferences(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reference, ok := references["merapi"]
	if !ok || reference.Name != "Gunung Merapi" {
		t.Fatalf("missing seeded volcano reference: %#v", references)
	}
	references["merapi"] = domain.VolcanoReference{Name: "mutated"}
	fresh, err := store.LoadVolcanoReferences(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fresh["merapi"].Name != "Gunung Merapi" {
		t.Fatal("volcano reference load leaked mutable store state")
	}
}

func TestStoreOutboxLeasePreventsConcurrentClaim(t *testing.T) {
	store := New()
	event := domain.HazardEvent{HazardID: "h-lease", Source: domain.SourceBMKG, SourceRefID: "source-lease", HazardType: domain.HazardSeismic, Severity: domain.SeverityNormal, OccurredAt: time.Now(), Attributes: map[string]any{}}
	message := domain.OutboxMessage{HazardID: event.HazardID, CorrelationID: "test", RoutingKey: "hazard.event", Payload: []byte("event")}
	if err := store.PersistHazardAndEnqueue(context.Background(), event, message); err != nil {
		t.Fatal(err)
	}

	claimed, err := store.ClaimPendingOutbox(context.Background(), 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].LeaseToken == "" || claimed[0].Attempts != 1 {
		t.Fatalf("unexpected first claim: %#v", claimed)
	}
	claimedAgain, err := store.ClaimPendingOutbox(context.Background(), 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimedAgain) != 0 {
		t.Fatalf("leased message was claimed twice: %#v", claimedAgain)
	}
	if err := store.MarkOutboxPublished(context.Background(), claimed[0].ID, claimed[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	remaining, err := store.ClaimPendingOutbox(context.Background(), 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("published message remained pending: %#v", remaining)
	}
}

func TestStoreLeaseExpiresAndCanBeClaimedAgain(t *testing.T) {
	store := New()
	event := domain.HazardEvent{HazardID: "h-lease", Source: domain.SourcePVMBG, SourceRefID: "r-lease", HazardType: domain.HazardVolcanic, Severity: domain.SeverityWaspada, OccurredAt: time.Now().UTC()}
	message := domain.OutboxMessage{HazardID: event.HazardID, CorrelationID: "test", RoutingKey: "hazard.event", Payload: []byte("event")}
	if err := store.PersistHazardAndEnqueue(context.Background(), event, message); err != nil {
		t.Fatal(err)
	}

	claimed, err := store.ClaimPendingOutbox(context.Background(), 1, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d messages, want 1", len(claimed))
	}
	claimedAgain, err := store.ClaimPendingOutbox(context.Background(), 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimedAgain) != 0 {
		t.Fatalf("claimed leased message before lease expiry: %#v", claimedAgain)
	}

	time.Sleep(30 * time.Millisecond)
	claimedAfterExpiry, err := store.ClaimPendingOutbox(context.Background(), 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimedAfterExpiry) != 1 || claimedAfterExpiry[0].Attempts != 2 {
		t.Fatalf("lease was not reclaimed with incremented attempts: %#v", claimedAfterExpiry)
	}
}
