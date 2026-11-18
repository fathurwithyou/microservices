package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
)

func TestSaveQuarantineSpansChunks(t *testing.T) {
	databaseURL := os.Getenv("COASTAL_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("COASTAL_DATABASE_URL not set")
	}
	ctx := context.Background()
	state, err := New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(state.Close)
	prefix := "chunks-" + time.Now().UTC().Format("20060102150405.000000000")
	t.Cleanup(func() {
		_, _ = state.pool.Exec(context.Background(), `DELETE FROM coastal_quarantine WHERE message_id LIKE $1`, prefix+"%")
	})

	// Two full chunks and a partial one.
	records := make([]domain.QuarantineRecord, 2*quarantineChunk+203)
	for i := range records {
		records[i] = domain.QuarantineRecord{
			MessageID: fmt.Sprintf("%s-%05d", prefix, i), Source: domain.SourceCoastalSensor,
			ReceivedAt: time.Now().UTC(), Reason: domain.ReasonInvalidLatitude, Payload: []byte(`{}`),
		}
	}
	if err := state.SaveQuarantine(ctx, records); err != nil {
		t.Fatalf("save quarantine: %v", err)
	}
	var stored int
	if err := state.pool.QueryRow(ctx, `SELECT count(DISTINCT message_id) FROM coastal_quarantine WHERE message_id LIKE $1`, prefix+"%").Scan(&stored); err != nil {
		t.Fatalf("count quarantine: %v", err)
	}
	if stored != len(records) {
		t.Fatalf("quarantine rows = %d, want %d", stored, len(records))
	}
}

func TestStateRoundTrip(t *testing.T) {
	databaseURL := os.Getenv("COASTAL_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("COASTAL_DATABASE_URL not set")
	}
	ctx := context.Background()
	state, err := New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(state.Close)

	prefix := "integration-" + time.Now().UTC().Format("20060102150405.000000000")
	t.Cleanup(func() {
		_, _ = state.pool.Exec(context.Background(), `DELETE FROM coastal_quarantine WHERE message_id LIKE $1`, prefix+"%")
	})

	receivedAt := time.Now().UTC().Truncate(time.Microsecond)
	records := []domain.QuarantineRecord{
		{
			MessageID: prefix + "-a", Source: domain.SourceCoastalSensor,
			ReceivedAt: receivedAt, Reason: domain.ReasonInvalidLatitude,
			Retryable: false, Payload: []byte(`{"id":"a"}`),
		},
		{
			MessageID: prefix + "-b", Source: domain.SourceCoastalSensor,
			ReceivedAt: receivedAt.Add(time.Second), Reason: domain.ReasonOccurredAtFuture,
			Retryable: true,
		},
	}
	if err := state.SaveQuarantine(ctx, records); err != nil {
		t.Fatalf("save quarantine: %v", err)
	}

	var stored int
	if err := state.pool.QueryRow(ctx, `SELECT count(*) FROM coastal_quarantine WHERE message_id LIKE $1`, prefix+"%").Scan(&stored); err != nil {
		t.Fatalf("count quarantine: %v", err)
	}
	if stored != len(records) {
		t.Fatalf("quarantine rows = %d, want %d", stored, len(records))
	}
	var reason domain.QuarantineReason
	var retryable bool
	var payload []byte
	if err := state.pool.QueryRow(ctx, `SELECT reason, retryable, payload FROM coastal_quarantine WHERE message_id = $1`, prefix+"-a").Scan(&reason, &retryable, &payload); err != nil {
		t.Fatalf("read quarantine: %v", err)
	}
	if reason != domain.ReasonInvalidLatitude || retryable || string(payload) != `{"id":"a"}` {
		t.Fatalf("quarantine row = %q retryable=%v payload=%q", reason, retryable, payload)
	}

	capturedAt := receivedAt.Add(2 * time.Minute)
	readings := []domain.Reading{{
		ID: prefix, SensorID: "sensor-1",
		OccurredAt: receivedAt.Add(-time.Minute), ReceivedAt: receivedAt,
		Latitude: -6.12, Longitude: 106.88, Severity: domain.SeverityWaspada,
		Critical: true, Payload: []byte(`{"raw":true}`),
	}}
	if err := state.SaveLastGood(ctx, readings, capturedAt); err != nil {
		t.Fatalf("save last good: %v", err)
	}

	got, gotAt, err := state.LastGood(ctx)
	if err != nil {
		t.Fatalf("load last good: %v", err)
	}
	if !gotAt.Equal(capturedAt) {
		t.Fatalf("captured_at = %s, want %s", gotAt, capturedAt)
	}
	if len(got) != 1 {
		t.Fatalf("readings count = %d, want 1", len(got))
	}
	want, have := readings[0], got[0]
	if have.ID != want.ID || have.SensorID != want.SensorID ||
		!have.OccurredAt.Equal(want.OccurredAt) || !have.ReceivedAt.Equal(want.ReceivedAt) ||
		have.Latitude != want.Latitude || have.Longitude != want.Longitude ||
		have.Severity != want.Severity || have.Critical != want.Critical ||
		string(have.Payload) != string(want.Payload) {
		t.Fatalf("reading = %+v, want %+v", have, want)
	}
}
