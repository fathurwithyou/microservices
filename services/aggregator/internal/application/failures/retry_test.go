package failures

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type attempt struct {
	id              string
	retryable, park bool
}

type recordingRepository struct {
	fakeRepository
	attempts []attempt
}

func (r *recordingRepository) RecordIngestFailureAttempt(_ context.Context, id, _ string, retryable, park bool) error {
	r.attempts = append(r.attempts, attempt{id: id, retryable: retryable, park: park})
	return nil
}

func volcanicPayload(t *testing.T, reportID, volcanoID string) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"report_id": reportID, "volcano_id": volcanoID, "alert_level": "Siaga",
		"eruption_count_24h": 2, "ash_column_height_m": 400, "reported_at": "2026-11-12T02:43:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestRetryPendingRecoversOnceReferenceArrives(t *testing.T) {
	repository := &recordingRepository{fakeRepository: fakeRepository{items: []domain.IngestFailure{
		{MessageID: "m-parked", Source: domain.SourcePVMBG, Status: domain.FailureParked, Payload: []byte(`{"level":"x"}`)},
		{MessageID: "m-retry", Source: domain.SourcePVMBG, Status: domain.FailureRetrying, Retryable: true, Payload: volcanicPayload(t, "r-1", "kelud")},
	}}}
	references := map[string]domain.VolcanoReference{"merapi": {Name: "Gunung Merapi"}}
	submitter := &fakeSubmitter{}
	replayer, err := New(Config{
		Repository: repository,
		Submitter:  submitter,
		References: func(context.Context) (map[string]domain.VolcanoReference, error) { return references, nil },
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := replayer.RetryPending(context.Background(), repository, 5)
	if err != nil {
		t.Fatal(err)
	}
	if result.Retried != 1 || len(repository.attempts) != 1 || repository.attempts[0].park || !repository.attempts[0].retryable {
		t.Fatalf("first pass result=%+v attempts=%+v", result, repository.attempts)
	}

	// An operator adds the missing reference row; no code change, no replay.
	references = map[string]domain.VolcanoReference{"merapi": {Name: "Gunung Merapi"}, "kelud": {Name: "Gunung Kelud", Latitude: -7.93, Longitude: 112.31}}
	result, err = replayer.RetryPending(context.Background(), repository, 5)
	if err != nil {
		t.Fatal(err)
	}
	if result.Recovered != 1 || len(submitter.submitted) != 1 || submitter.submitted[0].AreaName != "Gunung Kelud" {
		t.Fatalf("second pass result=%+v submitted=%+v", result, submitter.submitted)
	}
	for _, id := range repository.recovered {
		if id == "m-parked" {
			t.Fatal("automatic retry touched a parked message")
		}
	}
}

func TestRetryPendingParksAfterMaxAttempts(t *testing.T) {
	repository := &recordingRepository{fakeRepository: fakeRepository{items: []domain.IngestFailure{
		{MessageID: "m-retry", Source: domain.SourcePVMBG, Status: domain.FailureRetrying, Retryable: true, Attempts: 4, Payload: volcanicPayload(t, "r-1", "kelud")},
	}}}
	replayer, err := New(Config{Repository: repository, Submitter: &fakeSubmitter{}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	result, err := replayer.RetryPending(context.Background(), repository, 5)
	if err != nil {
		t.Fatal(err)
	}
	if result.Parked != 1 || !repository.attempts[0].park || !repository.attempts[0].retryable {
		t.Fatalf("result=%+v attempts=%+v", result, repository.attempts)
	}
}

// A replica whose lease lapses mid-pass must stop before its next write.
func TestRetryPassStopsWhenFenceFails(t *testing.T) {
	repository := &recordingRepository{fakeRepository: fakeRepository{items: []domain.IngestFailure{
		{MessageID: "m-1", Source: domain.SourcePVMBG, Status: domain.FailureRetrying, Payload: volcanicPayload(t, "r-1", "merapi")},
		{MessageID: "m-2", Source: domain.SourcePVMBG, Status: domain.FailureRetrying, Payload: volcanicPayload(t, "r-2", "merapi")},
	}}}
	replayer, err := New(Config{
		Repository: repository,
		Submitter:  &fakeSubmitter{},
		Volcanoes:  map[string]domain.VolcanoReference{"merapi": {Name: "Gunung Merapi"}},
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	checks := 0
	fence := func(context.Context) bool {
		checks++
		return checks == 1
	}
	result, err := replayer.retryPending(context.Background(), repository, 5, fence)
	if err != nil {
		t.Fatal(err)
	}
	if result.Recovered != 1 || len(repository.recovered) != 1 || repository.recovered[0] != "m-1" {
		t.Fatalf("result=%+v recovered=%v, want only m-1 before the fence closed", result, repository.recovered)
	}
}
