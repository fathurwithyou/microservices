package failures

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

type fakeRepository struct {
	items     []domain.IngestFailure
	recovered []string
}

func (f *fakeRepository) ListIngestFailures(context.Context, domain.Source, int, time.Time, string) ([]domain.IngestFailure, error) {
	return f.items, nil
}
func (f *fakeRepository) MarkIngestFailureRecovered(_ context.Context, id string) error {
	f.recovered = append(f.recovered, id)
	return nil
}

type fakeSubmitter struct{ submitted []domain.HazardEvent }

func (f *fakeSubmitter) Submit(_ context.Context, event domain.HazardEvent, _ string) (string, error) {
	f.submitted = append(f.submitted, event)
	return event.HazardID, nil
}

func TestReplayMapsAndMarksRecovered(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{
		"report_id": "r-1", "volcano_id": "merapi", "alert_level": "Siaga",
		"eruption_count_24h": 2, "ash_column_height_m": 400, "reported_at": "2026-11-12T02:43:00Z",
	})
	repository := &fakeRepository{items: []domain.IngestFailure{{MessageID: "m-1", Source: domain.SourcePVMBG, Payload: payload}}}
	submitter := &fakeSubmitter{}
	replayer, err := New(Config{
		Repository: repository,
		Submitter:  submitter,
		Volcanoes:  map[string]domain.VolcanoReference{"merapi": {Name: "Gunung Merapi", Latitude: -7.54, Longitude: 110.44}},
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:        func() time.Time { return time.Now().UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := replayer.Replay(context.Background(), domain.SourcePVMBG, 10)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 || len(submitter.submitted) != 1 || len(repository.recovered) != 1 {
		t.Fatalf("recovered=%d submitted=%d marked=%d", recovered, len(submitter.submitted), len(repository.recovered))
	}
}

func TestReplayStopsAtFirstFailure(t *testing.T) {
	valid, _ := json.Marshal(map[string]any{
		"report_id": "r-2", "volcano_id": "merapi", "alert_level": "Siaga",
		"eruption_count_24h": 2, "ash_column_height_m": 400, "reported_at": "2026-11-12T02:43:00Z",
	})
	repository := &fakeRepository{items: []domain.IngestFailure{
		{MessageID: "m-bad", Source: domain.SourcePVMBG, Payload: []byte(`{"truncated"`)},
		{MessageID: "m-good", Source: domain.SourcePVMBG, Payload: valid},
	}}
	submitter := &fakeSubmitter{}
	replayer, err := New(Config{
		Repository: repository,
		Submitter:  submitter,
		Volcanoes:  map[string]domain.VolcanoReference{"merapi": {Name: "Gunung Merapi"}},
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := replayer.Replay(context.Background(), domain.SourcePVMBG, 10)
	if err == nil {
		t.Fatal("Replay error = nil, want first failure")
	}
	if recovered != 0 || len(submitter.submitted) != 0 || len(repository.recovered) != 0 {
		t.Fatalf("recovered=%d submitted=%d marked=%d", recovered, len(submitter.submitted), len(repository.recovered))
	}
}
