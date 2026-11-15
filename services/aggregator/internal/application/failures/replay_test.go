package failures

import (
	"context"
	"encoding/json"
	"fmt"
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

// ListIngestFailures pages through items in slice order, metadata only,
// like the real stores. Items without a status count as parked.
func (f *fakeRepository) ListIngestFailures(_ context.Context, filter domain.FailureFilter) ([]domain.IngestFailure, error) {
	var out []domain.IngestFailure
	started := filter.AfterID == ""
	for _, item := range f.items {
		if !started {
			started = item.MessageID == filter.AfterID
			continue
		}
		item.Status = statusOf(item)
		if filter.Status != "" && item.Status != filter.Status {
			continue
		}
		item.PayloadBytes, item.Payload = len(item.Payload), nil
		out = append(out, item)
		if len(out) == filter.Limit {
			break
		}
	}
	return out, nil
}

func (f *fakeRepository) GetIngestFailure(_ context.Context, id string) (domain.IngestFailure, error) {
	for _, item := range f.items {
		if item.MessageID == id {
			item.Status = statusOf(item)
			return item, nil
		}
	}
	return domain.IngestFailure{}, domain.ErrNotFound
}

func statusOf(item domain.IngestFailure) domain.FailureStatus {
	if item.Status == "" {
		return domain.FailureParked
	}
	return item.Status
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
		{MessageID: "m-bad", Source: domain.SourcePVMBG, Payload: []byte(`{"report_id":"r-1","level":"Siaga"}`)},
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

func TestReplaySkipsUndecodableBodiesWithoutBlocking(t *testing.T) {
	valid, _ := json.Marshal(map[string]any{
		"report_id": "r-3", "volcano_id": "merapi", "alert_level": "Siaga",
		"eruption_count_24h": 2, "ash_column_height_m": 400, "reported_at": "2026-11-12T02:43:00Z",
	})
	repository := &fakeRepository{items: []domain.IngestFailure{
		{MessageID: "m-truncated", Source: domain.SourcePVMBG, Payload: []byte(`[{"report_id":`)},
		{MessageID: "m-utf8", Source: domain.SourcePVMBG, Payload: []byte("{\"report_id\":\"x\xff\"}")},
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
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 || len(repository.recovered) != 1 || repository.recovered[0] != "m-good" {
		t.Fatalf("recovered=%d marked=%v", recovered, repository.recovered)
	}
}

// A replay page full of undecodable bodies must not hide the recoverable
// message behind it, and skipped bodies do not count toward limit.
func TestReplayPagesPastUndecodableBacklog(t *testing.T) {
	items := make([]domain.IngestFailure, 0, pageSize+51)
	for i := 0; i < pageSize+50; i++ {
		items = append(items, domain.IngestFailure{MessageID: fmt.Sprintf("m-%04d-bad", i), Source: domain.SourcePVMBG, Payload: []byte(`[{"report_id":`)})
	}
	valid, _ := json.Marshal(map[string]any{
		"report_id": "r-late", "volcano_id": "merapi", "alert_level": "Siaga",
		"eruption_count_24h": 2, "ash_column_height_m": 400, "reported_at": "2026-11-12T02:43:00Z",
	})
	items = append(items, domain.IngestFailure{MessageID: "m-9999-good", Source: domain.SourcePVMBG, Payload: valid})
	repository := &fakeRepository{items: items}
	replayer, err := New(Config{
		Repository: repository,
		Submitter:  &fakeSubmitter{},
		Volcanoes:  map[string]domain.VolcanoReference{"merapi": {Name: "Gunung Merapi"}},
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := replayer.Replay(context.Background(), domain.SourcePVMBG, 1)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != 1 || len(repository.recovered) != 1 || repository.recovered[0] != "m-9999-good" {
		t.Fatalf("recovered=%d marked=%v", recovered, repository.recovered)
	}
}
