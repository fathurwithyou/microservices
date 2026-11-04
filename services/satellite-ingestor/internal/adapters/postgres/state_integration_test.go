package postgres

import (
	"context"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

// TestStateRoundTrip exercises the durable adapter against a real database.
// It skips unless SATELLITE_DATABASE_URL is set.
func TestStateRoundTrip(t *testing.T) {
	databaseURL := os.Getenv("SATELLITE_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("SATELLITE_DATABASE_URL not set")
	}
	ctx := context.Background()
	state, err := New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		_ = state.SaveDetections(ctx, nil)
		_ = state.SaveFires(ctx, nil)
		state.Close()
	})

	if err := state.SaveOffset(ctx, 42); err != nil {
		t.Fatalf("save offset: %v", err)
	}
	offset, err := state.Offset(ctx)
	if err != nil {
		t.Fatalf("read offset: %v", err)
	}
	if offset != 42 {
		t.Fatalf("offset = %d, want 42", offset)
	}

	detections := []domain.Detection{
		{
			Satellite:  "NPP-375",
			AcqDate:    "2026-11-01",
			AcqTime:    "0130",
			Latitude:   -6.2,
			Longitude:  106.8,
			Confidence: domain.ConfidenceNominal,
			FRP:        12.5,
		},
		{
			Satellite:  "MODIS",
			AcqDate:    "2026-11-01",
			AcqTime:    "0140",
			Latitude:   -6.21,
			Longitude:  106.81,
			Confidence: domain.ConfidenceHigh,
			FRP:        30,
		},
	}
	if err := state.SaveDetections(ctx, detections); err != nil {
		t.Fatalf("save detections: %v", err)
	}
	gotDetections, err := state.Detections(ctx)
	if err != nil {
		t.Fatalf("read detections: %v", err)
	}
	sort.Slice(gotDetections, func(i, j int) bool { return gotDetections[i].Key() < gotDetections[j].Key() })
	wantDetections := append([]domain.Detection(nil), detections...)
	sort.Slice(wantDetections, func(i, j int) bool { return wantDetections[i].Key() < wantDetections[j].Key() })
	if !reflect.DeepEqual(gotDetections, wantDetections) {
		t.Fatalf("detections = %+v, want %+v", gotDetections, wantDetections)
	}

	keys := []string{detections[0].Key(), detections[1].Key()}
	sort.Strings(keys)
	fires := []domain.Fire{
		{
			SourceRefID:    "fire-abc",
			OccurredAt:     time.Date(2026, 11, 1, 1, 30, 0, 0, time.UTC),
			AreaName:       "Unknown",
			Latitude:       -6.205,
			Longitude:      106.805,
			DetectionCount: 2,
			Satellites:     []domain.Satellite{"MODIS", "NPP-375"},
			MaxConfidence:  domain.ConfidenceHigh,
			TotalFRP:       42.5,
			DetectionKeys:  keys,
			Severity:       domain.SeveritySiaga,
		},
	}
	if err := state.SaveFires(ctx, fires); err != nil {
		t.Fatalf("save fires: %v", err)
	}
	gotFires, err := state.Fires(ctx)
	if err != nil {
		t.Fatalf("read fires: %v", err)
	}
	normalizeFires := func(in []domain.Fire) {
		for i := range in {
			in[i].OccurredAt = in[i].OccurredAt.UTC().Truncate(time.Microsecond)
		}
	}
	normalizeFires(gotFires)
	wantFires := append([]domain.Fire(nil), fires...)
	normalizeFires(wantFires)
	if !reflect.DeepEqual(gotFires, wantFires) {
		t.Fatalf("fires = %+v, want %+v", gotFires, wantFires)
	}

	if err := state.SaveDetections(ctx, detections[:1]); err != nil {
		t.Fatalf("replace detections: %v", err)
	}
	if got, err := state.Detections(ctx); err != nil {
		t.Fatalf("read replaced detections: %v", err)
	} else if len(got) != 1 {
		t.Fatalf("detections after replace = %d, want 1", len(got))
	}
	if err := state.SaveFires(ctx, nil); err != nil {
		t.Fatalf("replace fires: %v", err)
	}
	if got, err := state.Fires(ctx); err != nil {
		t.Fatalf("read replaced fires: %v", err)
	} else if len(got) != 0 {
		t.Fatalf("fires after replace = %d, want 0", len(got))
	}
}
