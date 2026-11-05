package application

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"reflect"
	"testing"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

func det(satellite string, clock string, lat, lon float64, conf domain.Confidence, frp float64) domain.Detection {
	return domain.Detection{
		Satellite:  domain.Satellite(satellite),
		AcqDate:    "2024-01-01",
		AcqTime:    clock,
		Latitude:   lat,
		Longitude:  lon,
		Confidence: conf,
		FRP:        frp,
	}
}

// offsetNorth returns the latitude reached by moving km north from lat.
func offsetNorth(lat, km float64) float64 {
	return lat + km/earthRadiusKm*180/math.Pi
}

func TestClusterBoundaries(t *testing.T) {
	const clock = "0000"
	tests := []struct {
		name        string
		detections  []domain.Detection
		wantFires   int
		wantSingles int
	}{
		{
			name: "distance just under limit connects",
			detections: []domain.Detection{
				det("NOAA-20", clock, -2.0, 110.0, domain.ConfidenceNominal, 1),
				det("NOAA-20", clock, offsetNorth(-2.0, 0.999), 110.0, domain.ConfidenceNominal, 1),
			},
			wantFires: 1,
		},
		{
			name: "distance just over limit does not connect",
			detections: []domain.Detection{
				det("NOAA-20", clock, -2.0, 110.0, domain.ConfidenceNominal, 1),
				det("NOAA-20", clock, offsetNorth(-2.0, 1.001), 110.0, domain.ConfidenceNominal, 1),
			},
			wantSingles: 2,
		},
		{
			name: "time exactly at limit connects",
			detections: []domain.Detection{
				det("NOAA-20", "0000", -2.0, 110.0, domain.ConfidenceNominal, 1),
				det("NOAA-20", "0600", -2.0, 110.0, domain.ConfidenceNominal, 1),
			},
			wantFires: 1,
		},
		{
			name: "time over limit does not connect",
			detections: []domain.Detection{
				det("NOAA-20", "0000", -2.0, 110.0, domain.ConfidenceNominal, 1),
				det("NOAA-20", "0601", -2.0, 110.0, domain.ConfidenceNominal, 1),
			},
			wantSingles: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fires, singles := Cluster(tt.detections)
			if len(fires) != tt.wantFires {
				t.Fatalf("fires = %d, want %d", len(fires), tt.wantFires)
			}
			if singles != tt.wantSingles {
				t.Fatalf("singles = %d, want %d", singles, tt.wantSingles)
			}
		})
	}
}

func TestClusterFiltersLowConfidence(t *testing.T) {
	base := det("NOAA-20", "0000", -2.0, 110.0, domain.ConfidenceNominal, 1)
	low := det("NOAA-20", "0000", -2.0, 110.0, domain.ConfidenceLow, 1)

	t.Run("low does not join and is not a single", func(t *testing.T) {
		fires, singles := Cluster([]domain.Detection{base, low})
		if len(fires) != 0 || singles != 1 {
			t.Fatalf("fires = %d, singles = %d, want 0 and 1", len(fires), singles)
		}
	})

	t.Run("low only input yields nothing", func(t *testing.T) {
		fires, singles := Cluster([]domain.Detection{low})
		if len(fires) != 0 || singles != 0 {
			t.Fatalf("fires = %d, singles = %d, want 0 and 0", len(fires), singles)
		}
	})
}

func TestClusterConnectsAcrossGridCells(t *testing.T) {
	detections := []domain.Detection{
		det("NOAA-20", "0000", 0.0199, 0.0199, domain.ConfidenceNominal, 1),
		det("NOAA-20", "0000", offsetNorth(0.0199, 0.9), 0.0199, domain.ConfidenceNominal, 1),
	}
	fires, singles := Cluster(detections)
	if len(fires) != 1 || singles != 0 {
		t.Fatalf("fires = %d, singles = %d, want 1 and 0", len(fires), singles)
	}
}

func TestClusterTransitiveChain(t *testing.T) {
	detections := []domain.Detection{
		det("NOAA-20", "0000", -2.0, 110.0, domain.ConfidenceNominal, 1),
		det("NOAA-20", "0000", offsetNorth(-2.0, 0.8), 110.0, domain.ConfidenceNominal, 1),
		det("NOAA-20", "0000", offsetNorth(-2.0, 1.6), 110.0, domain.ConfidenceNominal, 1),
	}
	fires, singles := Cluster(detections)
	if len(fires) != 1 || singles != 0 {
		t.Fatalf("fires = %d, singles = %d, want 1 and 0", len(fires), singles)
	}
	if fires[0].DetectionCount != 3 {
		t.Fatalf("DetectionCount = %d, want 3", fires[0].DetectionCount)
	}
}

func TestClusterCrossSatellite(t *testing.T) {
	detections := []domain.Detection{
		det("SUOMI-NPP", "0000", -2.0, 110.0, domain.ConfidenceNominal, 10),
		det("NOAA-20", "0000", offsetNorth(-2.0, 0.5), 110.0, domain.ConfidenceHigh, 20),
	}
	fires, singles := Cluster(detections)
	if len(fires) != 1 || singles != 0 {
		t.Fatalf("fires = %d, singles = %d, want 1 and 0", len(fires), singles)
	}
	fire := fires[0]
	wantSatellites := []domain.Satellite{"NOAA-20", "SUOMI-NPP"}
	if !reflect.DeepEqual(fire.Satellites, wantSatellites) {
		t.Fatalf("Satellites = %v, want %v", fire.Satellites, wantSatellites)
	}
	if fire.MaxConfidence != domain.ConfidenceHigh {
		t.Fatalf("MaxConfidence = %s, want %s", fire.MaxConfidence, domain.ConfidenceHigh)
	}
	if fire.TotalFRP != 30 {
		t.Fatalf("TotalFRP = %v, want 30", fire.TotalFRP)
	}
}

func TestClusterSingleDetectionCounted(t *testing.T) {
	fires, singles := Cluster([]domain.Detection{
		det("NOAA-20", "0000", -2.0, 110.0, domain.ConfidenceHigh, 5),
	})
	if len(fires) != 0 || singles != 1 {
		t.Fatalf("fires = %d, singles = %d, want 0 and 1", len(fires), singles)
	}
}

func TestClusterDeterministicIdentity(t *testing.T) {
	detections := []domain.Detection{
		det("NOAA-20", "0300", offsetNorth(-2.0, 0.4), 110.0, domain.ConfidenceNominal, 5),
		det("NOAA-20", "0100", -2.0, 110.0, domain.ConfidenceHigh, 5),
		det("NOAA-20", "0200", offsetNorth(-2.0, 0.2), 110.0, domain.ConfidenceNominal, 5),
	}
	first, _ := Cluster(detections)

	reversed := make([]domain.Detection, len(detections))
	for i, d := range detections {
		reversed[len(detections)-1-i] = d
	}
	second, _ := Cluster(reversed)

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("fires = %d and %d, want 1 and 1", len(first), len(second))
	}
	if first[0].SourceRefID != second[0].SourceRefID {
		t.Fatalf("SourceRefID differs: %q vs %q", first[0].SourceRefID, second[0].SourceRefID)
	}

	sum := sha256.Sum256([]byte(detections[1].Key()))
	wantID := "fire-" + hex.EncodeToString(sum[:])
	if first[0].SourceRefID != wantID {
		t.Fatalf("SourceRefID = %q, want %q", first[0].SourceRefID, wantID)
	}
	wantOccurredAt, err := detections[1].Time()
	if err != nil {
		t.Fatalf("Time() error = %v", err)
	}
	if !first[0].OccurredAt.Equal(wantOccurredAt) {
		t.Fatalf("OccurredAt = %v, want %v", first[0].OccurredAt, wantOccurredAt)
	}
}
