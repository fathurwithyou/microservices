package domain

import "testing"

func TestSeverityFor(t *testing.T) {
	tests := []struct {
		name           string
		detectionCount int
		totalFRP       float64
		want           Severity
	}{
		{"below all thresholds", 1, 0, SeverityWaspada},
		{"count siaga", 6, 0, SeveritySiaga},
		{"count just below siaga", 5, 29.9, SeverityWaspada},
		{"frp siaga", 2, 30, SeveritySiaga},
		{"count awas", 20, 0, SeverityAwas},
		{"frp awas", 2, 100, SeverityAwas},
		{"count just below awas", 19, 99.9, SeveritySiaga},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SeverityFor(tt.detectionCount, tt.totalFRP); got != tt.want {
				t.Fatalf("SeverityFor(%d, %v) = %s, want %s",
					tt.detectionCount, tt.totalFRP, got, tt.want)
			}
		})
	}
}

func TestNearestProvince(t *testing.T) {
	if got := NearestProvince(-2.2, 113.9); got != "Kalimantan Tengah" {
		t.Fatalf("NearestProvince = %q", got)
	}
	if got := NearestProvince(0.5, 101.5); got != "Riau" {
		t.Fatalf("NearestProvince = %q", got)
	}
	if got := NearestProvince(-7.25, 112.75); got != "Jawa Timur" {
		t.Fatalf("NearestProvince = %q", got)
	}
	if got := NearestProvince(-8.45, 115.15); got != "Bali" {
		t.Fatalf("NearestProvince = %q", got)
	}
	if got := NearestProvince(-4.05, 138.10); got != "Papua" {
		t.Fatalf("NearestProvince = %q", got)
	}
}
