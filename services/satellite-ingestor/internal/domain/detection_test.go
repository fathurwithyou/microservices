package domain

import (
	"errors"
	"testing"
	"time"
)

func TestDetectionKey(t *testing.T) {
	d := Detection{
		Satellite: "NOAA-20",
		AcqDate:   "2024-01-01",
		AcqTime:   "1230",
		Latitude:  -2.5,
		Longitude: 110.25,
	}
	want := "NOAA-20|2024-01-01|1230|-2.5|110.25"
	if got := d.Key(); got != want {
		t.Fatalf("Key() = %q, want %q", got, want)
	}
}

func TestDetectionKeyKeepsRawCoordinateText(t *testing.T) {
	d := Detection{
		Satellite:    "N20",
		AcqDate:      "2026-11-12",
		AcqTime:      "243",
		Latitude:     -2.5123,
		Longitude:    113.8044,
		RawLatitude:  "-2.51230",
		RawLongitude: "113.80440",
	}
	want := "N20|2026-11-12|243|-2.51230|113.80440"
	if got := d.Key(); got != want {
		t.Fatalf("Key() = %q, want %q", got, want)
	}
}

func TestDetectionIgnored(t *testing.T) {
	tests := []struct {
		name       string
		confidence Confidence
		want       bool
	}{
		{"low", ConfidenceLow, true},
		{"nominal", ConfidenceNominal, false},
		{"high", ConfidenceHigh, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Detection{Confidence: tt.confidence}
			if got := d.Ignored(); got != tt.want {
				t.Fatalf("Ignored() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDetectionTime(t *testing.T) {
	tests := []struct {
		name    string
		date    string
		clock   string
		want    time.Time
		wantErr bool
	}{
		{
			name:  "midnight",
			date:  "2024-01-01",
			clock: "0000",
			want:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "afternoon",
			date:  "2024-06-30",
			clock: "1435",
			want:  time.Date(2024, 6, 30, 14, 35, 0, 0, time.UTC),
		},
		{name: "bad date", date: "30-06-2024", clock: "1435", wantErr: true},
		{name: "unpadded", date: "2024-06-30", clock: "243", want: time.Date(2024, 6, 30, 2, 43, 0, 0, time.UTC)},
		{name: "colon", date: "2024-06-30", clock: "14:35", want: time.Date(2024, 6, 30, 14, 35, 0, 0, time.UTC)},
		{name: "bad clock", date: "2024-06-30", clock: "2575", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Detection{AcqDate: tt.date, AcqTime: tt.clock}
			got, err := d.Time()
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidDetectionTime) {
					t.Fatalf("Time() error = %v, want %v", err, ErrInvalidDetectionTime)
				}
				return
			}
			if err != nil {
				t.Fatalf("Time() error = %v", err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("Time() = %v, want %v", got, tt.want)
			}
		})
	}
}
