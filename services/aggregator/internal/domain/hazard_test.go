package domain

import "testing"

func TestSeverityFromMagnitude(t *testing.T) {
	for _, test := range []struct {
		magnitude float64
		expected  Severity
	}{{4.9, SeverityNormal}, {5.0, SeverityWaspada}, {6.5, SeveritySiaga}} {
		if got := SeverityFromMagnitude(test.magnitude); got != test.expected {
			t.Fatalf("magnitude %v: got %s, want %s", test.magnitude, got, test.expected)
		}
	}
}
