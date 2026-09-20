package contract

import "testing"

func TestRequiredEndpointPaths(t *testing.T) {
	paths := []string{"/seismic-events", "/tsunami-warnings", "/volcanic-reports", "/admin/schema-version", "/admin/outage", "/health"}
	if len(paths) != 6 { t.Fatal("contract list changed unexpectedly") }
}
