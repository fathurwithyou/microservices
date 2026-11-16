package grpcadapter

import (
	"encoding/json"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestAggregatorServiceConfigParses(t *testing.T) {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(aggregatorServiceConfig), &parsed); err != nil {
		t.Fatalf("service config is not JSON: %v", err)
	}
	// grpc.NewClient validates the default service config, including the
	// balancer name and retry policy.
	conn, err := grpc.NewClient("dns:///aggregator-grpc:9090", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultServiceConfig(aggregatorServiceConfig))
	if err != nil {
		t.Fatalf("service config rejected: %v", err)
	}
	_ = conn.Close()
}
