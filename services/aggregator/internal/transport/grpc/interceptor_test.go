package grpctransport

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestUnaryObservabilityLogsRequestAndPropagatesCorrelationID(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	stream := &testServerTransportStream{}
	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-correlation-id", "corr-aggregator-123"))
	ctx = grpc.NewContextWithServerTransportStream(ctx, stream)

	handler := unaryObservabilityInterceptor(logger)
	response, err := handler(ctx, "request", &grpc.UnaryServerInfo{FullMethod: "/bnpb.aggregator.Aggregator/ListHazards"}, func(ctx context.Context, request any) (any, error) {
		if got := ctx.Value(correlationIDContextKey{}); got != "corr-aggregator-123" {
			t.Fatalf("correlation ID in context = %v, want corr-aggregator-123", got)
		}
		return "response", nil
	})
	if err != nil {
		t.Fatalf("observability interceptor returned error: %v", err)
	}
	if response != "response" {
		t.Fatalf("response = %v, want response", response)
	}
	if got := stream.header.Get("x-correlation-id"); len(got) != 1 || got[0] != "corr-aggregator-123" {
		t.Fatalf("response correlation ID = %v, want corr-aggregator-123", got)
	}
	if got := stream.header.Get("correlation-id"); len(got) != 1 || got[0] != "corr-aggregator-123" {
		t.Fatalf("standard response correlation ID = %v, want corr-aggregator-123", got)
	}

	assertLogFields(t, logs.Bytes(), map[string]any{
		"service":        "aggregator",
		"rpc_method":     "/bnpb.aggregator.Aggregator/ListHazards",
		"correlation_id": "corr-aggregator-123",
		"status":         "OK",
		"status_code":    float64(codes.OK),
		"deadline":       deadline.UTC().Format(time.RFC3339Nano),
	})
	if !strings.Contains(logs.String(), "latency_ms") {
		t.Fatal("log does not contain latency_ms")
	}
}

func TestUnaryObservabilityLogsFailureAndRecoversPanic(t *testing.T) {
	var logs bytes.Buffer
	handler := unaryObservabilityInterceptor(slog.New(slog.NewJSONHandler(&logs, nil)))
	_, err := handler(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/bnpb.aggregator.Aggregator/GetHazard"}, func(context.Context, any) (any, error) {
		return nil, status.Error(codes.NotFound, "missing")
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("handler error code = %s, want NotFound", status.Code(err))
	}
	assertLogFields(t, logs.Bytes(), map[string]any{"status": "NotFound", "status_code": float64(codes.NotFound)})

	logs.Reset()
	_, err = handler(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/bnpb.aggregator.Aggregator/HealthCheck"}, func(context.Context, any) (any, error) {
		panic("boom")
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("panic error code = %s, want Internal", status.Code(err))
	}
	if !strings.Contains(logs.String(), "grpc panic recovered") {
		t.Fatal("panic log entry is missing")
	}
}

func TestUnaryAuthInterceptor(t *testing.T) {
	interceptor := unaryAuthInterceptor("service-secret")
	info := &grpc.UnaryServerInfo{FullMethod: "/bnpb.aggregator.Aggregator/ListHazards"}
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	validContext := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer service-secret"))
	response, err := interceptor(validContext, nil, info, handler)
	if err != nil || response != "ok" {
		t.Fatalf("valid token result = %v, %v; want ok, nil", response, err)
	}

	_, err = interceptor(context.Background(), nil, info, handler)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token code = %s, want Unauthenticated", status.Code(err))
	}
}

type testServerTransportStream struct {
	header metadata.MD
}

func (s *testServerTransportStream) Method() string {
	return "/bnpb.aggregator.Aggregator/ListHazards"
}

func (s *testServerTransportStream) SetHeader(header metadata.MD) error {
	s.header = header.Copy()
	return nil
}

func (*testServerTransportStream) SendHeader(metadata.MD) error {
	return nil
}

func (*testServerTransportStream) SetTrailer(metadata.MD) error {
	return nil
}

func assertLogFields(t *testing.T, data []byte, want map[string]any) {
	t.Helper()
	var record map[string]any
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var next map[string]any
		if err := json.Unmarshal(line, &next); err != nil {
			t.Fatalf("decode structured log: %v; log=%s", err, data)
		}
		record = next
	}
	for key, expected := range want {
		if record[key] != expected {
			t.Errorf("log field %s = %v, want %v", key, record[key], expected)
		}
	}
}
