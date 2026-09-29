package grpckit

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/pkg/correlation"
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
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("x-correlation-id", "corr-auth-123"))
	ctx = grpc.NewContextWithServerTransportStream(ctx, stream)

	handler := UnaryObservability(Config{Service: "auth", Logger: logger})
	response, err := handler(ctx, "request", &grpc.UnaryServerInfo{FullMethod: "/bnpb.auth.Auth/IssueToken"}, func(ctx context.Context, request any) (any, error) {
		if got := correlation.FromContext(ctx); got != "corr-auth-123" {
			t.Fatalf("correlation ID in context = %v, want corr-auth-123", got)
		}
		return "response", nil
	})
	if err != nil {
		t.Fatalf("observability interceptor returned error: %v", err)
	}
	if response != "response" {
		t.Fatalf("response = %v, want response", response)
	}
	if got := stream.header.Get("x-correlation-id"); len(got) != 1 || got[0] != "corr-auth-123" {
		t.Fatalf("response correlation ID = %v, want corr-auth-123", got)
	}
	if got := stream.header.Get("correlation-id"); len(got) != 1 || got[0] != "corr-auth-123" {
		t.Fatalf("standard response correlation ID = %v, want corr-auth-123", got)
	}

	assertLogFields(t, logs.Bytes(), map[string]any{
		"service":        "auth",
		"rpc_method":     "/bnpb.auth.Auth/IssueToken",
		"correlation_id": "corr-auth-123",
		"status":         "OK",
		"status_code":    float64(codes.OK),
		"deadline":       deadline.UTC().Format(time.RFC3339Nano),
	})
	if !strings.Contains(logs.String(), "latency_ms") {
		t.Fatal("log does not contain latency_ms")
	}
}

func TestUnaryObservabilityRecoversPanic(t *testing.T) {
	var logs bytes.Buffer
	handler := UnaryObservability(Config{Service: "auth", Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	_, err := handler(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/bnpb.auth.Auth/HealthCheck"}, func(context.Context, any) (any, error) {
		panic("boom")
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("panic error code = %s, want Internal", status.Code(err))
	}
	if !strings.Contains(logs.String(), "grpc panic recovered") {
		t.Fatal("panic log entry is missing")
	}
	assertLogFields(t, logs.Bytes(), map[string]any{"status": "Internal", "status_code": float64(codes.Internal)})
}

func TestUnaryAuthInterceptor(t *testing.T) {
	interceptor := UnaryAuth(AuthConfig{Token: "service-secret"})
	info := &grpc.UnaryServerInfo{FullMethod: "/bnpb.auth.Auth/IssueToken"}
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

func TestUnaryAuthPublicMethodsBypassAuth(t *testing.T) {
	interceptor := UnaryAuth(AuthConfig{Token: "service-secret", PublicMethods: []string{"/bnpb.auth.Auth/HealthCheck"}})
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	publicInfo := &grpc.UnaryServerInfo{FullMethod: "/bnpb.auth.Auth/HealthCheck"}
	response, err := interceptor(context.Background(), nil, publicInfo, handler)
	if err != nil || response != "ok" {
		t.Fatalf("public method result = %v, %v; want ok, nil", response, err)
	}

	protectedInfo := &grpc.UnaryServerInfo{FullMethod: "/bnpb.auth.Auth/IssueToken"}
	_, err = interceptor(context.Background(), nil, protectedInfo, handler)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("protected method without token code = %s, want Unauthenticated", status.Code(err))
	}
}

func TestHasBearerTokenRejectsWrongTokens(t *testing.T) {
	if HasBearerToken([]string{"Bearer wrong-secret"}, "service-secret") {
		t.Fatal("wrong bearer token was accepted")
	}
	if !HasBearerToken([]string{"Bearer service-secret"}, "service-secret") {
		t.Fatal("matching bearer token was rejected")
	}
	if HasBearerToken([]string{"service-secret"}, "service-secret") {
		t.Fatal("token without Bearer prefix was accepted")
	}
	if HasBearerToken(nil, "service-secret") {
		t.Fatal("missing authorization header was accepted")
	}
}

type testServerTransportStream struct {
	header metadata.MD
}

func (s *testServerTransportStream) Method() string {
	return "/bnpb.auth.Auth/IssueToken"
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
