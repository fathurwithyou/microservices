package grpctransport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func unaryAuthInterceptor(expected string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == "/bnpb.auth.Auth/HealthCheck" {
			return handler(ctx, request)
		}
		incoming, ok := metadata.FromIncomingContext(ctx)
		if !ok || !hasServiceToken(incoming.Get("authorization"), expected) {
			return nil, status.Error(codes.Unauthenticated, "invalid service token")
		}
		return handler(ctx, request)
	}
}

func hasServiceToken(values []string, expected string) bool {
	for _, value := range values {
		if strings.HasPrefix(value, "Bearer ") && strings.TrimPrefix(value, "Bearer ") == expected {
			return true
		}
	}
	return false
}

func unaryObservabilityInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		logger = slog.Default()
	}

	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		started := time.Now()
		correlationID := incomingCorrelationID(ctx)
		ctx = context.WithValue(ctx, correlationIDContextKey{}, correlationID)
		_ = grpc.SetHeader(ctx, metadata.Pairs("correlation-id", correlationID, "x-correlation-id", correlationID))

		defer func() {
			if recovered := recover(); recovered != nil {
				response = nil
				err = status.Error(codes.Internal, "internal server error")
				logger.ErrorContext(ctx, "grpc panic recovered",
					"service", "auth",
					"component", "grpc-server",
					"operation", info.FullMethod,
					"rpc_method", info.FullMethod,
					"request_type", fmt.Sprintf("%T", request),
					"correlation_id", correlationID,
					"panic", recovered,
					"stack", string(debug.Stack()),
				)
			}
			logUnaryResult(logger, ctx, info.FullMethod, request, correlationID, started, err)
		}()

		return handler(ctx, request)
	}
}

func logUnaryResult(logger *slog.Logger, ctx context.Context, method string, request any, correlationID string, started time.Time, err error) {
	deadline := "none"
	deadlineRemaining := int64(-1)
	if value, ok := ctx.Deadline(); ok {
		deadline = value.UTC().Format(time.RFC3339Nano)
		deadlineRemaining = time.Until(value).Milliseconds()
	}

	code := status.Code(err)
	attrs := []any{
		"service", "auth",
		"component", "grpc-server",
		"operation", method,
		"rpc_method", method,
		"request_type", fmt.Sprintf("%T", request),
		"correlation_id", correlationID,
		"latency_ms", time.Since(started).Milliseconds(),
		"status", code.String(),
		"status_code", int32(code),
		"deadline", deadline,
		"deadline_remaining_ms", deadlineRemaining,
	}
	if err != nil {
		attrs = append(attrs, "error", err)
		logger.ErrorContext(ctx, "grpc request completed", attrs...)
		return
	}
	logger.InfoContext(ctx, "grpc request completed", attrs...)
}

func incomingCorrelationID(ctx context.Context) string {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if ok {
		for _, key := range []string{"correlation-id", "x-correlation-id"} {
			for _, value := range incoming.Get(key) {
				if value = strings.TrimSpace(value); value != "" {
					return value
				}
			}
		}
	}
	return newCorrelationID()
}

func newCorrelationID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err == nil {
		return hex.EncodeToString(value[:])
	}
	return fmt.Sprintf("%x", time.Now().UnixNano())
}

type correlationIDContextKey struct{}
