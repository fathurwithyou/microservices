// Package grpckit provides shared gRPC server interceptors: request
// observability with panic recovery and correlation ID propagation, and
// bearer service-token authentication. Service-specific policy (token
// values, public method paths) is supplied through configuration.
package grpckit

import (
	"context"
	"crypto/subtle"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"github.com/fathurwithyou/microservices/pkg/correlation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Config configures the observability interceptor.
type Config struct {
	// Service is the service name recorded in structured logs.
	Service string
	// Logger receives structured log output. Defaults to slog.Default().
	Logger *slog.Logger
}

// AuthConfig configures the bearer service-token auth interceptor.
type AuthConfig struct {
	// Token is the expected bearer token, compared in constant time.
	Token string
	// PublicMethods lists full method paths (for example
	// "/bnpb.auth.Auth/HealthCheck") that bypass authentication.
	PublicMethods []string
}

// UnaryObservability returns an interceptor that extracts or generates a
// correlation ID, echoes it in response headers, recovers panics, and logs
// one structured record per unary call.
func UnaryObservability(cfg Config) grpc.UnaryServerInterceptor {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		started := time.Now()
		correlationID := incomingCorrelationID(ctx)
		ctx = correlation.WithContext(ctx, correlationID)
		_ = grpc.SetHeader(ctx, metadata.Pairs(correlation.GRPCKey, correlationID, correlation.GRPCKeyAlt, correlationID))

		defer func() {
			if recovered := recover(); recovered != nil {
				response = nil
				err = status.Error(codes.Internal, "internal server error")
				logger.ErrorContext(ctx, "grpc panic recovered",
					"service", cfg.Service,
					"component", "grpc-server",
					"operation", info.FullMethod,
					"rpc_method", info.FullMethod,
					"request_type", fmt.Sprintf("%T", request),
					"correlation_id", correlationID,
					"panic", recovered,
					"stack", string(debug.Stack()),
				)
			}
			logUnaryResult(logger, cfg.Service, ctx, info.FullMethod, request, correlationID, started, err)
		}()

		return handler(ctx, request)
	}
}

// UnaryAuth returns an interceptor that requires a valid bearer service
// token on every method except those listed in cfg.PublicMethods.
func UnaryAuth(cfg AuthConfig) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		for _, public := range cfg.PublicMethods {
			if info.FullMethod == public {
				return handler(ctx, request)
			}
		}
		incoming, ok := metadata.FromIncomingContext(ctx)
		if !ok || !HasBearerToken(incoming.Get("authorization"), cfg.Token) {
			return nil, status.Error(codes.Unauthenticated, "invalid service token")
		}
		return handler(ctx, request)
	}
}

// HasBearerToken reports whether values contains an "Bearer <token>" entry
// matching expected. The comparison is constant time.
func HasBearerToken(values []string, expected string) bool {
	for _, value := range values {
		token, ok := strings.CutPrefix(value, "Bearer ")
		if !ok {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1 {
			return true
		}
	}
	return false
}

// Authorization builds outgoing gRPC metadata carrying the bearer token.
func Authorization(token string) metadata.MD {
	return metadata.Pairs("authorization", "Bearer "+token)
}

func logUnaryResult(logger *slog.Logger, service string, ctx context.Context, method string, request any, correlationID string, started time.Time, err error) {
	deadline := "none"
	deadlineRemaining := int64(-1)
	if value, ok := ctx.Deadline(); ok {
		deadline = value.UTC().Format(time.RFC3339Nano)
		deadlineRemaining = time.Until(value).Milliseconds()
	}

	code := status.Code(err)
	attrs := []any{
		"service", service,
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
		for _, key := range []string{correlation.GRPCKey, correlation.GRPCKeyAlt} {
			for _, value := range incoming.Get(key) {
				if value = strings.TrimSpace(value); value != "" {
					return value
				}
			}
		}
	}
	return correlation.New()
}
