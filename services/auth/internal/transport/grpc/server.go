package grpctransport

import (
	"log/slog"

	authpb "github.com/fathurwithyou/microservices/contracts/gen/go/auth"
	"github.com/fathurwithyou/microservices/pkg/grpckit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// HealthMethod is the full gRPC method path of the auth health check. It is
// public so callers can serve it without a service token.
const HealthMethod = "/bnpb.auth.Auth/HealthCheck"

// ServerConfig holds the collaborators the gRPC server needs. Unlike a
// variadic logger, every dependency is explicit.
type ServerConfig struct {
	// Tokens is the application service backing the auth RPCs.
	Tokens TokenService
	// ServiceToken is the bearer token required on protected methods.
	ServiceToken string
	// Logger receives structured log output. Defaults to slog.Default().
	Logger *slog.Logger
	// HealthMethod is the public method path that bypasses auth.
	// Defaults to the HealthMethod constant.
	HealthMethod string
}

// NewServer builds the auth gRPC server with shared observability and
// service-token auth interceptors.
func NewServer(cfg ServerConfig) *grpc.Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	healthMethod := cfg.HealthMethod
	if healthMethod == "" {
		healthMethod = HealthMethod
	}
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		grpckit.UnaryObservability(grpckit.Config{Service: "auth", Logger: logger}),
		grpckit.UnaryAuth(grpckit.AuthConfig{Token: cfg.ServiceToken, PublicMethods: []string{healthMethod}}),
	))
	authpb.RegisterAuthServer(server, newAuthHandler(cfg.Tokens, logger))
	registerHealth(server)
	reflection.Register(server)
	return server
}

func registerHealth(server *grpc.Server) {
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
}
