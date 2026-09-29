// Package grpctransport exposes the aggregator query API over gRPC.
package grpctransport

import (
	"log/slog"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	"github.com/fathurwithyou/microservices/pkg/grpckit"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// HealthMethod is the full gRPC method path that bypasses service-token
// authentication.
const HealthMethod = "/bnpb.aggregator.Aggregator/HealthCheck"

// ServerConfig configures the gRPC server.
type ServerConfig struct {
	// Queries backs the hazard read API. Required.
	Queries *hazards.Queries
	// ServiceToken is the internal bearer token clients must present.
	ServiceToken string
	// Logger receives structured logs. Defaults to slog.Default().
	Logger *slog.Logger
}

// NewServer returns a gRPC server with observability and service-token
// interceptors, the query handler, health reporting, and reflection.
func NewServer(cfg ServerConfig) *grpc.Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		grpckit.UnaryObservability(grpckit.Config{Service: "aggregator", Logger: logger}),
		grpckit.UnaryAuth(grpckit.AuthConfig{Token: cfg.ServiceToken, PublicMethods: []string{HealthMethod}}),
	))
	aggregatorpb.RegisterAggregatorServer(server, newQueryHandler(cfg.Queries))
	registerHealth(server)
	reflection.Register(server)
	return server
}

func registerHealth(server *grpc.Server) {
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
}
