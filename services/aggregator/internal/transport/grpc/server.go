package grpctransport

import (
	"log/slog"

	aggregatorpb "github.com/fathurwithyou/microservices/contracts/gen/go/aggregator"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/application/hazards"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func NewServer(queries *hazards.Queries, internalServiceToken string, loggers ...*slog.Logger) *grpc.Server {
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		unaryObservabilityInterceptor(logger),
		unaryAuthInterceptor(internalServiceToken),
	))
	aggregatorpb.RegisterAggregatorServer(server, newQueryHandler(queries))
	registerHealth(server)
	reflection.Register(server)
	return server
}

func registerHealth(server *grpc.Server) {
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
}
