package grpckit

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var (
	grpcRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "grpc_requests_total",
		Help: "gRPC server requests by service, method, and status code.",
	}, []string{"service", "method", "code"})
	grpcDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "grpc_request_duration_seconds",
		Help:    "gRPC server latency by service and method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"service", "method"})
)

func init() {
	prometheus.MustRegister(grpcRequests, grpcDuration)
}

// UnaryMetrics records request count and latency on the default registry.
func UnaryMetrics(service string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		response, err := handler(ctx, request)
		grpcRequests.WithLabelValues(service, info.FullMethod, status.Code(err).String()).Inc()
		grpcDuration.WithLabelValues(service, info.FullMethod).Observe(time.Since(started).Seconds())
		return response, err
	}
}
