package observability

import (
	"context"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Setup installs an OTLP/HTTP tracer provider when OTEL_EXPORTER_OTLP_ENDPOINT
// is set. Without an endpoint it returns a no-op shutdown so local runs do not
// fail when no collector is present.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", service)))
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler()),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return provider.Shutdown, nil
}

// sampler keeps every trace by default. OTEL_TRACES_SAMPLER_ARG (0..1) keeps
// that fraction of new traces; spans with a sampled parent are always kept,
// so a sampled request is traced end to end across services. At thousands of
// requests per second, recording and exporting a span for every rejected
// request costs the entry tier more CPU than serving the admitted ones.
func sampler() sdktrace.Sampler {
	ratio, err := strconv.ParseFloat(os.Getenv("OTEL_TRACES_SAMPLER_ARG"), 64)
	if err != nil || ratio >= 1 {
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	}
	if ratio < 0 {
		ratio = 0
	}
	return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
}

// HTTPTracing wraps a stdlib handler with server spans.
func HTTPTracing(handler http.Handler, service string) http.Handler {
	return otelhttp.NewHandler(handler, service)
}

// GinTracing records server spans for Gin routes.
func GinTracing(service string) gin.HandlerFunc { return otelgin.Middleware(service) }
