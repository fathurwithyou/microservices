package ingest

import (
	"testing"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/eventmessage"
)

func TestRoutingKeyUsesCriticalExchangeOnlyForCriticalCoastal(t *testing.T) {
	tests := []struct {
		name  string
		event domain.HazardEvent
		want  string
	}{
		{"normal coastal", domain.HazardEvent{Source: domain.SourceCoastalSensor, Attributes: map[string]any{"critical": false}}, eventmessage.RoutingKey},
		{"critical coastal", domain.HazardEvent{Source: domain.SourceCoastalSensor, Attributes: map[string]any{"critical": true}}, eventmessage.CriticalRoutingKey},
		{"other source", domain.HazardEvent{Source: domain.SourceSatellite, Attributes: map[string]any{"critical": true}}, eventmessage.RoutingKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := routingKey(tt.event); got != tt.want {
				t.Fatalf("routingKey = %q, want %q", got, tt.want)
			}
		})
	}
}
