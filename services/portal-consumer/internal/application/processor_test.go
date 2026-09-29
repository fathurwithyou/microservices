package application

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"github.com/fathurwithyou/microservices/pkg/consumerkit"
	"google.golang.org/protobuf/proto"
)

func TestProcessorDeduplicatesWithinProcessButNotAcrossRestart(t *testing.T) {
	payload, err := proto.Marshal(&eventspb.HazardEvent{HazardId: "h-1"})
	if err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	processor := NewProcessor("portal-consumer", logger)

	tests := []struct {
		name     string
		delivery consumerkit.Delivery
		want     consumerkit.Outcome
	}{
		{
			name:     "invalid payload is dead-lettered",
			delivery: consumerkit.Delivery{Body: []byte{0xff, 0xfe, 0xfd}, CorrelationID: "c-bad"},
			want:     consumerkit.DeadLetter,
		},
		{
			name:     "first event is acked and logged",
			delivery: consumerkit.Delivery{Body: payload, CorrelationID: "c-1"},
			want:     consumerkit.Ack,
		},
		{
			name:     "duplicate event is acked without reprocessing",
			delivery: consumerkit.Delivery{Body: payload, CorrelationID: "c-1", Redelivered: true},
			want:     consumerkit.Ack,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := processor.Process(context.Background(), tc.delivery); got != tc.want {
				t.Fatalf("Process() outcome = %v, want %v", got, tc.want)
			}
		})
	}

	if got := strings.Count(output.String(), "event processed"); got != 1 {
		t.Fatalf("same-process delivery count = %d, want 1", got)
	}
	if !strings.Contains(output.String(), "invalid hazard event") {
		t.Error("decode failure log does not wrap ErrInvalidEvent")
	}

	restarted := NewProcessor("portal-consumer", logger)
	if got := restarted.Process(context.Background(), consumerkit.Delivery{Body: payload, CorrelationID: "c-1", Redelivered: true}); got != consumerkit.Ack {
		t.Fatalf("post-restart Process() outcome = %v, want %v", got, consumerkit.Ack)
	}
	if got := strings.Count(output.String(), "event processed"); got != 2 {
		t.Fatalf("post-restart delivery count = %d, want 2 to document in-memory deduplication", got)
	}
}
