package application

import (
	"log/slog"
	"testing"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"google.golang.org/protobuf/proto"
)

func TestProcessorAcceptsDuplicateEvent(t *testing.T) {
	processor := NewProcessor("dashboard-consumer", slog.Default())
	payload, err := proto.Marshal(&eventspb.HazardEvent{HazardId: "h-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(payload, "c-1", false); err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(payload, "c-1", true); err != nil {
		t.Fatal(err)
	}
}
