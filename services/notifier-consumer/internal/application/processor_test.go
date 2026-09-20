package application

import (
	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"google.golang.org/protobuf/proto"
	"log/slog"
	"testing"
)

func TestProcessorAcceptsDuplicateEvent(t *testing.T) {
	processor := NewProcessor("notifier-consumer", slog.Default())
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
