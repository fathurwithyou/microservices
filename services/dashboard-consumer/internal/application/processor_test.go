package application

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	eventspb "github.com/fathurwithyou/microservices/contracts/gen/go/events"
	"google.golang.org/protobuf/proto"
)

func TestProcessorDeduplicatesWithinProcessButNotAcrossRestart(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	processor := NewProcessor("dashboard-consumer", logger)
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
	if got := strings.Count(output.String(), "event processed"); got != 1 {
		t.Fatalf("same-process delivery count = %d, want 1", got)
	}

	restarted := NewProcessor("dashboard-consumer", logger)
	if err := restarted.Process(payload, "c-1", true); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(output.String(), "event processed"); got != 2 {
		t.Fatalf("post-restart delivery count = %d, want 2 to document in-memory deduplication", got)
	}
}
