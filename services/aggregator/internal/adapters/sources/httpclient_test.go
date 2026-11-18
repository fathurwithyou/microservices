package sources

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
)

func TestGetKeepsMalformedBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"broken"`))
	}))
	defer server.Close()

	client, err := New(ClientConfig{BaseURL: server.URL, Name: "test", Header: "X-Test", Value: "1", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Get(context.Background(), "/events", "correlation-1")
	var malformed *domain.MalformedPayloadError
	if !errors.As(err, &malformed) {
		t.Fatalf("err = %v, want MalformedPayloadError", err)
	}
	if string(malformed.Body) != `{"broken"` {
		t.Fatalf("body = %q", malformed.Body)
	}
}

func TestGetRejectsOversizedAndUndecodableBodies(t *testing.T) {
	cases := map[string]struct {
		body   []byte
		reason string
		keep   int
	}{
		"oversized":    {body: append(append([]byte(`[{"padding":"`), bytes.Repeat([]byte("x"), MaxResponseBytes)...), []byte(`"}]`)...), reason: "payload_too_large", keep: MaxResponseBytes},
		"deep nesting": {body: []byte(strings.Repeat("[", 20000) + strings.Repeat("]", 20000)), reason: "invalid_json", keep: 40000},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write(tc.body)
			}))
			defer server.Close()
			client, err := New(ClientConfig{BaseURL: server.URL, Name: "test", Header: "X-Test", Value: "1", Timeout: 5 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.Get(context.Background(), "/events", "correlation-1")
			var malformed *domain.MalformedPayloadError
			if !errors.As(err, &malformed) {
				t.Fatalf("err = %v, want MalformedPayloadError", err)
			}
			if !strings.Contains(malformed.Error(), tc.reason) {
				t.Fatalf("error %q does not name %s", malformed.Error(), tc.reason)
			}
			if len(malformed.Body) != tc.keep {
				t.Fatalf("kept %d bytes, want %d", len(malformed.Body), tc.keep)
			}
		})
	}
}

func TestGetSplitsInvalidRecordsFromValidOnes(t *testing.T) {
	body := []byte("[{\"volcano_id\":\"merapi\xff\xfe\"},{\"volcano_id\":\"semeru\"},42]")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer server.Close()
	client, err := New(ClientConfig{BaseURL: server.URL, Name: "test", Header: "X-Test", Value: "1", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	records, err := client.Get(context.Background(), "/events", "correlation-1")
	var partial *domain.MalformedRecordsError
	if !errors.As(err, &partial) {
		t.Fatalf("err = %v, want MalformedRecordsError", err)
	}
	if len(records) != 1 || string(records[0]["volcano_id"]) != `"semeru"` {
		t.Fatalf("records = %v, want only the valid one", records)
	}
	if len(partial.Records) != 2 {
		t.Fatalf("bad records = %d, want 2", len(partial.Records))
	}
	if got := string(partial.Records[0].Body); got != "{\"volcano_id\":\"merapi\xff\xfe\"}" {
		t.Fatalf("bad record body = %q", got)
	}
	if !strings.Contains(partial.Records[0].Err.Error(), "invalid_utf8") || !strings.Contains(partial.Records[1].Err.Error(), "invalid_json") {
		t.Fatalf("reasons = %v / %v", partial.Records[0].Err, partial.Records[1].Err)
	}
}
