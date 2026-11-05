package sources

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
