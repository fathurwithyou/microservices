package integration

import (
	"net/http"
	"os"
	"testing"
)

func TestHealthSmoke(t *testing.T) {
	baseURL := os.Getenv("CLIENT_API_URL")
	if baseURL == "" { t.Skip("set CLIENT_API_URL to run against a live compose or K3s deployment") }
	response, err := http.Get(baseURL + "/health"); if err != nil { t.Fatal(err) }; defer response.Body.Close()
	if response.StatusCode != http.StatusOK { t.Fatalf("health status: %d", response.StatusCode) }
}
