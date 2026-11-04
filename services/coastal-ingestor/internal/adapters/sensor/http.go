// Package sensor implements the outbound HTTP source for coastal sensor
// readings.
package sensor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/ports"
)

// Client fetches readings from a JSON HTTP feed.
type Client struct {
	url    string
	token  string
	client *http.Client
}

// New builds a client. An empty url selects fixture mode: Fetch returns no
// readings without making a request.
func New(url, token string, timeout time.Duration) *Client {
	return &Client{
		url:    url,
		token:  token,
		client: &http.Client{Timeout: timeout},
	}
}

// wireReading mirrors the upstream JSON shape.
type wireReading struct {
	ID         string          `json:"id"`
	SensorID   string          `json:"sensor_id"`
	OccurredAt time.Time       `json:"occurred_at"`
	ReceivedAt time.Time       `json:"received_at"`
	Latitude   float64         `json:"latitude"`
	Longitude  float64         `json:"longitude"`
	Severity   string          `json:"severity"`
	Critical   bool            `json:"critical"`
	Payload    json.RawMessage `json:"payload"`
}

// Fetch retrieves the current batch of readings.
func (c *Client) Fetch(ctx context.Context) ([]domain.Reading, error) {
	if c.url == "" {
		return nil, nil
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("build sensor request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}

	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ports.ErrSourceUnavailable, err)
	}
	defer response.Body.Close()

	switch {
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		return nil, fmt.Errorf("%w: status %d", ports.ErrSourceUnauthorized, response.StatusCode)
	case response.StatusCode < 200 || response.StatusCode >= 300:
		return nil, fmt.Errorf("%w: status %d", ports.ErrSourceUnavailable, response.StatusCode)
	}

	var wire []wireReading
	if err := json.NewDecoder(response.Body).Decode(&wire); err != nil {
		return nil, fmt.Errorf("%w: %v", ports.ErrSourcePayload, err)
	}

	readings := make([]domain.Reading, 0, len(wire))
	for _, item := range wire {
		readings = append(readings, domain.Reading{
			ID:         item.ID,
			SensorID:   item.SensorID,
			OccurredAt: item.OccurredAt,
			ReceivedAt: item.ReceivedAt,
			Latitude:   item.Latitude,
			Longitude:  item.Longitude,
			Severity:   domain.Severity(item.Severity),
			Critical:   item.Critical,
			Payload:    item.Payload,
		})
	}
	return readings, nil
}
