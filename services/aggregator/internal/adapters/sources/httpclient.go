package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type Client struct {
	baseURL string
	client  *http.Client
	name    string
	header  string
	value   string
	logger  *slog.Logger
}

func New(baseURL, name, header, value string, timeout time.Duration, logger *slog.Logger) *Client {
	return &Client{baseURL: baseURL, name: name, header: header, value: value, client: &http.Client{Timeout: timeout}, logger: logger}
}

func (c *Client) Get(ctx context.Context, path, correlationID string) ([]map[string]json.RawMessage, error) {
	started := time.Now()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set(c.header, c.value)
	request.Header.Set("X-Correlation-ID", correlationID)
	response, err := c.client.Do(request)
	if err != nil {
		c.log(correlationID, started, 0, err)
		return nil, fmt.Errorf("%s unavailable: %w", c.name, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		c.log(correlationID, started, response.StatusCode, fmt.Errorf("upstream status %d", response.StatusCode))
		return nil, fmt.Errorf("%s returned status %d", c.name, response.StatusCode)
	}
	var result []map[string]json.RawMessage
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		c.log(correlationID, started, response.StatusCode, err)
		return nil, err
	}
	c.log(correlationID, started, response.StatusCode, nil)
	return result, nil
}

func (c *Client) log(correlationID string, started time.Time, status int, err error) {
	attrs := []any{"component", "source-client", "remote_service", c.name, "correlation_id", correlationID, "latency_ms", time.Since(started).Milliseconds(), "status", status}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	c.logger.Info("outbound call", attrs...)
}
