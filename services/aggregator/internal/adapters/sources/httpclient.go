// Package sources provides the HTTP client used to poll external hazard
// feeds. Each client instance carries its own base URL, credential header,
// and timeout; no feed-specific logic lives here.
package sources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// Client performs authenticated GET requests against one external feed and
// logs every outbound call with latency and correlation ID.
type Client struct {
	baseURL string
	client  *http.Client
	name    string
	header  string
	value   string
	logger  *slog.Logger
}

// ClientConfig configures a Client.
type ClientConfig struct {
	// BaseURL is the feed root, e.g. "http://bmkg:8081". Required.
	BaseURL string
	// Name identifies the feed in logs and error messages. Required.
	Name string
	// Header carries the credential. Required.
	Header string
	// Value is the credential value sent in Header.
	Value string
	// Timeout bounds each request. Must be positive.
	Timeout time.Duration
	// Logger receives outbound call logs. Defaults to slog.Default().
	Logger *slog.Logger
}

// ErrInvalidConfig is returned by New for missing required configuration.
var ErrInvalidConfig = errors.New("sources: invalid config")

// New validates cfg and returns a Client.
func New(cfg ClientConfig) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("%w: BaseURL is required", ErrInvalidConfig)
	}
	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidConfig)
	}
	if cfg.Header == "" {
		return nil, fmt.Errorf("%w: Header is required", ErrInvalidConfig)
	}
	if cfg.Timeout <= 0 {
		return nil, fmt.Errorf("%w: Timeout must be positive", ErrInvalidConfig)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Client{baseURL: cfg.BaseURL, client: &http.Client{Timeout: cfg.Timeout}, name: cfg.Name, header: cfg.Header, value: cfg.Value, logger: cfg.Logger}, nil
}

// Get fetches path and decodes a JSON array of raw payloads, propagating
// the correlation ID to the upstream service.
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
