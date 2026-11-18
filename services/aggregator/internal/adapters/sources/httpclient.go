// Package sources provides the HTTP client used to poll external hazard
// feeds. Each client instance carries its own base URL, credential header,
// and timeout; no feed-specific logic lives here.
package sources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/fathurwithyou/microservices/pkg/correlation"
	"github.com/fathurwithyou/microservices/services/aggregator/internal/domain"
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

// MaxResponseBytes bounds one upstream response body.
const MaxResponseBytes = 8 << 20

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
	request.Header.Set(correlation.Header, correlationID)
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
	// Bound one response so a broken or hostile upstream cannot exhaust the
	// aggregator heap with a giant JSON document. Read one byte past the
	// limit to tell "exactly at the limit" from "too large". Keep the body on
	// every decode failure so the poller can park it instead of discarding it.
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxResponseBytes+1))
	if err != nil {
		c.log(correlationID, started, response.StatusCode, err)
		return nil, fmt.Errorf("%s read body: %w", c.name, err)
	}
	if len(body) > MaxResponseBytes {
		err := fmt.Errorf("payload_too_large: response exceeds %d bytes; first %d bytes kept", MaxResponseBytes, MaxResponseBytes)
		c.log(correlationID, started, response.StatusCode, err)
		return nil, &domain.MalformedPayloadError{Body: body[:MaxResponseBytes], Err: err}
	}
	// Split the array first: only a body that is not a JSON array at all
	// (truncated, wrong shape, too deep) is a whole-response failure. Each
	// element is then validated on its own so one bad record cannot hide the
	// valid ones next to it.
	var elements []json.RawMessage
	if err := json.Unmarshal(body, &elements); err != nil {
		err = fmt.Errorf("invalid_json: %w", err)
		c.log(correlationID, started, response.StatusCode, err)
		return nil, &domain.MalformedPayloadError{Body: body, Err: err}
	}
	result := make([]map[string]json.RawMessage, 0, len(elements))
	var bad []domain.MalformedRecord
	for _, element := range elements {
		// encoding/json silently replaces invalid UTF-8 with U+FFFD, which
		// would store corrupted text as if it were valid. Reject it explicitly.
		if !utf8.Valid(element) {
			bad = append(bad, domain.MalformedRecord{Body: element, Err: errors.New("invalid_utf8: record is not valid UTF-8")})
			continue
		}
		var record map[string]json.RawMessage
		if err := json.Unmarshal(element, &record); err != nil {
			bad = append(bad, domain.MalformedRecord{Body: element, Err: fmt.Errorf("invalid_json: %w", err)})
			continue
		}
		result = append(result, record)
	}
	if len(bad) > 0 {
		err := &domain.MalformedRecordsError{Records: bad}
		c.log(correlationID, started, response.StatusCode, err)
		return result, err
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
