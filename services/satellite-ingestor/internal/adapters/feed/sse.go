// Package feed implements the inbound SSE hotspot feed adapter.
package feed

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/ports"
)

// Config configures the SSE feed client.
type Config struct {
	// URL is the feed base URL; /v1/stream is appended to it.
	URL string
	// Token is sent as a bearer credential.
	Token string
	// Timeout bounds one connection attempt, including streaming.
	Timeout time.Duration
}

// Client is an HTTP SSE implementation of ports.Feed.
type Client struct {
	url    string
	token  string
	client *http.Client
}

// New returns a feed client.
func New(cfg Config) *Client {
	return &Client{
		url:    cfg.URL,
		token:  cfg.Token,
		client: &http.Client{Timeout: cfg.Timeout},
	}
}

// Stream opens the SSE endpoint at offset and dispatches decoded events.
func (c *Client) Stream(ctx context.Context, offset int64, handler ports.EventHandler) error {
	endpoint, err := streamURL(c.url, offset)
	if err != nil {
		return fmt.Errorf("build feed URL: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build feed request: %w", err)
	}
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Authorization", "Bearer "+c.token)
	if offset > 0 {
		request.Header.Set("Last-Event-ID", strconv.FormatInt(offset, 10))
	}

	response, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("connect feed: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return fmt.Errorf("feed status %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return readEvents(ctx, response.Body, handler)
}

func streamURL(base string, offset int64) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/v1/stream"
	query := parsed.Query()
	query.Set("from", strconv.FormatInt(offset, 10))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

// readEvents parses SSE frames until the stream ends. Comment lines and blank
// lines carry no data and are skipped.
func readEvents(ctx context.Context, body io.Reader, handler ports.EventHandler) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var (
		kind string
		id   string
		data []string
	)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			event, ok, err := decodeEvent(kind, id, data)
			if err != nil {
				return err
			}
			if ok {
				if err := handler(ctx, event); err != nil {
					return err
				}
			}
			kind, id, data = "", "", nil
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			kind = value
		case "id":
			id = value
		case "data":
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read feed stream: %w", err)
	}
	return nil
}

func decodeEvent(kind, id string, data []string) (ports.Event, bool, error) {
	if kind == "" && id == "" && len(data) == 0 {
		return ports.Event{}, false, nil
	}
	offset, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return ports.Event{}, false, fmt.Errorf("invalid event id %q: %w", id, err)
	}

	switch ports.EventKind(kind) {
	case ports.KindHeartbeat:
		return ports.Event{Offset: offset, Kind: ports.KindHeartbeat}, true, nil
	case ports.KindDetection, ports.KindUpdate:
		var payload detectionPayload
		if err := json.Unmarshal([]byte(strings.Join(data, "\n")), &payload); err != nil {
			return ports.Event{}, false, fmt.Errorf("decode %s event %d: %w", kind, offset, err)
		}
		return ports.Event{Offset: offset, Kind: ports.EventKind(kind), Detection: payload.detection()}, true, nil
	case ports.KindRetract:
		var payload struct {
			Key string `json:"key"`
		}
		if err := json.Unmarshal([]byte(strings.Join(data, "\n")), &payload); err != nil {
			return ports.Event{}, false, fmt.Errorf("decode retract event %d: %w", offset, err)
		}
		if payload.Key == "" {
			return ports.Event{}, false, fmt.Errorf("decode retract event %d: missing detection key", offset)
		}
		return ports.Event{Offset: offset, Kind: ports.KindRetract, DetectionKey: payload.Key}, true, nil
	default:
		// Unknown event kinds are ignored so a feed that adds new events does
		// not take the ingestor down.
		return ports.Event{}, false, nil
	}
}

type detectionPayload struct {
	Satellite  string  `json:"satellite"`
	AcqDate    string  `json:"acq_date"`
	AcqTime    string  `json:"acq_time"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	Confidence string  `json:"confidence"`
	FRP        float64 `json:"frp"`
}

func (p detectionPayload) detection() domain.Detection {
	return domain.Detection{
		Satellite:  domain.Satellite(p.Satellite),
		AcqDate:    p.AcqDate,
		AcqTime:    p.AcqTime,
		Latitude:   p.Latitude,
		Longitude:  p.Longitude,
		Confidence: domain.Confidence(p.Confidence),
		FRP:        p.FRP,
	}
}
