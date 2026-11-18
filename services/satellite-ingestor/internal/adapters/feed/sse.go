// Package feed implements the inbound SSE hotspot feed adapter.
package feed

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	// IdleTimeout ends a stream that delivered nothing, not even a heartbeat,
	// for this long. A connection that died without a FIN or RST would
	// otherwise look alive forever.
	IdleTimeout time.Duration
}

// StatusError is a non-200 answer from the feed.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string { return fmt.Sprintf("feed status %d: %s", e.Code, e.Body) }

// Client is an HTTP SSE implementation of ports.Feed.
type Client struct {
	url     string
	token   string
	idle    time.Duration
	stream  *http.Client
	session *http.Client
}

// New returns a feed client.
func New(cfg Config) *Client {
	idle := cfg.IdleTimeout
	if idle <= 0 {
		idle = 45 * time.Second
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 15 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		// The stream is one long response; compression would buffer events.
		DisableCompression: true,
	}
	return &Client{
		url:   cfg.URL,
		token: cfg.Token,
		idle:  idle,
		// No overall timeout: the response body is the stream. Liveness is
		// enforced by the idle watchdog instead.
		stream:  &http.Client{Transport: transport},
		session: &http.Client{Transport: transport, Timeout: 10 * time.Second},
	}
}

// Session reads the feed state used for lag and session-change detection.
func (c *Client) Session(ctx context.Context) (ports.Session, error) {
	endpoint, err := c.endpoint("/v1/session")
	if err != nil {
		return ports.Session{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return ports.Session{}, fmt.Errorf("build session request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.session.Do(request)
	if err != nil {
		return ports.Session{}, fmt.Errorf("connect session: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if response.StatusCode != http.StatusOK {
		return ports.Session{}, &StatusError{Code: response.StatusCode, Body: clip(string(body))}
	}
	return decodeSession(body)
}

// decodeSession reads the session leniently: speed may be a number or a
// string such as "60x", and unknown fields are ignored.
func decodeSession(body []byte) (ports.Session, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return ports.Session{}, fmt.Errorf("decode session: %w", err)
	}
	session := ports.Session{
		Group:   text(fields["group"]),
		Profile: text(fields["profile"]),
	}
	session.Speed, _ = strconv.ParseFloat(strings.TrimRight(text(fields["speed"]), "xX× "), 64)
	session.LatestOffset, _ = strconv.ParseInt(text(fields["latest_offset"]), 10, 64)
	if at, err := time.Parse(time.RFC3339Nano, text(fields["virtual_now"])); err == nil {
		session.VirtualNow = at.UTC()
	}
	return session, nil
}

func (c *Client) endpoint(path string) (string, error) {
	parsed, err := url.Parse(c.url)
	if err != nil {
		return "", err
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + path
	return parsed.String(), nil
}

// Stream opens the SSE endpoint after offset and dispatches decoded events.
// Both resume conventions are sent (from= and Last-Event-ID); the caller
// drops any event at or below its committed offset, so either reading of
// "from" by the server is safe.
func (c *Client) Stream(ctx context.Context, offset int64, handler ports.EventHandler) error {
	endpoint, err := streamURL(c.url, offset)
	if err != nil {
		return fmt.Errorf("build feed URL: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build feed request: %w", err)
	}
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Cache-Control", "no-cache")
	request.Header.Set("Authorization", "Bearer "+c.token)
	if offset > 0 {
		request.Header.Set("Last-Event-ID", strconv.FormatInt(offset, 10))
	}

	response, err := c.stream.Do(request)
	if err != nil {
		return fmt.Errorf("connect feed: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return &StatusError{Code: response.StatusCode, Body: clip(strings.TrimSpace(string(body)))}
	}

	idled := false
	watchdog := time.AfterFunc(c.idle, func() {
		idled = true
		cancel()
	})
	defer watchdog.Stop()
	err = readEvents(ctx, response.Body, handler, func() { watchdog.Reset(c.idle) })
	if idled {
		return fmt.Errorf("feed idle for %s, reconnecting", c.idle)
	}
	return err
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

// maxEventBytes bounds one SSE line. A longer line ends the stream with an
// error instead of growing the buffer without limit.
const maxEventBytes = 1 << 20

// readEvents parses SSE frames until the stream ends. Comment lines keep the
// connection alive but carry no data.
func readEvents(ctx context.Context, body io.Reader, handler ports.EventHandler, alive func()) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), maxEventBytes)

	var (
		kind string
		id   string
		data []string
	)
	for scanner.Scan() {
		alive()
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			event, ok := decodeEvent(kind, id, data)
			kind, id, data = "", "", nil
			if ok {
				event.ReceivedAt = time.Now()
				if err := handler(ctx, event); err != nil {
					return err
				}
			}
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
	if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("read feed stream: %w", err)
	}
	return nil
}

// decodeEvent turns one SSE frame into an event. It never fails the stream:
// a frame with an offset that cannot be understood becomes a skipped event,
// so the offset still advances and one bad frame cannot wedge the consumer
// in a reconnect loop on the same position. ok is false only for frames that
// carry nothing to act on.
func decodeEvent(kind, id string, data []string) (ports.Event, bool) {
	if kind == "" && id == "" && len(data) == 0 {
		return ports.Event{}, false
	}
	payload := strings.Join(data, "\n")
	fields := objectFields([]byte(payload))
	if kind == "" || kind == "message" {
		for _, name := range []string{"type", "event", "kind"} {
			if value := text(fields[name]); value != "" {
				kind = value
				break
			}
		}
	}
	kind = strings.ToLower(strings.TrimSpace(kind))
	offset, idErr := strconv.ParseInt(strings.TrimSpace(id), 10, 64)
	if idErr != nil {
		offset = 0
	}

	switch ports.EventKind(kind) {
	case ports.KindHeartbeat:
		return ports.Event{Kind: ports.KindHeartbeat, Offset: offset}, true
	case ports.KindDetection, ports.KindUpdate, ports.KindRetract:
	default:
		if offset == 0 {
			return ports.Event{}, false
		}
		// An event kind this version does not know: move past it.
		return ports.Event{Offset: offset, Skipped: "unknown_kind"}, true
	}
	if offset == 0 {
		// Without an offset the event cannot be checkpointed or deduplicated.
		return ports.Event{Skipped: "missing_offset"}, true
	}
	if fields == nil {
		return ports.Event{Offset: offset, Skipped: "undecodable"}, true
	}
	fields = unwrap(fields)

	if ports.EventKind(kind) == ports.KindRetract {
		key := text(fields["key"])
		if key == "" {
			key = text(fields["detection_key"])
		}
		if key == "" {
			if detection, ok := detectionOf(fields); ok {
				key = detection.Key()
			}
		}
		if key == "" {
			return ports.Event{Offset: offset, Skipped: "undecodable"}, true
		}
		return ports.Event{Offset: offset, Kind: ports.KindRetract, DetectionKey: key}, true
	}
	detection, ok := detectionOf(fields)
	if !ok {
		return ports.Event{Offset: offset, Skipped: "undecodable"}, true
	}
	detection.Confidence = domain.NormalizeConfidence(text(fields["confidence"]))
	detection.FRP, _ = strconv.ParseFloat(text(fields["frp"]), 64)
	return ports.Event{Offset: offset, Kind: ports.EventKind(kind), Detection: detection}, true
}

func objectFields(payload []byte) map[string]json.RawMessage {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil
	}
	return fields
}

// unwrap follows one level of nesting when the detection sits under a
// wrapper key instead of at the top level.
func unwrap(fields map[string]json.RawMessage) map[string]json.RawMessage {
	if _, flat := fields["satellite"]; flat {
		return fields
	}
	for _, name := range []string{"detection", "data", "payload"} {
		if inner := objectFields(fields[name]); inner != nil {
			for key, value := range fields {
				if _, exists := inner[key]; !exists && key != name {
					inner[key] = value
				}
			}
			return inner
		}
	}
	return fields
}

// detectionOf reads the identity of a detection. Coordinates and the
// acquisition time keep their exact source text, because the audit contract
// builds detection keys from the feed's values without rounding.
func detectionOf(fields map[string]json.RawMessage) (domain.Detection, bool) {
	detection := domain.Detection{
		Satellite:    domain.Satellite(text(fields["satellite"])),
		AcqDate:      text(fields["acq_date"]),
		AcqTime:      text(fields["acq_time"]),
		RawLatitude:  text(fields["latitude"]),
		RawLongitude: text(fields["longitude"]),
	}
	var errLat, errLon error
	detection.Latitude, errLat = strconv.ParseFloat(detection.RawLatitude, 64)
	detection.Longitude, errLon = strconv.ParseFloat(detection.RawLongitude, 64)
	if detection.Satellite == "" || detection.AcqDate == "" || detection.AcqTime == "" || errLat != nil || errLon != nil {
		return domain.Detection{}, false
	}
	if !(detection.Latitude >= -90 && detection.Latitude <= 90) || !(detection.Longitude >= -180 && detection.Longitude <= 180) {
		return domain.Detection{}, false
	}
	return detection, true
}

// text returns a JSON scalar as its source text: a string without quotes, a
// number exactly as written.
func text(raw json.RawMessage) string {
	value := strings.TrimSpace(string(raw))
	if value == "" || value == "null" {
		return ""
	}
	if value[0] == '"' {
		var decoded string
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return ""
		}
		return strings.TrimSpace(decoded)
	}
	if value[0] == '{' || value[0] == '[' {
		return ""
	}
	return value
}

func clip(value string) string {
	if len(value) > 200 {
		return value[:200]
	}
	return value
}
