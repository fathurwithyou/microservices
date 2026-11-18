// Package sensor implements the outbound HTTP source for coastal sensor
// readings. The vendor image exposes two read-only endpoints:
//
//	GET /v1/stations      station registry with coordinates and province
//	GET /v1/observations  paged observation stream, cursor in next_cursor
//
// An empty URL selects fixture mode: Fetch returns no readings without making
// a request.
//
// The sensor is a black box that is expected to misbehave, so nothing it sends
// is trusted to be well formed. A response is split into items without
// validating them (scan.go), and every item is then decoded on its own
// (decode.go): one broken item becomes one quarantined reading and never costs
// the page it arrived in, because the readings behind it may include the one
// tsunami warning that matters.
package sensor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/ports"
)

const (
	// maxResponseBytes bounds one JSON response so a broken upstream cannot
	// exhaust the ingestor heap.
	maxResponseBytes = 8 << 20
	// maxItemBytes bounds one observation. A contract item is ~200 bytes;
	// anything near this size is garbage, and only its head is kept.
	maxItemBytes = 64 << 10
	// oversizedPayloadBytes is how much of an oversized item is quarantined.
	oversizedPayloadBytes = 4 << 10
	// maxItemsPerPage bounds the readings one response can produce (8 MiB of
	// "null," would otherwise become two million quarantine records). A page
	// beyond it is treated as damaged: its first items are processed and the
	// cursor does not advance.
	maxItemsPerPage = 20_000
	// maxPagesPerFetch bounds one poll cycle while the ingestor catches up.
	maxPagesPerFetch = 64
	// observedSlack is how far observed_at may run ahead of generated_at and
	// still count as the newest data of the feed.
	observedSlack = 5 * time.Minute
	// stationRefreshEvery throttles registry reloads triggered by unknown
	// station IDs, so a stream of garbage station_ids cannot double the
	// request rate towards an unhealthy sensor.
	stationRefreshEvery = time.Minute
	// maxCriticalFirst bounds the first-receipt memory for critical readings.
	maxCriticalFirst = 4096
	// clockTolerance absorbs jitter in the sensor's generated_at.
	clockTolerance = time.Second
	// forgetAfter is how long the stored cursor may keep answering "nothing
	// new" before it is treated as forgotten without clock evidence (the
	// ingestor itself restarted and has not seen the sensor clock yet). A
	// healthy sensor produces observations every few seconds.
	forgetAfter = 60 * time.Second
)

// cursorResets counts cursors the sensor no longer recognised.
var cursorResets = promauto.NewCounter(prometheus.CounterOpts{
	Name: "coastal_sensor_cursor_resets_total",
	Help: "Times the sensor was found restarted (its generated_at clock went backwards, or the stored cursor stayed empty for a minute) and the stream was re-read from the start.",
})

// sensorRequests counts every HTTP request sent to the sensor by outcome, so
// a breaker that stops retrying an unhealthy sensor is visible as a flat line.
var sensorRequests = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "coastal_sensor_requests_total",
	Help: "HTTP requests sent to Sensor Pantai, by outcome.",
}, []string{"outcome"})

// Client fetches readings from the Sensor Pantai HTTP contract.
type Client struct {
	base  string
	token string
	http  *http.Client

	stations          map[string]station
	loaded            bool
	stationsFetchedAt time.Time
	cursor            string
	// tailSeen holds the dedupe keys of the page at tailCursor, the page
	// that is fetched again next cycle because the cursor did not move past
	// it (the tail of the stream, or a damaged page).
	tailSeen   map[string]struct{}
	tailCursor string

	// generated is the newest sensor clock (generated_at) seen; a restarted
	// sensor starts its virtual clock over, so the value moves backwards.
	generated time.Time
	// newestObserved is the newest plausible observed_at of the current
	// stream; atTail records whether the last Fetch reached its end.
	newestObserved time.Time
	atTail         bool
	// emptySince is when the stored cursor first answered "nothing new" in
	// the current run of empty answers; zero while pages carry items.
	emptySince time.Time
	// criticalFirst remembers when each critical observation was first
	// received. A critical reading carries its receipt time downstream (for
	// the latency measurement), and the Aggregator treats changed attributes
	// as a changed event; without this memory every repeated read of the
	// stream (cursor forgotten, sensor merely silent) would re-notify Tim
	// Lapangan about every old critical observation. It is dropped when the
	// sensor's clock proves a restart, because a restarted sensor renumbers
	// its stream and a reused ID may be a different observation.
	criticalFirst map[string]firstReceipt
	now           func() time.Time
}

type firstReceipt struct {
	receivedAt  time.Time
	generatedAt time.Time
}

type station struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
}

// New builds a client. An empty url selects fixture mode.
func New(baseURL, token string, timeout time.Duration) *Client {
	return &Client{
		base:          strings.TrimRight(baseURL, "/"),
		token:         token,
		http:          &http.Client{Timeout: timeout},
		stations:      map[string]station{},
		tailSeen:      map[string]struct{}{},
		criticalFirst: map[string]firstReceipt{},
		now:           time.Now,
	}
}

// Fetch walks the observation pages from the last committed cursor and
// returns only observations not seen in the current tail page.
//
// Readings and an error can come back together. When a page fails after
// earlier pages were read, the earlier pages' readings are returned and the
// cursor stays behind the failed page. When a page is damaged (cut short,
// structurally broken, absurdly large) its complete items are returned too
// and the cursor does not move past it: the cursor only ever advances on a
// next_cursor read from a structurally complete response.
func (c *Client) Fetch(ctx context.Context) ([]domain.Reading, error) {
	if c.base == "" {
		return nil, nil
	}
	if err := c.loadStations(ctx); err != nil {
		return nil, err
	}

	c.atTail = false
	seen := make(map[string]struct{}, len(c.tailSeen))
	for key := range c.tailSeen {
		seen[key] = struct{}{}
	}
	var readings []domain.Reading
	for fetched := 0; fetched < maxPagesPerFetch; fetched++ {
		cursor := c.cursor
		result, pageErr := c.fetchPage(ctx, cursor)
		if pageErr != nil && !result.damaged {
			// Nothing usable came back (transport, status, not an object).
			return readings, pageErr
		}
		// next_cursor is the offset after the last item, so an empty page with
		// no next cursor is the normal "caught up" answer, not a lost cursor.
		// A restarted sensor (which forgets every cursor and renumbers its
		// stream) is recognised by its own clock going backwards; without
		// that evidence, a cursor that stays empty for forgetAfter is treated
		// as forgotten. Either way the stream is walked again from the start;
		// the Aggregator upserts readings by observation, so unchanged ones
		// produce no new event.
		restarted := !result.generated.IsZero() && !c.generated.IsZero() && result.generated.Before(c.generated.Add(-clockTolerance))
		if !result.generated.IsZero() && (restarted || result.generated.After(c.generated)) {
			c.generated = result.generated
		}
		if restarted {
			c.criticalFirst = map[string]firstReceipt{}
			c.newestObserved = time.Time{}
		}
		forgotten := false
		if pageErr == nil && cursor != "" && len(result.items) == 0 && !result.hasNext {
			if c.emptySince.IsZero() {
				c.emptySince = c.now()
			}
			forgotten = c.now().Sub(c.emptySince) >= forgetAfter
		} else if len(result.items) > 0 {
			c.emptySince = time.Time{}
		}
		if cursor != "" && (restarted || forgotten) {
			cursorResets.Inc()
			c.cursor = ""
			c.emptySince = time.Time{}
			c.newestObserved = time.Time{}
			c.tailSeen = map[string]struct{}{}
			seen = map[string]struct{}{}
			continue
		}

		decoded := make([]domain.Reading, 0, len(result.items))
		unknownStation := false
		for _, raw := range result.items {
			reading := c.decodeItem(raw, result)
			if reading.SensorID != "" && reading.AreaName == "" {
				unknownStation = true
			}
			decoded = append(decoded, reading)
		}
		if unknownStation && c.refreshStationsThrottled(ctx) {
			for i := range decoded {
				c.locate(&decoded[i])
			}
		}

		pageKeys := make(map[string]struct{}, len(decoded))
		for _, reading := range decoded {
			key := dedupeKey(reading)
			pageKeys[key] = struct{}{}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			c.remember(&reading)
			readings = append(readings, reading)
		}

		if pageErr != nil {
			// Damaged page: it is fetched again, so remember what was already
			// handed out. Keys accumulate while the same page keeps arriving
			// damaged; maxItemsPerPage bounds them.
			if c.tailCursor == cursor {
				for key := range c.tailSeen {
					pageKeys[key] = struct{}{}
				}
			}
			c.tailSeen, c.tailCursor = pageKeys, cursor
			return readings, pageErr
		}
		// The page just processed is the one that will be re-fetched next
		// cycle when it is the tail. Keeping only its keys bounds memory.
		c.tailSeen, c.tailCursor = pageKeys, cursor
		if !result.hasNext {
			c.atTail = true
			break
		}
		c.cursor = result.next
	}
	return readings, nil
}

// Cursor returns the page cursor that will be requested next.
func (c *Client) Cursor() string { return c.cursor }

// SetCursor seeds the page cursor after a restart.
func (c *Client) SetCursor(cursor string) {
	c.cursor = cursor
	c.tailSeen = map[string]struct{}{}
	c.tailCursor = ""
	c.emptySince = time.Time{}
}

// FeedStatus reports the sensor clock and the newest data seen, for the
// feed-freshness gauge.
func (c *Client) FeedStatus() domain.FeedStatus {
	return domain.FeedStatus{GeneratedAt: c.generated, NewestObservedAt: c.newestObserved, AtTail: c.atTail}
}

// remember applies the per-stream memory to a reading about to be returned:
// the newest observed_at of the feed and the first receipt of a critical one.
func (c *Client) remember(reading *domain.Reading) {
	if reading.IdentityViolation != "" || reading.OccurredAt.IsZero() {
		return
	}
	// Only a timestamp the sensor's own clock makes plausible may move the
	// freshness reference; one garbage far-future value would pin it.
	if !reading.GeneratedAt.IsZero() && !reading.OccurredAt.After(reading.GeneratedAt.Add(observedSlack)) && reading.OccurredAt.After(c.newestObserved) {
		c.newestObserved = reading.OccurredAt
	}
	if !reading.Critical || reading.ID == "" {
		return
	}
	key := reading.ID + "\x00" + reading.OccurredAt.UTC().Format(time.RFC3339Nano)
	if first, ok := c.criticalFirst[key]; ok {
		reading.ReceivedAt, reading.GeneratedAt = first.receivedAt, first.generatedAt
		return
	}
	if len(c.criticalFirst) >= maxCriticalFirst {
		// Forgetting everything is safe: the cost is at most one repeated
		// notification per old critical observation.
		c.criticalFirst = map[string]firstReceipt{}
	}
	c.criticalFirst[key] = firstReceipt{receivedAt: reading.ReceivedAt, generatedAt: reading.GeneratedAt}
}

func (c *Client) loadStations(ctx context.Context) error {
	if c.loaded {
		return nil
	}
	return c.refreshStations(ctx)
}

// refreshStationsThrottled reloads the registry at most once per
// stationRefreshEvery and reports whether it changed. A failed reload keeps
// the registry already loaded: observations of known stations must keep
// flowing while /v1/stations misbehaves.
func (c *Client) refreshStationsThrottled(ctx context.Context) bool {
	if c.now().Sub(c.stationsFetchedAt) < stationRefreshEvery {
		return false
	}
	return c.refreshStations(ctx) == nil
}

func (c *Client) refreshStations(ctx context.Context) (err error) {
	c.stationsFetchedAt = c.now()
	defer func() { countRequest(err) }()
	endpoint, err := c.endpoint("/v1/stations")
	if err != nil {
		return err
	}
	body, _, err := c.get(ctx, endpoint)
	if err != nil {
		return err
	}
	stations, err := decodeStations(body)
	if err != nil {
		return err
	}
	c.stations = stations
	c.loaded = true
	return nil
}

func (c *Client) fetchPage(ctx context.Context, cursor string) (result page, err error) {
	defer func() { countRequest(err) }()
	endpoint, err := c.endpoint("/v1/observations")
	if err != nil {
		return page{}, err
	}
	if cursor != "" {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return page{}, fmt.Errorf("%w: %v", ports.ErrSourcePayload, err)
		}
		query := parsed.Query()
		query.Set("cursor", cursor)
		parsed.RawQuery = query.Encode()
		endpoint = parsed.String()
	}
	body, receivedAt, getErr := c.get(ctx, endpoint)
	if getErr != nil && body == nil {
		return page{}, getErr
	}
	result, err = decodePage(body)
	result.receivedAt = receivedAt
	if getErr != nil {
		// The body was cut by the transport or by our size cap: whatever
		// decoded is salvage, and the page must not advance the cursor.
		result.damaged = true
		result.hasNext, result.next = false, ""
		return result, getErr
	}
	return result, err
}

func (c *Client) endpoint(path string) (string, error) {
	parsed, err := url.Parse(c.base)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ports.ErrSourcePayload, err)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + path
	return parsed.String(), nil
}

// get performs one request and returns the body with the wall-clock time it
// finished arriving. A body that was cut short (read error after the status
// line, or longer than maxResponseBytes) is returned together with an
// ErrSourcePayload so the caller can salvage its complete items.
func (c *Client) get(ctx context.Context, endpoint string) ([]byte, time.Time, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("%w: %v", ports.ErrSourcePayload, err)
	}
	request.Header.Set("Accept", "application/json")
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("%w: %v", ports.ErrSourceUnavailable, err)
	}
	defer response.Body.Close()

	switch {
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		return nil, time.Time{}, fmt.Errorf("%w: status %d", ports.ErrSourceUnauthorized, response.StatusCode)
	case response.StatusCode < 200 || response.StatusCode >= 300:
		return nil, time.Time{}, fmt.Errorf("%w: status %d", ports.ErrSourceUnavailable, response.StatusCode)
	}
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	// Taken once per response, immediately after its last byte: this is the
	// "received" instant every reading of the page is measured from.
	receivedAt := c.now().UTC()
	if body == nil {
		body = []byte{}
	}
	switch {
	case readErr != nil:
		return body, receivedAt, fmt.Errorf("%w: body cut short: %v", ports.ErrSourcePayload, readErr)
	case len(body) > maxResponseBytes:
		return body[:maxResponseBytes], receivedAt, fmt.Errorf("%w: body exceeds %d bytes", ports.ErrSourcePayload, maxResponseBytes)
	}
	return body, receivedAt, nil
}

// countRequest records one request to the sensor by outcome.
func countRequest(err error) {
	outcome := "ok"
	switch {
	case err == nil:
	case errors.Is(err, ports.ErrSourceUnauthorized):
		outcome = "unauthorized"
	case errors.Is(err, ports.ErrSourcePayload):
		outcome = "bad_payload"
	default:
		outcome = "unavailable"
	}
	sensorRequests.WithLabelValues(outcome).Inc()
}

// decodeStations reads the station registry leniently: a station that cannot
// be decoded is skipped instead of costing the whole registry. An answer
// without a single usable station is an error, because adopting it would
// make every observation (critical ones included) an unknown-station reject.
func decodeStations(body []byte) (map[string]station, error) {
	var envelope struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("%w: stations: %v", ports.ErrSourcePayload, err)
	}
	stations := make(map[string]station, len(envelope.Items))
	for _, raw := range envelope.Items {
		var item struct {
			station
			StationID string `json:"station_id"`
		}
		if json.Unmarshal(raw, &item) != nil || item.StationID == "" {
			continue
		}
		stations[item.StationID] = item.station
	}
	if len(stations) == 0 {
		return nil, fmt.Errorf("%w: stations: no usable station", ports.ErrSourcePayload)
	}
	return stations, nil
}
