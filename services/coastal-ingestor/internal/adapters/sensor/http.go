// Package sensor implements the outbound HTTP source for coastal sensor
// readings. The vendor image exposes two read-only endpoints:
//
//	GET /v1/stations      station registry with coordinates and province
//	GET /v1/observations  paged observation stream, cursor in next_cursor
//
// An empty URL selects fixture mode: Fetch returns no readings without making
// a request.
package sensor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/domain"
	"github.com/fathurwithyou/microservices/services/coastal-ingestor/internal/ports"
)

const (
	// maxResponseBytes bounds one JSON response so a broken upstream cannot
	// exhaust the ingestor heap.
	maxResponseBytes = 8 << 20
	// maxPagesPerFetch bounds one poll cycle while the ingestor catches up.
	maxPagesPerFetch = 64
)

// cursorResets counts cursors the sensor no longer recognised.
var cursorResets = promauto.NewCounter(prometheus.CounterOpts{
	Name: "coastal_sensor_cursor_resets_total",
	Help: "Times the sensor answered the stored cursor with an empty final page (sensor restart or reset) and the stream was re-read from the start.",
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

	stations map[string]station
	loaded   bool
	cursor   string
	tailSeen map[string]struct{}
}

type station struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Name      string  `json:"name"`
	Province  string  `json:"province"`
}

type observation struct {
	ID            string  `json:"observation_id"`
	StationID     string  `json:"station_id"`
	ObservedAt    string  `json:"observed_at"`
	Quality       string  `json:"quality"`
	WaveHeightM   float64 `json:"wave_height_m"`
	SeaLevelAnomM float64 `json:"sea_level_anomaly_m"`
	Critical      bool    `json:"critical"`
}

type observationPage struct {
	GeneratedAt string        `json:"generated_at"`
	Items       []observation `json:"items"`
	NextCursor  *string       `json:"next_cursor"`
}

type stationPage struct {
	Items []struct {
		station
		StationID string `json:"station_id"`
	} `json:"items"`
}

// New builds a client. An empty url selects fixture mode.
func New(baseURL, token string, timeout time.Duration) *Client {
	return &Client{
		base:     strings.TrimRight(baseURL, "/"),
		token:    token,
		http:     &http.Client{Timeout: timeout},
		stations: map[string]station{},
		tailSeen: map[string]struct{}{},
	}
}

// Fetch walks the observation pages from the last committed cursor and
// returns only observations not seen in the current tail page.
func (c *Client) Fetch(ctx context.Context) ([]domain.Reading, error) {
	if c.base == "" {
		return nil, nil
	}
	if err := c.loadStations(ctx); err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(c.tailSeen))
	for id := range c.tailSeen {
		seen[id] = struct{}{}
	}
	var readings []domain.Reading
	for page := 0; page < maxPagesPerFetch; page++ {
		cursor := c.cursor
		result, err := c.fetchPage(ctx, cursor)
		if err != nil {
			return nil, err
		}
		if cursor != "" && len(result.Items) == 0 && result.NextCursor == nil {
			// The tail page is always re-fetched, so a healthy sensor never
			// answers our cursor with an empty last page. An empty one means
			// the cursor points past the sensor's stream: it restarted or
			// reset and forgot the position while still reporting healthy.
			// Walk again from the start; the Aggregator upserts readings by
			// observation, so unchanged ones produce no new event.
			cursorResets.Inc()
			c.cursor = ""
			c.tailSeen = map[string]struct{}{}
			seen = map[string]struct{}{}
			continue
		}
		if refreshStations(c.stations, result.Items) {
			if err := c.refreshStations(ctx); err != nil {
				return nil, err
			}
		}
		pageIDs := make(map[string]struct{}, len(result.Items))
		for _, item := range result.Items {
			pageIDs[item.ID] = struct{}{}
			if _, ok := seen[item.ID]; ok {
				continue
			}
			seen[item.ID] = struct{}{}
			readings = append(readings, c.reading(item))
		}
		// The page just processed is the one that will be re-fetched next
		// cycle when it is the tail. Keeping only its IDs bounds memory.
		c.tailSeen = pageIDs
		if result.NextCursor == nil {
			break
		}
		c.cursor = *result.NextCursor
	}
	return readings, nil
}

// Cursor returns the page cursor that will be requested next.
func (c *Client) Cursor() string { return c.cursor }

// SetCursor seeds the page cursor after a restart.
func (c *Client) SetCursor(cursor string) {
	c.cursor = cursor
	c.tailSeen = map[string]struct{}{}
}

func (c *Client) loadStations(ctx context.Context) error {
	if c.loaded {
		return nil
	}
	return c.refreshStations(ctx)
}

func (c *Client) refreshStations(ctx context.Context) error {
	endpoint, err := c.endpoint("/v1/stations")
	if err != nil {
		return err
	}
	var page stationPage
	if err := c.getJSON(ctx, endpoint, &page); err != nil {
		return err
	}
	stations := make(map[string]station, len(page.Items))
	for _, item := range page.Items {
		stations[item.StationID] = item.station
	}
	c.stations = stations
	c.loaded = true
	return nil
}

func (c *Client) fetchPage(ctx context.Context, cursor string) (observationPage, error) {
	endpoint, err := c.endpoint("/v1/observations")
	if err != nil {
		return observationPage{}, err
	}
	if cursor != "" {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return observationPage{}, fmt.Errorf("%w: %v", ports.ErrSourcePayload, err)
		}
		query := parsed.Query()
		query.Set("cursor", cursor)
		parsed.RawQuery = query.Encode()
		endpoint = parsed.String()
	}
	var page observationPage
	if err := c.getJSON(ctx, endpoint, &page); err != nil {
		return observationPage{}, err
	}
	return page, nil
}

func (c *Client) reading(item observation) domain.Reading {
	observedAt, err := time.Parse(time.RFC3339, item.ObservedAt)
	if err != nil {
		observedAt = time.Time{}
	}
	located := c.stations[item.StationID]
	severity := domain.SeverityNormal
	if item.Critical {
		severity = domain.SeverityAwas
	}
	payload, _ := json.Marshal(item)
	return domain.Reading{
		ID:         item.ID,
		SensorID:   item.StationID,
		AreaName:   located.Province,
		Quality:    item.Quality,
		OccurredAt: observedAt,
		ReceivedAt: time.Now().UTC(),
		Latitude:   located.Latitude,
		Longitude:  located.Longitude,
		Severity:   severity,
		Critical:   item.Critical,
		Payload:    payload,
	}
}

func (c *Client) endpoint(path string) (string, error) {
	parsed, err := url.Parse(c.base)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ports.ErrSourcePayload, err)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + path
	return parsed.String(), nil
}

func (c *Client) getJSON(ctx context.Context, endpoint string, target any) (err error) {
	defer func() {
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
	}()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return fmt.Errorf("%w: %v", ports.ErrSourcePayload, err)
	}
	request.Header.Set("Accept", "application/json")
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %v", ports.ErrSourceUnavailable, err)
	}
	defer response.Body.Close()

	switch {
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		return fmt.Errorf("%w: status %d", ports.ErrSourceUnauthorized, response.StatusCode)
	case response.StatusCode < 200 || response.StatusCode >= 300:
		return fmt.Errorf("%w: status %d", ports.ErrSourceUnavailable, response.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, maxResponseBytes)).Decode(target); err != nil {
		return fmt.Errorf("%w: %v", ports.ErrSourcePayload, err)
	}
	return nil
}

// refreshStations reports whether the page references an unknown station.
func refreshStations(known map[string]station, items []observation) bool {
	for _, item := range items {
		if _, ok := known[item.StationID]; !ok {
			return true
		}
	}
	return false
}
