package application

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

// Outcome says what one feed event did to the working set.
type Outcome string

const (
	OutcomeAdded       Outcome = "added"
	OutcomeUpdated     Outcome = "updated"
	OutcomeDuplicate   Outcome = "duplicate"
	OutcomeRemoved     Outcome = "removed"
	OutcomeLow         Outcome = "low_confidence"
	OutcomeLate        Outcome = "late"
	OutcomeInvalidTime Outcome = "invalid_time"
	OutcomeFuture      Outcome = "future"
	OutcomeUnknownKey  Outcome = "retract_unknown"
)

type member struct {
	detection domain.Detection
	key       string
	at        time.Time
	cell      gridCell
	comp      *component
}

// component is one set of transitively connected detections. It is a fire
// when it has at least two members; fireID is empty otherwise.
type component struct {
	members map[string]*member
	fireID  string
}

// Engine keeps the detections that can still change and maintains their
// connected components one event at a time.
//
// Every event is applied on its own, in feed order, so the fires and their
// identifiers are a function of the event sequence alone. How the stream was
// cut into batches, where a connection dropped, or where the process died and
// replayed from its checkpoint cannot change the result (P4 criteria 5, 8).
//
// Identity: a fire keeps its source_ref_id while it grows, shrinks, or absorbs
// another fire. When two fires merge, the larger one's identifier survives and
// the other is retracted. When a retraction splits a fire, the larger part
// keeps the identifier. A new fire is named after its earliest detection.
type Engine struct {
	members map[string]*member
	grid    map[gridCell]map[string]*member
	comps   map[*component]struct{}
	fires   map[string]*component

	// last is the content most recently sent for every fire id this engine
	// knows, including retracted tombstones.
	last map[string]domain.Fire

	// Changes since the last Drain.
	touched map[string]struct{}
	// final is the content each retired identifier had just before it was
	// withdrawn.
	final      map[string]domain.Fire
	closed     []domain.Fire
	detections map[string]bool // key -> true: upsert, false: delete

	watermark  time.Time
	lateWindow time.Duration
	// horizon, when set, rejects acquisition times further ahead than this:
	// one corrupt timestamp must not drag the watermark past everything.
	horizon time.Time

	finalizedFires   int64
	finalizedSingles int64
}

// NewEngine returns an empty engine with the accepted lateness.
func NewEngine(lateWindow time.Duration) *Engine {
	return &Engine{
		members:    map[string]*member{},
		grid:       map[gridCell]map[string]*member{},
		comps:      map[*component]struct{}{},
		fires:      map[string]*component{},
		last:       map[string]domain.Fire{},
		touched:    map[string]struct{}{},
		final:      map[string]domain.Fire{},
		detections: map[string]bool{},
		lateWindow: lateWindow,
	}
}

// Restore rebuilds the engine from durable state. fires are the active fires
// as last committed; a component takes the identifier of the stored fire that
// owns its detections, so identifiers survive a restart unchanged.
func (e *Engine) Restore(detections []domain.Detection, fires []domain.Fire, watermark time.Time) {
	owner := map[string]string{}
	for _, fire := range fires {
		if fire.Status != domain.StatusActive {
			continue
		}
		e.last[fire.SourceRefID] = fire
		for _, key := range fire.DetectionKeys {
			owner[key] = fire.SourceRefID
		}
	}
	for _, detection := range detections {
		at, err := detection.Time()
		if err != nil || detection.Ignored() {
			continue
		}
		e.link(&member{detection: detection, key: detection.Key(), at: at})
	}
	for comp := range e.comps {
		if len(comp.members) < 2 {
			continue
		}
		votes := map[string]int{}
		for key := range comp.members {
			if id := owner[key]; id != "" {
				votes[id]++
			}
		}
		best := ""
		for id, count := range votes {
			if _, taken := e.fires[id]; taken {
				continue
			}
			if best == "" || count > votes[best] || (count == votes[best] && id < best) {
				best = id
			}
		}
		if best == "" {
			best = e.newID(comp)
		}
		comp.fireID = best
		e.fires[best] = comp
	}
	// A stored fire without a component, or a component whose content moved,
	// is reconciled by the first Drain.
	for id := range e.last {
		e.touched[id] = struct{}{}
	}
	for id := range e.fires {
		e.touched[id] = struct{}{}
	}
	e.watermark = watermark
	e.detections = map[string]bool{}
}

// SetHorizon bounds acceptable acquisition times (feed virtual now plus a
// margin). The zero time disables the bound.
func (e *Engine) SetHorizon(horizon time.Time) { e.horizon = horizon }

// Watermark is the latest acquisition time seen on the stream.
func (e *Engine) Watermark() time.Time { return e.watermark }

// Upsert applies a detection or update event.
//
// Lateness is judged in event time: a detection whose acquisition time is
// more than lateWindow behind the watermark is not admitted. The watermark
// advances with every well-formed detection, whatever its confidence.
func (e *Engine) Upsert(detection domain.Detection, offset int64) Outcome {
	at, err := detection.Time()
	if err != nil {
		return OutcomeInvalidTime
	}
	if !e.horizon.IsZero() && at.After(e.horizon) {
		return OutcomeFuture
	}
	if !e.watermark.IsZero() && e.watermark.Sub(at) > e.lateWindow {
		return OutcomeLate
	}
	if at.After(e.watermark) {
		e.watermark = at
	}

	key := detection.Key()
	existing := e.members[key]
	if detection.Ignored() {
		if existing != nil {
			e.remove(existing)
		}
		return OutcomeLow
	}
	if existing != nil {
		// Same key means same satellite, time and position, so only the
		// measured values can differ.
		if existing.detection.Confidence == detection.Confidence && existing.detection.FRP == detection.FRP {
			return OutcomeDuplicate
		}
		detection.FirstOffset = existing.detection.FirstOffset
		existing.detection = detection
		e.detections[key] = true
		if existing.comp.fireID != "" {
			e.touched[existing.comp.fireID] = struct{}{}
		}
		return OutcomeUpdated
	}
	detection.FirstOffset = offset
	fresh := &member{detection: detection, key: key, at: at}
	e.link(fresh)
	e.detections[key] = true
	e.identify(fresh.comp)
	return OutcomeAdded
}

// Retract applies a retract event. Retractions are never late: the feed may
// withdraw any detection it still knows.
func (e *Engine) Retract(key string) Outcome {
	existing := e.members[key]
	if existing == nil {
		return OutcomeUnknownKey
	}
	e.remove(existing)
	return OutcomeRemoved
}

// link inserts m and merges every component it touches, without deciding
// identity yet.
func (e *Engine) link(m *member) {
	m.cell = cellOf(m.detection.Latitude, m.detection.Longitude)
	var touching []*component
	seen := map[*component]bool{}
	for dLat := -1; dLat <= 1; dLat++ {
		for dLon := -1; dLon <= 1; dLon++ {
			for _, other := range e.grid[gridCell{m.cell.lat + dLat, m.cell.lon + dLon}] {
				if !seen[other.comp] && linked(m, other) {
					seen[other.comp] = true
					touching = append(touching, other.comp)
				}
			}
		}
	}
	bucket := e.grid[m.cell]
	if bucket == nil {
		bucket = map[string]*member{}
		e.grid[m.cell] = bucket
	}
	bucket[m.key] = m
	e.members[m.key] = m

	if len(touching) == 0 {
		comp := &component{members: map[string]*member{m.key: m}}
		m.comp = comp
		e.comps[comp] = struct{}{}
		return
	}
	// Decide which fire identity survives before sizes change.
	sort.Slice(touching, func(i, j int) bool { return precedes(touching[i], touching[j]) })
	target := touching[0]
	for _, other := range touching[1:] {
		if other.fireID != "" {
			if target.fireID == "" {
				// Cannot happen while precedes puts fires first, kept for safety.
				target.fireID = other.fireID
				e.fires[other.fireID] = target
			} else {
				e.retire(other.fireID, buildFire(other.fireID, other))
			}
		}
		for key, moved := range other.members {
			moved.comp = target
			target.members[key] = moved
		}
		delete(e.comps, other)
	}
	target.members[m.key] = m
	m.comp = target
}

// precedes orders components by who keeps its identity in a merge: fires
// before singles, then more detections, then the smaller identifier.
func precedes(a, b *component) bool {
	if (a.fireID != "") != (b.fireID != "") {
		return a.fireID != ""
	}
	if len(a.members) != len(b.members) {
		return len(a.members) > len(b.members)
	}
	if a.fireID != b.fireID {
		return a.fireID < b.fireID
	}
	return minKey(a) < minKey(b)
}

func minKey(comp *component) string {
	best := ""
	for key := range comp.members {
		if best == "" || key < best {
			best = key
		}
	}
	return best
}

// identify gives comp a fire identifier when it has become a fire and marks
// it changed.
func (e *Engine) identify(comp *component) {
	if len(comp.members) < 2 {
		return
	}
	if comp.fireID == "" {
		comp.fireID = e.newID(comp)
		e.fires[comp.fireID] = comp
	}
	e.touched[comp.fireID] = struct{}{}
}

// newID names a fire after its earliest detection. If that name belongs to a
// live fire (the detection once anchored a fire it has since been split
// from), the next detection is used.
func (e *Engine) newID(comp *component) string {
	ordered := orderedMembers(comp)
	for _, m := range ordered {
		id := fireID(m.key)
		if _, live := e.fires[id]; !live {
			return id
		}
	}
	for n := 2; ; n++ {
		id := fireID(ordered[0].key + "#" + strconv.Itoa(n))
		if _, live := e.fires[id]; !live {
			return id
		}
	}
}

func fireID(anchor string) string {
	sum := sha256.Sum256([]byte(anchor))
	return "fire-" + hex.EncodeToString(sum[:8])
}

// retire withdraws a fire identifier. content is the fire as it was just
// before it stopped existing; the tombstone is built from it, so what a
// retracted event says never depends on when the previous send happened.
func (e *Engine) retire(id string, content domain.Fire) {
	e.final[id] = content
	delete(e.fires, id)
	e.touched[id] = struct{}{}
}

func (e *Engine) remove(m *member) {
	comp := m.comp
	var before domain.Fire
	if comp.fireID != "" {
		before = buildFire(comp.fireID, comp)
	}
	e.unlink(m)
	e.detections[m.key] = false
	if len(comp.members) == 0 {
		delete(e.comps, comp)
		return
	}
	parts := e.split(comp)
	if len(parts) == 1 {
		if comp.fireID == "" {
			return
		}
		if len(comp.members) < 2 {
			e.retire(comp.fireID, before)
			comp.fireID = ""
			return
		}
		e.touched[comp.fireID] = struct{}{}
		return
	}
	// The retraction cut the component: the biggest part keeps the identity.
	inherited := comp.fireID
	delete(e.comps, comp)
	if inherited != "" {
		delete(e.fires, inherited)
	}
	sort.Slice(parts, func(i, j int) bool {
		if len(parts[i]) != len(parts[j]) {
			return len(parts[i]) > len(parts[j])
		}
		return smallestKey(parts[i]) < smallestKey(parts[j])
	})
	fresh := make([]*component, len(parts))
	for index, part := range parts {
		next := &component{members: make(map[string]*member, len(part))}
		for _, moved := range part {
			moved.comp = next
			next.members[moved.key] = moved
		}
		e.comps[next] = struct{}{}
		fresh[index] = next
	}
	if inherited != "" {
		if len(fresh[0].members) >= 2 {
			fresh[0].fireID = inherited
			e.fires[inherited] = fresh[0]
			e.touched[inherited] = struct{}{}
		} else {
			e.retire(inherited, before)
		}
	}
	for _, next := range fresh {
		e.identify(next)
	}
}

func smallestKey(part []*member) string {
	best := part[0].key
	for _, m := range part[1:] {
		if m.key < best {
			best = m.key
		}
	}
	return best
}

func (e *Engine) unlink(m *member) {
	delete(e.members, m.key)
	delete(m.comp.members, m.key)
	if bucket := e.grid[m.cell]; bucket != nil {
		delete(bucket, m.key)
		if len(bucket) == 0 {
			delete(e.grid, m.cell)
		}
	}
}

// split returns the connected parts of comp's remaining members.
func (e *Engine) split(comp *component) [][]*member {
	visited := make(map[string]bool, len(comp.members))
	var parts [][]*member
	for _, start := range comp.members {
		if visited[start.key] {
			continue
		}
		visited[start.key] = true
		queue := []*member{start}
		var part []*member
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			part = append(part, current)
			for dLat := -1; dLat <= 1; dLat++ {
				for dLon := -1; dLon <= 1; dLon++ {
					for _, other := range e.grid[gridCell{current.cell.lat + dLat, current.cell.lon + dLon}] {
						if !visited[other.key] && linked(current, other) {
							visited[other.key] = true
							queue = append(queue, other)
						}
					}
				}
			}
		}
		parts = append(parts, part)
		if len(part) == len(comp.members) {
			break
		}
	}
	return parts
}

// Finalize closes every component that no admitted detection can reach any
// more: all its detections are older than the watermark by more than the
// accepted lateness plus the linking window. Closed detections leave the
// working set; the fire keeps its last content and is never retracted by
// ageing out.
func (e *Engine) Finalize() {
	if e.watermark.IsZero() {
		return
	}
	cutoff := e.watermark.Add(-e.lateWindow - clusterWindow)
	for comp := range e.comps {
		newest := time.Time{}
		for _, m := range comp.members {
			if m.at.After(newest) {
				newest = m.at
			}
		}
		if !newest.Before(cutoff) {
			continue
		}
		if comp.fireID != "" {
			fire := buildFire(comp.fireID, comp)
			fire.Status = domain.StatusClosed
			e.closed = append(e.closed, fire)
			delete(e.fires, comp.fireID)
			delete(e.touched, comp.fireID)
			e.finalizedFires++
		} else {
			e.finalizedSingles += int64(len(comp.members))
		}
		for _, m := range comp.members {
			delete(e.members, m.key)
			if bucket := e.grid[m.cell]; bucket != nil {
				delete(bucket, m.key)
				if len(bucket) == 0 {
					delete(e.grid, m.cell)
				}
			}
			e.detections[m.key] = false
		}
		delete(e.comps, comp)
	}
}

// Delta is what one batch changed.
type Delta struct {
	// Fires must be sent downstream: active fires whose content changed and
	// retracted tombstones.
	Fires []domain.Fire
	// Closed fires need no send; only their state row changes.
	Closed []domain.Fire
	// UpsertDetections and DeleteDetections are the working-set changes.
	UpsertDetections []domain.Detection
	DeleteDetections []string
}

// Drain returns the net effect of the events applied since the last Drain.
// Identifiers created and withdrawn inside the window are reported as
// retracted, exactly as if every event had been drained on its own.
func (e *Engine) Drain() Delta {
	var delta Delta
	ids := make([]string, 0, len(e.touched))
	for id := range e.touched {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		previous, known := e.last[id]
		if comp, live := e.fires[id]; live {
			fire := buildFire(id, comp)
			if known && previous.Status == domain.StatusActive && sameFire(previous, fire) {
				continue
			}
			e.last[id] = fire
			delta.Fires = append(delta.Fires, fire)
			continue
		}
		if known && previous.Status == domain.StatusRetracted {
			continue
		}
		tombstone := previous
		if content, recorded := e.final[id]; recorded {
			tombstone = content
		}
		tombstone.SourceRefID = id
		tombstone.Status = domain.StatusRetracted
		tombstone.DetectionCount = 0
		tombstone.DetectionKeys = nil
		tombstone.Satellites = nil
		tombstone.TotalFRP = 0
		e.last[id] = tombstone
		delta.Fires = append(delta.Fires, tombstone)
	}
	for _, fire := range e.closed {
		// A fire first seen and closed inside one window still has to exist
		// downstream with its final content.
		if previous, known := e.last[fire.SourceRefID]; !known || previous.Status != domain.StatusActive || !sameContent(previous, fire) {
			active := fire
			active.Status = domain.StatusActive
			delta.Fires = append(delta.Fires, active)
		}
		delete(e.last, fire.SourceRefID)
		delta.Closed = append(delta.Closed, fire)
	}
	keys := make([]string, 0, len(e.detections))
	for key := range e.detections {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if m := e.members[key]; m != nil && e.detections[key] {
			delta.UpsertDetections = append(delta.UpsertDetections, m.detection)
		} else if m == nil {
			delta.DeleteDetections = append(delta.DeleteDetections, key)
		}
	}
	e.touched = map[string]struct{}{}
	e.final = map[string]domain.Fire{}
	e.closed = nil
	e.detections = map[string]bool{}
	return delta
}

func sameContent(left, right domain.Fire) bool {
	left.Status, right.Status = "", ""
	return sameFire(left, right)
}

// Stats is the reportable size of the working set.
type Stats struct {
	Detections       int
	ActiveFires      int
	Singles          int
	FinalizedFires   int64
	FinalizedSingles int64
}

// Stats counts live detections, fires and lone detections.
func (e *Engine) Stats() Stats {
	stats := Stats{Detections: len(e.members), ActiveFires: len(e.fires), FinalizedFires: e.finalizedFires, FinalizedSingles: e.finalizedSingles}
	for comp := range e.comps {
		if len(comp.members) == 1 {
			stats.Singles++
		}
	}
	return stats
}

// SetFinalized restores the counters of closed fires and singles.
func (e *Engine) SetFinalized(fires, singles int64) {
	e.finalizedFires, e.finalizedSingles = fires, singles
}

// ActiveFires returns every live fire, ordered by identifier.
func (e *Engine) ActiveFires() []domain.Fire {
	fires := make([]domain.Fire, 0, len(e.fires))
	for id, comp := range e.fires {
		fires = append(fires, buildFire(id, comp))
	}
	sort.Slice(fires, func(i, j int) bool { return fires[i].SourceRefID < fires[j].SourceRefID })
	return fires
}

func orderedMembers(comp *component) []*member {
	ordered := make([]*member, 0, len(comp.members))
	for _, m := range comp.members {
		ordered = append(ordered, m)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if !ordered[i].at.Equal(ordered[j].at) {
			return ordered[i].at.Before(ordered[j].at)
		}
		return ordered[i].key < ordered[j].key
	})
	return ordered
}

// buildFire derives the HazardEvent content of a component. Sums run in a
// fixed order so the same detections always give the same numbers.
func buildFire(id string, comp *component) domain.Fire {
	ordered := orderedMembers(comp)
	keys := make([]string, len(ordered))
	seen := map[domain.Satellite]bool{}
	var satellites []domain.Satellite
	maxConfidence := ordered[0].detection.Confidence
	var totalFRP, latitude, longitude float64
	for index, m := range ordered {
		keys[index] = m.key
		latitude += m.detection.Latitude
		longitude += m.detection.Longitude
		totalFRP += m.detection.FRP
		if !seen[m.detection.Satellite] {
			seen[m.detection.Satellite] = true
			satellites = append(satellites, m.detection.Satellite)
		}
		if m.detection.Confidence.Rank() > maxConfidence.Rank() {
			maxConfidence = m.detection.Confidence
		}
	}
	sort.Slice(satellites, func(i, j int) bool { return satellites[i] < satellites[j] })
	sort.Strings(keys)
	count := float64(len(ordered))
	latitude = round(latitude/count, 5)
	longitude = round(longitude/count, 5)
	totalFRP = round(totalFRP, 2)
	return domain.Fire{
		SourceRefID:    id,
		Status:         domain.StatusActive,
		OccurredAt:     ordered[0].at,
		AreaName:       domain.NearestProvince(latitude, longitude),
		Latitude:       latitude,
		Longitude:      longitude,
		DetectionCount: len(ordered),
		Satellites:     satellites,
		MaxConfidence:  maxConfidence,
		TotalFRP:       totalFRP,
		DetectionKeys:  keys,
		Severity:       domain.SeverityFor(len(ordered), totalFRP),
	}
}

func round(value float64, decimals int) float64 {
	scale := math.Pow(10, float64(decimals))
	return math.Round(value*scale) / scale
}

func cellOf(latitude, longitude float64) gridCell {
	return gridCell{
		lat: int(math.Floor(latitude / clusterCellDegrees)),
		lon: int(math.Floor(longitude / clusterCellDegrees)),
	}
}

func linked(a, b *member) bool {
	if a.at.Sub(b.at).Abs() > clusterWindow {
		return false
	}
	return distanceKm(a.detection.Latitude, a.detection.Longitude,
		b.detection.Latitude, b.detection.Longitude) <= clusterRadiusKm
}
