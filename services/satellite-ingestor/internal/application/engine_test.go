package application

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/fathurwithyou/microservices/services/satellite-ingestor/internal/domain"
)

var day = time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)

// spot builds a detection minute minutes into the day, offsetKm east of a
// fixed point. One degree of longitude at the equator is 111.19 km.
func spot(satellite string, minute int, km float64, confidence domain.Confidence, frp float64) domain.Detection {
	at := day.Add(time.Duration(minute) * time.Minute)
	longitude := 113.0 + km/111.19
	return domain.Detection{
		Satellite:    domain.Satellite(satellite),
		AcqDate:      at.Format("2006-01-02"),
		AcqTime:      unpadded(at),
		Latitude:     0,
		Longitude:    longitude,
		RawLatitude:  "0.00000",
		RawLongitude: fmt.Sprintf("%.5f", longitude),
		Confidence:   confidence,
		FRP:          frp,
	}
}

// unpadded writes HHMM the way FIRMS does: an integer without leading zeros.
func unpadded(at time.Time) string {
	return fmt.Sprintf("%d", at.Hour()*100+at.Minute())
}

func keySets(fires []domain.Fire) []string {
	var sets []string
	for _, fire := range fires {
		if fire.Status == domain.StatusActive {
			sets = append(sets, strings.Join(fire.DetectionKeys, ","))
		}
	}
	sort.Strings(sets)
	return sets
}

func TestMidnightAcquisitionTimesWithoutPaddingParse(t *testing.T) {
	for raw, want := range map[string]string{"5": "00:05", "243": "02:43", "1312": "13:12", "0243": "02:43", "02:43": "02:43"} {
		at, err := domain.Detection{AcqDate: "2026-11-12", AcqTime: raw}.Time()
		if err != nil || at.Format("15:04") != want {
			t.Errorf("acq_time %q = %v, %v; want %s", raw, at, err, want)
		}
	}
	for _, raw := range []string{"", "2460", "99999", "ab", "-5"} {
		if _, err := (domain.Detection{AcqDate: "2026-11-12", AcqTime: raw}).Time(); err == nil {
			t.Errorf("acq_time %q accepted", raw)
		}
	}
}

func TestFireKeepsIdentityWhileItGrowsAndShrinks(t *testing.T) {
	e := NewEngine(12 * time.Hour)
	a, b, c := spot("N20", 10, 0, "n", 5), spot("N21", 60, 0.5, "h", 5), spot("N", 120, 1.0, "n", 5)
	e.Upsert(a, 1)
	if delta := e.Drain(); len(delta.Fires) != 0 {
		t.Fatalf("one detection made a fire: %+v", delta.Fires)
	}
	e.Upsert(b, 2)
	first := e.Drain().Fires
	if len(first) != 1 || first[0].DetectionCount != 2 || len(first[0].Satellites) != 2 {
		t.Fatalf("want one two-satellite fire, got %+v", first)
	}
	id := first[0].SourceRefID
	e.Upsert(c, 3)
	grown := e.Drain().Fires
	if len(grown) != 1 || grown[0].SourceRefID != id || grown[0].DetectionCount != 3 {
		t.Fatalf("growth changed identity: %+v", grown)
	}
	// Retracting the anchor shrinks the same event.
	e.Retract(a.Key())
	shrunk := e.Drain().Fires
	if len(shrunk) != 1 || shrunk[0].SourceRefID != id || shrunk[0].Status != domain.StatusActive || shrunk[0].DetectionCount != 2 {
		t.Fatalf("shrink changed identity: %+v", shrunk)
	}
	e.Retract(b.Key())
	gone := e.Drain().Fires
	if len(gone) != 1 || gone[0].SourceRefID != id || gone[0].Status != domain.StatusRetracted {
		t.Fatalf("fire below two detections must retract the same event: %+v", gone)
	}
	if stats := e.Stats(); stats.ActiveFires != 0 || stats.Singles != 1 {
		t.Fatalf("stats after retract: %+v", stats)
	}
}

func TestMergeKeepsLargerFireAndRetractsTheOther(t *testing.T) {
	e := NewEngine(12 * time.Hour)
	// Fire one: three detections around 0 km. Fire two: two around 1.8 km.
	for i, d := range []domain.Detection{spot("N20", 0, 0, "n", 1), spot("N20", 1, 0.2, "n", 1), spot("N20", 2, 0.4, "n", 1), spot("N21", 0, 1.8, "n", 1), spot("N21", 1, 2.0, "n", 1)} {
		e.Upsert(d, int64(i+1))
	}
	before := e.Drain().Fires
	if len(before) != 2 {
		t.Fatalf("want two fires, got %+v", before)
	}
	big, small := before[0], before[1]
	if big.DetectionCount < small.DetectionCount {
		big, small = small, big
	}
	e.Upsert(spot("N", 3, 1.1, "n", 1), 6) // bridges 0.4 km and 1.8 km
	after := e.Drain().Fires
	if len(after) != 2 {
		t.Fatalf("merge must update one event and retract one, got %+v", after)
	}
	for _, fire := range after {
		switch fire.SourceRefID {
		case big.SourceRefID:
			if fire.Status != domain.StatusActive || fire.DetectionCount != 6 {
				t.Errorf("surviving fire: %+v", fire)
			}
		case small.SourceRefID:
			if fire.Status != domain.StatusRetracted {
				t.Errorf("absorbed fire not retracted: %+v", fire)
			}
		default:
			t.Errorf("merge created a new event %s", fire.SourceRefID)
		}
	}
	// Retracting the bridge splits it again: the larger part keeps the id.
	e.Retract(spot("N", 3, 1.1, "n", 1).Key())
	split := e.Drain().Fires
	active := 0
	for _, fire := range split {
		if fire.Status == domain.StatusActive {
			active++
			if fire.DetectionCount == 3 && fire.SourceRefID != big.SourceRefID {
				t.Errorf("larger part lost its identity: %+v", fire)
			}
		}
	}
	if active != 2 {
		t.Fatalf("want two active fires after the split, got %+v", split)
	}
}

func TestLowConfidenceUpdateRemovesAndPromotionAdds(t *testing.T) {
	e := NewEngine(12 * time.Hour)
	a, b := spot("N20", 0, 0, "n", 1), spot("N20", 5, 0.3, "l", 1)
	e.Upsert(a, 1)
	if got := e.Upsert(b, 2); got != OutcomeLow {
		t.Fatalf("low confidence outcome = %s", got)
	}
	if len(e.Drain().Fires) != 0 {
		t.Fatal("low-confidence detection joined a fire")
	}
	b.Confidence = "n"
	e.Upsert(b, 3)
	fires := e.Drain().Fires
	if len(fires) != 1 || fires[0].Status != domain.StatusActive {
		t.Fatalf("promotion to nominal must form the fire: %+v", fires)
	}
	b.Confidence = "l"
	e.Upsert(b, 4)
	fires = e.Drain().Fires
	if len(fires) != 1 || fires[0].Status != domain.StatusRetracted {
		t.Fatalf("demotion to low must retract the fire: %+v", fires)
	}
}

func TestLatenessIsJudgedInEventTime(t *testing.T) {
	e := NewEngine(6 * time.Hour)
	e.Upsert(spot("N20", 24*60, 0, "n", 1), 1) // watermark: day + 24 h
	if got := e.Upsert(spot("N21", 24*60-359, 0.2, "n", 1), 2); got != OutcomeAdded {
		t.Fatalf("detection inside the bound = %s", got)
	}
	if got := e.Upsert(spot("N21", 24*60-361, 0.2, "n", 1), 3); got != OutcomeLate {
		t.Fatalf("detection past the bound = %s", got)
	}
	if !e.Watermark().Equal(day.Add(24 * time.Hour)) {
		t.Fatalf("a late detection moved the watermark: %s", e.Watermark())
	}
}

func TestClosedFireIsNotRetractedAndLeavesTheWorkingSet(t *testing.T) {
	e := NewEngine(6 * time.Hour)
	e.Upsert(spot("N20", 0, 0, "n", 1), 1)
	e.Upsert(spot("N21", 10, 0.2, "n", 1), 2)
	e.Upsert(spot("N20", 5, 50, "n", 1), 3) // a lone detection
	e.Finalize()
	id := e.Drain().Fires[0].SourceRefID
	// 12 h 11 min later: past lateness (6 h) + linking window (6 h).
	e.Upsert(spot("N20", 12*60+11, 300, "n", 1), 4)
	e.Finalize()
	delta := e.Drain()
	if len(delta.Fires) != 0 {
		t.Fatalf("closing must not send or retract anything: %+v", delta.Fires)
	}
	if len(delta.Closed) != 1 || delta.Closed[0].SourceRefID != id || delta.Closed[0].Status != domain.StatusClosed || delta.Closed[0].DetectionCount != 2 {
		t.Fatalf("closed = %+v", delta.Closed)
	}
	if len(delta.DeleteDetections) != 3 {
		t.Fatalf("closed detections must leave the working set, deleted %v", delta.DeleteDetections)
	}
	if stats := e.Stats(); stats.FinalizedFires != 1 || stats.FinalizedSingles != 1 || stats.Detections != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

type op struct {
	retract   bool
	detection domain.Detection
}

// randomOps is a dense scenario: a few hot areas, repeated keys, updates,
// low-confidence noise and retractions.
func randomOps(seed int64, n int) []op {
	rng := rand.New(rand.NewSource(seed))
	var ops []op
	var seen []domain.Detection
	satellites := []string{"N", "N20", "N21"}
	confidences := []domain.Confidence{"l", "n", "n", "h"}
	for len(ops) < n {
		switch roll := rng.Intn(10); {
		case roll < 2 && len(seen) > 0:
			ops = append(ops, op{retract: true, detection: seen[rng.Intn(len(seen))]})
		case roll < 4 && len(seen) > 0:
			d := seen[rng.Intn(len(seen))]
			d.FRP = float64(rng.Intn(40))
			d.Confidence = confidences[rng.Intn(len(confidences))]
			ops = append(ops, op{detection: d})
		default:
			area := float64(rng.Intn(4)) * 30
			d := spot(satellites[rng.Intn(3)], len(ops)*3+rng.Intn(200), area+rng.Float64()*4, confidences[rng.Intn(len(confidences))], float64(rng.Intn(40)))
			seen = append(seen, d)
			ops = append(ops, op{detection: d})
		}
	}
	return ops
}

func apply(e *Engine, o op, offset int64) {
	if o.retract {
		e.Retract(o.detection.Key())
		return
	}
	e.Upsert(o.detection, offset)
}

// The engine must agree with the batch definition of a fire at every step.
func TestIncrementalComponentsMatchTheDefinition(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		e := NewEngine(1000 * time.Hour)
		live := map[string]domain.Detection{}
		for index, o := range randomOps(seed, 400) {
			apply(e, o, int64(index+1))
			key := o.detection.Key()
			if o.retract || o.detection.Ignored() {
				delete(live, key)
			} else {
				live[key] = o.detection
			}
			if index%25 != 0 && index != 399 {
				continue
			}
			var detections []domain.Detection
			for _, d := range live {
				detections = append(detections, d)
			}
			want, singles := Cluster(detections)
			got := e.ActiveFires()
			if a, b := strings.Join(keySets(got), "|"), strings.Join(keySets(want), "|"); a != b {
				t.Fatalf("seed %d step %d: components differ\n got %s\nwant %s", seed, index, a, b)
			}
			if stats := e.Stats(); stats.Singles != singles {
				t.Fatalf("seed %d step %d: singles %d, want %d", seed, index, stats.Singles, singles)
			}
			ids := map[string]bool{}
			for _, fire := range got {
				if ids[fire.SourceRefID] {
					t.Fatalf("seed %d step %d: duplicate id %s", seed, index, fire.SourceRefID)
				}
				ids[fire.SourceRefID] = true
			}
		}
	}
}

// final folds a sequence of deltas into what the Aggregator would hold.
func final(deltas ...Delta) map[string]string {
	store := map[string]string{}
	for _, delta := range deltas {
		for _, fire := range delta.Fires {
			store[fire.SourceRefID] = fire.Status + ":" + strings.Join(fire.DetectionKeys, ",") + fmt.Sprintf(":%s:%.2f", fire.Severity, fire.TotalFRP)
		}
	}
	return store
}

// However the stream is cut into batches, and wherever the engine is rebuilt
// from stored state, the stored events and their identifiers are the same.
func TestResultDoesNotDependOnBatchingOrRestarts(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		ops := randomOps(seed, 300)

		perEvent := NewEngine(8 * time.Hour)
		var eachDeltas []Delta
		for index, o := range ops {
			apply(perEvent, o, int64(index+1))
			perEvent.Finalize()
			eachDeltas = append(eachDeltas, perEvent.Drain())
		}
		want := final(eachDeltas...)

		rng := rand.New(rand.NewSource(seed * 7))
		engine := NewEngine(8 * time.Hour)
		var deltas []Delta
		detections := map[string]domain.Detection{}
		fires := map[string]domain.Fire{}
		for index := 0; index < len(ops); {
			size := 1 + rng.Intn(40)
			for ; size > 0 && index < len(ops); size, index = size-1, index+1 {
				apply(engine, ops[index], int64(index+1))
				// Closing is evaluated per event in the stream too.
				engine.Finalize()
			}
			delta := engine.Drain()
			deltas = append(deltas, delta)
			for _, d := range delta.UpsertDetections {
				detections[d.Key()] = d
			}
			for _, key := range delta.DeleteDetections {
				delete(detections, key)
			}
			for _, fire := range append(append([]domain.Fire{}, delta.Fires...), delta.Closed...) {
				fires[fire.SourceRefID] = fire
			}
			if rng.Intn(3) == 0 {
				// Crash and restart from what was committed.
				watermark := engine.Watermark()
				engine = NewEngine(8 * time.Hour)
				var storedDetections []domain.Detection
				for _, d := range detections {
					storedDetections = append(storedDetections, d)
				}
				var storedFires []domain.Fire
				for _, fire := range fires {
					storedFires = append(storedFires, fire)
				}
				engine.Restore(storedDetections, storedFires, watermark)
				if extra := engine.Drain(); len(extra.Fires) != 0 {
					t.Fatalf("seed %d: restart alone changed fires: %+v", seed, extra.Fires)
				}
			}
		}
		got := final(deltas...)
		if len(got) != len(want) {
			t.Fatalf("seed %d: %d events, want %d", seed, len(got), len(want))
		}
		for id, content := range want {
			if got[id] != content {
				t.Fatalf("seed %d: event %s = %q, want %q", seed, id, got[id], content)
			}
		}
	}
}
