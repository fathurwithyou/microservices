// Package feed is a local stand-in for the assistants' Satellite Feed
// (IF4031 M2, Problem 4). The scenario is generated with integer arithmetic
// only, so the same seed yields a byte-identical event log on every platform
// and Go version.
package feed

import (
	"fmt"
	"sort"
	"strconv"
	"time"
)

type Kind uint8

const (
	KindDetection Kind = iota
	KindUpdate
	KindRetract
)

var (
	kindNames = [3]string{"detection", "update", "retract"}
	satNames  = [3]string{"N", "N20", "N21"}
	confNames = [3]string{"l", "n", "h"}
)

func (k Kind) String() string { return kindNames[k] }

// Det is one VIIRS detection. Coordinates are held in 1e-5 degree units and
// FRP in 0.01 MW so the wire tokens are produced without float formatting.
type Det struct {
	AcqMin  int32 // minutes since the virtual start
	LatE5   int32
	LonE5   int32
	FrpC    int32
	BrightD int16 // 0.1 K
	Sat     uint8
	Conf    uint8
}

type Event struct {
	ArrivalMs int64 // virtual milliseconds since the virtual start
	D         Det
	seq       int32
	Kind      Kind
}

type Config struct {
	Seed      int64
	Start     time.Time // virtual start
	Hours     int       // acquisition span in virtual hours
	RateMilli int       // 1000 = ~2000 detections per virtual hour
}

type ExpFire struct {
	Keys          []string `json:"detection_keys"`
	Severity      string   `json:"severity"`
	MaxConfidence string   `json:"max_confidence,omitempty"`
	Satellites    []string `json:"satellites,omitempty"`
}

// Phase is what "Definisi Kebakaran" must yield for a special's own keys
// once every event up to AfterOffset has been applied.
type Phase struct {
	Label       string    `json:"label"`
	AfterOffset int       `json:"after_offset"`
	Fires       []ExpFire `json:"fires"`
	Singles     []string  `json:"singles"`
	nseq        int
}

type Step struct {
	Offset int    `json:"offset"`
	Event  string `json:"event"`
	Key    string `json:"key"`
}

// Special is a hand-designed case that exercises one rule.
type Special struct {
	Name     string  `json:"name"`
	Instance int     `json:"instance"`
	Rule     string  `json:"rule"`
	Steps    []Step  `json:"steps"`
	Phases   []Phase `json:"phases"`
	seqs     []int32
}

type Stats struct {
	Events               int            `json:"events"`
	ByKind               map[string]int `json:"by_kind"`
	UniqueKeys           int            `json:"unique_keys"`
	DuplicateDetections  int            `json:"duplicate_detections"`
	ByConfidence         map[string]int `json:"first_seen_by_confidence"`
	BySatellite          map[string]int `json:"first_seen_by_satellite"`
	ArrivalNormal        int            `json:"arrival_under_2h"`
	ArrivalLate          int            `json:"arrival_late_2h_to_13h"`
	ArrivalVeryLate      int            `json:"arrival_very_late_13h_plus"`
	Passes               int            `json:"passes"`
	GenericFireClusters  int            `json:"generic_fire_clusters"`
	GenericFirePoints    int            `json:"generic_fire_points"`
	NoisePoints          int            `json:"noise_points"`
	Specials             int            `json:"specials"`
	EventsWithinSpan     int            `json:"events_arriving_within_span"`
	FirstArrivalVirtual  string         `json:"first_arrival_virtual"`
	LastArrivalVirtual   string         `json:"last_arrival_virtual"`
	AcquisitionSpanHours int            `json:"acquisition_span_hours"`
}

type Scenario struct {
	Cfg      Config
	Events   []Event
	Specials []Special
	Stats    Stats
	// Problems lists generator invariants that did not hold (always empty for
	// a healthy scenario; asserted by the tests).
	Problems []string
}

// ---- rendering ------------------------------------------------------------

func coordToken(v int32) string {
	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}
	return fmt.Sprintf("%s%d.%05d", sign, v/100000, v%100000)
}

func frpToken(v int32) string {
	if v%10 == 0 {
		return fmt.Sprintf("%d.%d", v/100, (v%100)/10)
	}
	return fmt.Sprintf("%d.%02d", v/100, v%100)
}

func (c Config) acq(d Det) (date, hhmm string, t time.Time) {
	t = c.Start.Add(time.Duration(d.AcqMin) * time.Minute).UTC()
	// FIRMS writes acq_time as HHMM without zero padding.
	return t.Format("2006-01-02"), strconv.Itoa(t.Hour()*100 + t.Minute()), t
}

// Key is satellite|acq_date|acq_time|latitude|longitude with the wire tokens.
func (c Config) Key(d Det) string {
	date, hhmm, _ := c.acq(d)
	return satNames[d.Sat] + "|" + date + "|" + hhmm + "|" + coordToken(d.LatE5) + "|" + coordToken(d.LonE5)
}

// Data renders the one-line JSON payload of an event.
func (c Config) Data(e Event, retractShape string) string {
	d := e.D
	date, hhmm, t := c.acq(d)
	if e.Kind == KindRetract {
		if retractShape == "fields" {
			return fmt.Sprintf(`{"satellite":"%s","acq_date":"%s","acq_time":"%s","latitude":%s,"longitude":%s}`,
				satNames[d.Sat], date, hhmm, coordToken(d.LatE5), coordToken(d.LonE5))
		}
		return `{"key":"` + c.Key(d) + `"}`
	}
	// Local solar time decides day/night.
	local := (t.Hour()*60 + t.Minute() + int(d.LonE5)*4/100000 + 2880) % 1440
	dn := "N"
	if local >= 360 && local < 1080 {
		dn = "D"
	}
	return fmt.Sprintf(`{"satellite":"%s","acq_date":"%s","acq_time":"%s","latitude":%s,"longitude":%s,"confidence":"%s","frp":%s,"bright_ti4":%d.%d,"daynight":"%s"}`,
		satNames[d.Sat], date, hhmm, coordToken(d.LatE5), coordToken(d.LonE5),
		confNames[d.Conf], frpToken(d.FrpC), d.BrightD/10, d.BrightD%10, dn)
}

// ---- deterministic RNG (splitmix64) ----------------------------------------

type rng struct{ s uint64 }

func (r *rng) next() uint64 {
	r.s += 0x9e3779b97f4a7c15
	z := r.s
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}
func (r *rng) intn(n int) int { return int(r.next() % uint64(n)) }

// between returns a value in [lo, hi].
func (r *rng) between(lo, hi int) int { return lo + r.intn(hi-lo+1) }

// ---- builder ---------------------------------------------------------------

const (
	cL, cN, cH       = 0, 1, 2
	sN, sN20, sN21   = 0, 1, 2
	gridCell         = 3000 // 1e-5 deg, about 3.3 km
	isolationUnits   = 2000 // about 2.2 km: noise never links to anything
	fireSpacingUnits = 4500 // about 5 km between a new fire centre and any point
	minuteMs         = 60000
)

type detID struct {
	sat           uint8
	acq, lat, lon int32
}

type cell struct{ a, b int32 }

type builder struct {
	cfg      Config
	r        *rng
	events   []Event
	spOf     []int32
	keys     map[detID]struct{}
	grid     map[cell][][2]int32
	specials []Special
	spanMin  int32
	stats    Stats
}

func floorDiv(a, b int32) int32 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func (b *builder) near(lat, lon int32, units int64, reach int32) bool {
	ca, cb := floorDiv(lat, gridCell), floorDiv(lon, gridCell)
	for i := -reach; i <= reach; i++ {
		for j := -reach; j <= reach; j++ {
			for _, p := range b.grid[cell{ca + i, cb + j}] {
				dy, dx := int64(p[0]-lat), int64(p[1]-lon)
				if dy*dy+dx*dx < units*units {
					return true
				}
			}
		}
	}
	return false
}

func (b *builder) claim(d Det) bool {
	id := detID{d.Sat, d.AcqMin, d.LatE5, d.LonE5}
	if _, dup := b.keys[id]; dup {
		return false
	}
	b.keys[id] = struct{}{}
	c := cell{floorDiv(d.LatE5, gridCell), floorDiv(d.LonE5, gridCell)}
	b.grid[c] = append(b.grid[c], [2]int32{d.LatE5, d.LonE5})
	return true
}

func (b *builder) add(k Kind, d Det, arrivalMs int64, sp int) {
	seq := int32(len(b.events))
	b.events = append(b.events, Event{Kind: k, D: d, ArrivalMs: arrivalMs, seq: seq})
	b.spOf = append(b.spOf, int32(sp))
	if sp >= 0 {
		b.specials[sp].seqs = append(b.specials[sp].seqs, seq)
	}
}

// delayMs: most detections arrive 3-20 virtual minutes after acquisition,
// about 5% arrive 2-8 hours late and about 0.5% arrive 13-20 hours late.
func (b *builder) delayMs() int64 {
	switch roll := b.r.intn(1000); {
	case roll < 5:
		return int64(b.r.between(13*60*minuteMs, 20*60*minuteMs))
	case roll < 55:
		return int64(b.r.between(2*60*minuteMs, 8*60*minuteMs))
	default:
		return int64(b.r.between(3*minuteMs, 20*minuteMs))
	}
}

func (b *builder) scale(n int) int { return n * b.cfg.RateMilli / 1000 }

// ---- regions ---------------------------------------------------------------

type box struct{ latLo, latHi, lonLo, lonHi, weight int32 }

var regions = []box{
	{-500000, 500000, 9600000, 10600000, 200},   // Sumatra
	{-400000, 400000, 10900000, 11800000, 144},  // Kalimantan
	{-500000, 100000, 11900000, 12400000, 60},   // Sulawesi
	{-900000, -100000, 13100000, 14100000, 160}, // Papua
	{-900000, -650000, 10600000, 12500000, 95},  // Jawa, Bali, Nusa Tenggara
}

// Hand-designed cases live in their own rectangle in Kalimantan Tengah;
// nothing random is ever placed inside it.
const (
	spLat0, spLon0 = -285000, 11270000
	spStep         = 8000
	spRows, spCols = 10, 20
)

var exclusion = box{-295000, -195000, 11260000, 11440000, 0}

func (b *builder) randomPoint() (int32, int32) {
	total := 0
	for _, g := range regions {
		total += int(g.weight)
	}
	for {
		roll := b.r.intn(total)
		for _, g := range regions {
			if roll >= int(g.weight) {
				roll -= int(g.weight)
				continue
			}
			lat := g.latLo + int32(b.r.intn(int(g.latHi-g.latLo)))
			lon := g.lonLo + int32(b.r.intn(int(g.lonHi-g.lonLo)))
			if lat >= exclusion.latLo && lat <= exclusion.latHi && lon >= exclusion.lonLo && lon <= exclusion.lonHi {
				break
			}
			return lat, lon
		}
	}
}

// ---- passes ----------------------------------------------------------------

type pass struct {
	sat uint8
	min int32
}

func (b *builder) passes() []pass {
	var out []pass
	offsets := [3]int32{2, 35, 68} // each satellite every ~100 minutes
	for sat := uint8(0); sat < 3; sat++ {
		for base := offsets[sat]; ; base += 100 {
			m := base + int32(b.r.between(0, 4))
			if m+6 >= b.spanMin {
				break
			}
			out = append(out, pass{sat, m})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].min < out[j].min })
	return out
}

// ---- generic content -------------------------------------------------------

// followUps occasionally re-sends, updates or retracts a detection.
func (b *builder) followUps(d Det, arrival int64, fire bool) {
	roll := b.r.intn(10000)
	dupMax, updMax, retMax := 150, 190, 220
	if fire {
		dupMax, updMax, retMax = 150, 270, 330
	}
	switch {
	case roll < dupMax: // exact duplicate, same key and payload
		n := 1 + b.r.intn(2)
		at := arrival
		for i := 0; i < n; i++ {
			if b.r.intn(10) == 0 {
				at += int64(b.r.between(120*minuteMs, 360*minuteMs))
			} else {
				at += int64(b.r.between(1*minuteMs, 120*minuteMs))
			}
			b.add(KindDetection, d, at, -1)
		}
	case roll < updMax:
		u := d
		switch mode := b.r.intn(4); {
		case mode < 2 || (mode == 2 && d.Conf == cH) || (mode == 3 && d.Conf == cL):
			u.FrpC = d.FrpC*int32(b.r.between(50, 180))/100 + 1
		case mode == 2:
			u.Conf = d.Conf + 1 // l->n creates, n->h raises max_confidence
		default:
			u.Conf = d.Conf - 1 // n->l removes, h->n lowers max_confidence
		}
		b.add(KindUpdate, u, arrival+int64(b.r.between(5*minuteMs, 90*minuteMs)), -1)
	case roll < retMax:
		b.add(KindRetract, d, arrival+int64(b.r.between(10*minuteMs, 240*minuteMs)), -1)
	}
}

func (b *builder) fire(ps []pass, pi int) {
	var n, life, radius, drift, frpLo, frpHi int
	switch roll := b.r.intn(100); {
	case roll < 60:
		n, life, radius = b.r.between(2, 6), b.r.between(1, 3), b.r.between(70, 220)
		frpLo, frpHi = 50, 600
	case roll < 90:
		n, life, radius = b.r.between(6, 20), b.r.between(2, 6), b.r.between(150, 350)
		frpLo, frpHi = 80, 1200
	default:
		n, life, radius = b.r.between(20, 60), b.r.between(4, 10), b.r.between(200, 400)
		frpLo, frpHi, drift = 150, 2500, b.r.between(0, 120)
	}
	if life > len(ps)-pi {
		life = len(ps) - pi
	}
	var lat, lon int32
	placed := false
	for try := 0; try < 30 && !placed; try++ {
		lat, lon = b.randomPoint()
		placed = !b.near(lat, lon, fireSpacingUnits, 2)
	}
	if !placed {
		return
	}
	dirLat, dirLon := int32(b.r.between(-1, 1)), int32(b.r.between(-1, 1))
	minuteOff := make([]int32, life)
	for i := range minuteOff {
		minuteOff[i] = int32(b.r.intn(6))
	}
	b.stats.GenericFireClusters++
	for i := 0; i < n; i++ {
		j := b.r.intn(life)
		p := ps[pi+j]
		var d Det
		for {
			dy, dx := b.r.between(-radius, radius), b.r.between(-radius, radius)
			if dy*dy+dx*dx > radius*radius {
				continue
			}
			d = Det{
				Sat: p.sat, AcqMin: p.min + minuteOff[j],
				LatE5: lat + int32(dy) + dirLat*int32(drift*j),
				LonE5: lon + int32(dx) + dirLon*int32(drift*j),
			}
			if b.claim(d) {
				break
			}
		}
		switch roll := b.r.intn(100); {
		case roll < 12:
			d.Conf = cL
		case roll < 80:
			d.Conf = cN
		default:
			d.Conf = cH
		}
		// Product of two uniforms: most points are weak, a few are strong.
		span := frpHi - frpLo
		d.FrpC = int32(frpLo + b.r.intn(span+1)*b.r.intn(span+1)/span)
		d.BrightD = int16(b.r.between(3050, 3670))
		arrival := int64(d.AcqMin)*minuteMs + b.delayMs()
		b.add(KindDetection, d, arrival, -1)
		b.stats.GenericFirePoints++
		b.followUps(d, arrival, true)
	}
}

// noise is an "atap seng": one isolated point, mostly low confidence.
func (b *builder) noise(p pass) {
	for try := 0; try < 20; try++ {
		lat, lon := b.randomPoint()
		if b.near(lat, lon, isolationUnits, 1) {
			continue
		}
		d := Det{Sat: p.sat, AcqMin: p.min + int32(b.r.intn(6)), LatE5: lat, LonE5: lon}
		if !b.claim(d) {
			continue
		}
		switch roll := b.r.intn(100); {
		case roll < 80:
			d.Conf = cL
		case roll < 95:
			d.Conf = cN
		default:
			d.Conf = cH
		}
		d.FrpC = int32(b.r.between(30, 400))
		d.BrightD = int16(b.r.between(3000, 3400))
		arrival := int64(d.AcqMin)*minuteMs + b.delayMs()
		b.add(KindDetection, d, arrival, -1)
		b.stats.NoisePoints++
		b.followUps(d, arrival, false)
		return
	}
}

// Generate builds the whole event log for a seed.
func Generate(cfg Config) *Scenario {
	if cfg.Hours <= 0 {
		cfg.Hours = 48
	}
	if cfg.RateMilli <= 0 {
		cfg.RateMilli = 1000
	}
	cfg.Start = cfg.Start.UTC().Truncate(time.Minute)
	b := &builder{
		cfg: cfg, r: &rng{s: uint64(cfg.Seed)},
		keys: map[detID]struct{}{}, grid: map[cell][][2]int32{},
		spanMin: int32(cfg.Hours * 60),
	}
	b.buildSpecials()
	ps := b.passes()
	for pi := range ps {
		for i, n := 0, b.scale(b.r.between(26, 34)); i < n; i++ {
			b.fire(ps, pi)
		}
	}
	for _, p := range ps {
		for i, n := 0, b.scale(b.r.between(690, 930)); i < n; i++ {
			b.noise(p)
		}
	}

	sort.SliceStable(b.events, func(i, j int) bool {
		if b.events[i].ArrivalMs != b.events[j].ArrivalMs {
			return b.events[i].ArrivalMs < b.events[j].ArrivalMs
		}
		return b.events[i].seq < b.events[j].seq
	})
	sc := &Scenario{Cfg: cfg, Events: b.events, Specials: b.specials}
	offsetOf := make([]int, len(b.events))
	for i, e := range b.events {
		offsetOf[e.seq] = i + 1
	}

	st := b.stats
	st.Events, st.Passes, st.Specials = len(b.events), len(ps), len(b.specials)
	st.AcquisitionSpanHours = cfg.Hours
	st.ByKind, st.ByConfidence, st.BySatellite = map[string]int{}, map[string]int{}, map[string]int{}
	seen := make(map[detID]struct{}, len(b.keys))
	for _, e := range b.events {
		st.ByKind[e.Kind.String()]++
		if e.ArrivalMs < int64(b.spanMin)*minuteMs {
			st.EventsWithinSpan++
		}
		if e.Kind != KindDetection {
			continue
		}
		id := detID{e.D.Sat, e.D.AcqMin, e.D.LatE5, e.D.LonE5}
		if _, dup := seen[id]; dup {
			st.DuplicateDetections++
			continue
		}
		seen[id] = struct{}{}
		st.ByConfidence[confNames[e.D.Conf]]++
		st.BySatellite[satNames[e.D.Sat]]++
		switch late := e.ArrivalMs - int64(e.D.AcqMin)*minuteMs; {
		case late >= 13*60*minuteMs:
			st.ArrivalVeryLate++
		case late >= 2*60*minuteMs:
			st.ArrivalLate++
		default:
			st.ArrivalNormal++
		}
	}
	st.UniqueKeys = len(seen)
	if len(b.events) > 0 {
		st.FirstArrivalVirtual = sc.VirtualTime(b.events[0].ArrivalMs).Format(time.RFC3339)
		st.LastArrivalVirtual = sc.VirtualTime(b.events[len(b.events)-1].ArrivalMs).Format(time.RFC3339)
	}
	sc.Stats = st

	for i := range sc.Specials {
		sp := &sc.Specials[i]
		for _, seq := range sp.seqs {
			e := b.events[offsetOf[seq]-1]
			sp.Steps = append(sp.Steps, Step{offsetOf[seq], e.Kind.String(), cfg.Key(e.D)})
		}
		for pi := range sp.Phases {
			ph := &sp.Phases[pi]
			for _, seq := range sp.seqs[:ph.nseq] {
				ph.AfterOffset = max(ph.AfterOffset, offsetOf[seq])
			}
			for _, seq := range sp.seqs[ph.nseq:] {
				if offsetOf[seq] <= ph.AfterOffset {
					sc.Problems = append(sc.Problems, fmt.Sprintf("%s#%d phase %q: a later step is released before the phase ends", sp.Name, sp.Instance, ph.Label))
				}
			}
		}
	}
	return sc
}

func (s *Scenario) VirtualTime(ms int64) time.Time {
	return s.Cfg.Start.Add(time.Duration(ms) * time.Millisecond).UTC()
}
