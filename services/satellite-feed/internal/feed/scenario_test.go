package feed

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

var defaultStart = time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)

func logHash(t *testing.T, sc *Scenario) string {
	t.Helper()
	h := sha256.New()
	if err := Dump(sc, "key", func(b []byte) error { _, err := h.Write(b); return err }); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestSameSeedSameLog(t *testing.T) {
	cfg := Config{Seed: 4031, Start: defaultStart, Hours: 48, RateMilli: 1000}
	a, b := logHash(t, Generate(cfg)), logHash(t, Generate(cfg))
	if a != b {
		t.Fatalf("same seed produced different logs: %s vs %s", a, b)
	}
	// Pins the default scenario byte for byte across platforms and Go
	// versions; update it only when the scenario is changed on purpose.
	const golden = "ac40061b8a2d07774c06fdd481e07b323991c821e82c58b3548efd93ce5806ac"
	if a != golden {
		t.Fatalf("default scenario changed: %s", a)
	}
	cfg.Seed = 4032
	if logHash(t, Generate(cfg)) == a {
		t.Fatal("different seeds produced the same log")
	}
}

func TestWireFormatAndOrdering(t *testing.T) {
	cfg := Config{Seed: 7, Start: defaultStart, Hours: 48, RateMilli: 300}
	sc := Generate(cfg)
	if len(sc.Problems) > 0 {
		t.Fatalf("generator problems: %v", sc.Problems)
	}
	detRe := regexp.MustCompile(`^\{"satellite":"(N|N20|N21)","acq_date":"\d{4}-\d\d-\d\d","acq_time":"(0|[1-9]\d{0,3})","latitude":-?\d+\.\d{5},"longitude":-?\d+\.\d{5},"confidence":"[lnh]","frp":\d+\.\d{1,2},"bright_ti4":\d+\.\d,"daynight":"[DN]"\}$`)
	keyRe := regexp.MustCompile(`^\{"key":"(N|N20|N21)\|\d{4}-\d\d-\d\d\|(0|[1-9]\d{0,3})\|-?\d+\.\d{5}\|-?\d+\.\d{5}"\}$`)
	fieldsRe := regexp.MustCompile(`^\{"satellite":"(N|N20|N21)","acq_date":"\d{4}-\d\d-\d\d","acq_time":"(0|[1-9]\d{0,3})","latitude":-?\d+\.\d{5},"longitude":-?\d+\.\d{5}\}$`)
	payload := map[string]string{} // live payload by key
	gone := map[string]bool{}
	counts := map[Kind]int{}
	dups, promos, demos := 0, 0, 0
	var prev int64
	for i, e := range sc.Events {
		if e.ArrivalMs < prev {
			t.Fatalf("event %d is released before its predecessor", i+1)
		}
		prev = e.ArrivalMs
		counts[e.Kind]++
		data, key := cfg.Data(e, "key"), cfg.Key(e.D)
		if !json.Valid([]byte(data)) {
			t.Fatalf("offset %d: invalid JSON %s", i+1, data)
		}
		switch e.Kind {
		case KindRetract:
			if !keyRe.MatchString(data) || !fieldsRe.MatchString(cfg.Data(e, "fields")) {
				t.Fatalf("offset %d: bad retract %s", i+1, data)
			}
			if _, live := payload[key]; !live {
				t.Fatalf("offset %d: retract of a key that is not live", i+1)
			}
			delete(payload, key)
			gone[key] = true
		default:
			if !detRe.MatchString(data) {
				t.Fatalf("offset %d: bad payload %s", i+1, data)
			}
			if gone[key] {
				t.Fatalf("offset %d: %s re-sent after its retract", i+1, key)
			}
			old, live := payload[key]
			if e.Kind == KindUpdate {
				if !live || old == data {
					t.Fatalf("offset %d: update must change a live detection", i+1)
				}
				if strings.Contains(old, `"confidence":"l"`) && !strings.Contains(data, `"confidence":"l"`) {
					promos++
				}
				if !strings.Contains(old, `"confidence":"l"`) && strings.Contains(data, `"confidence":"l"`) {
					demos++
				}
			} else if live {
				if old != data {
					t.Fatalf("offset %d: duplicate detection with a different payload", i+1)
				}
				dups++
			}
			payload[key] = data
		}
	}
	if counts[KindUpdate] == 0 || counts[KindRetract] == 0 || dups == 0 || promos == 0 || demos == 0 {
		t.Fatalf("scenario lacks a feature: %v dups=%d promotions=%d demotions=%d", counts, dups, promos, demos)
	}
	st := sc.Stats
	if st.ArrivalLate == 0 || st.ArrivalVeryLate == 0 || st.UniqueKeys < 20000 {
		t.Fatalf("unexpected stats: %+v", st)
	}
	for _, sat := range satNames {
		if st.BySatellite[sat] == 0 {
			t.Fatalf("no detections from %s", sat)
		}
	}
}

// A deliberately naive O(n^2) reading of "Definisi Kebakaran", used only to
// check that every hand-designed case behaves as its manifest says.
func naiveFires(cfg Config, live map[string]Det) (fires map[string]ExpFire, singles []string) {
	var keys []string
	for k, d := range live {
		if d.Conf != cL {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	parent := make([]int, len(keys))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(i int) int {
		if parent[i] != i {
			parent[i] = find(parent[i])
		}
		return parent[i]
	}
	rad := func(e5 int32) float64 { return float64(e5) / 1e5 * math.Pi / 180 }
	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			a, b := live[keys[i]], live[keys[j]]
			dt := a.AcqMin - b.AcqMin
			if dt < 0 {
				dt = -dt
			}
			h := math.Pow(math.Sin((rad(b.LatE5)-rad(a.LatE5))/2), 2) +
				math.Cos(rad(a.LatE5))*math.Cos(rad(b.LatE5))*math.Pow(math.Sin((rad(b.LonE5)-rad(a.LonE5))/2), 2)
			if dt <= 360 && 2*6371.0088*math.Asin(math.Sqrt(h)) <= 1.0 {
				parent[find(i)] = find(j)
			}
		}
	}
	groups := map[int][]string{}
	for i, k := range keys {
		groups[find(i)] = append(groups[find(i)], k)
	}
	fires = map[string]ExpFire{}
	for _, g := range groups {
		if len(g) < 2 {
			singles = append(singles, g[0])
			continue
		}
		var frp int32
		var conf uint8
		sats := map[string]bool{}
		for _, k := range g {
			frp += live[k].FrpC
			conf = max(conf, live[k].Conf)
			sats[satNames[live[k].Sat]] = true
		}
		f := ExpFire{Keys: g, Severity: "WASPADA", MaxConfidence: confNames[conf]}
		if len(g) >= 20 || frp >= 10000 {
			f.Severity = "AWAS"
		} else if len(g) >= 6 || frp >= 3000 {
			f.Severity = "SIAGA"
		}
		for s := range sats {
			f.Satellites = append(f.Satellites, s)
		}
		sort.Strings(f.Satellites)
		fires[strings.Join(g, ",")] = f
	}
	sort.Strings(singles)
	return fires, singles
}

func TestSpecialCasesBehaveAsDeclared(t *testing.T) {
	cfg := Config{Seed: 4031, Start: defaultStart, Hours: 48, RateMilli: 100}
	sc := Generate(cfg)
	if len(sc.Problems) > 0 {
		t.Fatalf("generator problems: %v", sc.Problems)
	}
	names := map[string]int{}
	for _, sp := range sc.Specials {
		names[sp.Name]++
		if len(sp.Phases) == 0 {
			t.Fatalf("%s has no phase", sp.Name)
		}
		for _, ph := range sp.Phases {
			live := map[string]Det{}
			steps := append([]Step(nil), sp.Steps...)
			sort.Slice(steps, func(i, j int) bool { return steps[i].Offset < steps[j].Offset })
			for _, st := range steps {
				if st.Offset > ph.AfterOffset {
					break
				}
				e := sc.Events[st.Offset-1]
				if cfg.Key(e.D) != st.Key || e.Kind.String() != st.Event {
					t.Fatalf("%s: step does not match the log at offset %d", sp.Name, st.Offset)
				}
				if e.Kind == KindRetract {
					delete(live, st.Key)
				} else {
					live[st.Key] = e.D
				}
			}
			fires, singles := naiveFires(cfg, live)
			if len(fires) != len(ph.Fires) {
				t.Fatalf("%s#%d %q: %d fires, want %d", sp.Name, sp.Instance, ph.Label, len(fires), len(ph.Fires))
			}
			for _, want := range ph.Fires {
				ks := append([]string(nil), want.Keys...)
				sort.Strings(ks)
				got, ok := fires[strings.Join(ks, ",")]
				if !ok {
					t.Fatalf("%s#%d %q: expected fire is missing", sp.Name, sp.Instance, ph.Label)
				}
				if got.Severity != want.Severity ||
					(want.MaxConfidence != "" && got.MaxConfidence != want.MaxConfidence) ||
					(want.Satellites != nil && strings.Join(got.Satellites, ",") != strings.Join(want.Satellites, ",")) {
					t.Fatalf("%s#%d %q: got %+v, want %+v", sp.Name, sp.Instance, ph.Label, got, want)
				}
			}
			wantSingles := append([]string(nil), ph.Singles...)
			sort.Strings(wantSingles)
			if strings.Join(singles, ",") != strings.Join(wantSingles, ",") {
				t.Fatalf("%s#%d %q: singles %v, want %v", sp.Name, sp.Instance, ph.Label, singles, wantSingles)
			}
		}
	}
	for _, def := range specialDefs {
		if names[def.name] == 0 {
			t.Fatalf("special %s is not in a 48 h scenario", def.name)
		}
	}
}
