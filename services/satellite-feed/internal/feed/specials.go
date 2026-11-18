package feed

// Hand-designed cases. Each one sits alone on a slot of a reserved rectangle
// (slots are ~8.9 km apart, so cases never interact) and records, per phase,
// what "Definisi Kebakaran" must yield for its own keys. 1e-5 degree of
// latitude is 1.112 m, so 899 units is 0.99964 km and 900 units 1.00076 km.

type spBuild struct {
	b           *builder
	idx         int
	lat, lon, t int32
}

func (s *spBuild) det(sat uint8, dt, dLat, dLon int32, conf uint8, frpC int32, delayMin int32) Det {
	d := Det{Sat: sat, AcqMin: s.t + dt, LatE5: s.lat + dLat, LonE5: s.lon + dLon, Conf: conf, FrpC: frpC,
		BrightD: int16(s.b.r.between(3200, 3500))}
	if !s.b.claim(d) {
		panic("satellite-feed: special reuses a detection key")
	}
	s.b.add(KindDetection, d, int64(s.t+dt+delayMin)*minuteMs, s.idx)
	return d
}

// update, retract and dup take the arrival time in minutes after the base.
func (s *spBuild) update(d Det, conf uint8, frpC int32, at int32) Det {
	d.Conf, d.FrpC = conf, frpC
	s.b.add(KindUpdate, d, int64(s.t+at)*minuteMs, s.idx)
	return d
}
func (s *spBuild) retract(d Det, at int32) {
	s.b.add(KindRetract, d, int64(s.t+at)*minuteMs, s.idx)
}
func (s *spBuild) dup(d Det, at int32) {
	s.b.add(KindDetection, d, int64(s.t+at)*minuteMs, s.idx)
}

func (s *spBuild) keys(ds []Det) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, s.b.cfg.Key(d))
	}
	return out
}

func (s *spBuild) fire(severity string, ds ...Det) ExpFire {
	return ExpFire{Keys: s.keys(ds), Severity: severity}
}

func (s *spBuild) phase(label string, singles []Det, fires ...ExpFire) {
	sp := &s.b.specials[s.idx]
	if fires == nil {
		fires = []ExpFire{}
	}
	sp.Phases = append(sp.Phases, Phase{Label: label, Fires: fires, Singles: s.keys(singles), nseq: len(sp.seqs)})
}

// cluster places n points on a 50 m lattice (at most 250 m across).
func (s *spBuild) cluster(sat uint8, dt, dLat, dLon int32, n int, start int, conf uint8, frpC int32) []Det {
	out := make([]Det, 0, n)
	for i := start; i < start+n; i++ {
		out = append(out, s.det(sat, dt, dLat+int32(i%5)*45, dLon+int32(i/5)*45, conf, frpC, 5))
	}
	return out
}

type specialDef struct {
	name, rule string
	span       int32 // virtual minutes of acquisition the case needs
	fn         func(s *spBuild)
}

func cat(groups ...[]Det) []Det {
	var out []Det
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

var specialDefs = []specialDef{
	{"distance_just_under", "two detections 0.9996 km apart in the same minute are linked", 10, func(s *spBuild) {
		a := s.det(sN20, 0, 0, 0, cN, 420, 5)
		b := s.det(sN20, 0, 899, 0, cN, 380, 5)
		s.phase("linked", nil, s.fire("WASPADA", a, b))
	}},
	{"distance_just_over", "two detections 1.0008 km apart are not linked: two singles", 10, func(s *spBuild) {
		a := s.det(sN20, 0, 0, 0, cN, 420, 5)
		b := s.det(sN20, 0, 900, 0, cN, 380, 5)
		s.phase("separate", []Det{a, b})
	}},
	{"time_exactly_360", "two detections 360 minutes apart are linked", 370, func(s *spBuild) {
		a := s.det(sN20, 0, 0, 0, cN, 300, 5)
		b := s.det(sN21, 360, 100, 50, cN, 300, 5)
		s.phase("linked", nil, s.fire("WASPADA", a, b))
	}},
	{"time_361", "two detections 361 minutes apart are not linked: two singles", 370, func(s *spBuild) {
		a := s.det(sN20, 0, 0, 0, cN, 300, 5)
		b := s.det(sN21, 361, 100, 50, cN, 300, 5)
		s.phase("separate", []Det{a, b})
	}},
	{"chain_late_middle", "five detections chained 0.89 km and 300 min apart; the middle one arrives 11.7 h late and merges two fires", 1210, func(s *spBuild) {
		p0 := s.det(sN, 0, 0, 0, cN, 200, 5)
		p1 := s.det(sN20, 300, 800, 0, cN, 200, 5)
		p3 := s.det(sN, 900, 2400, 0, cH, 200, 5)
		p4 := s.det(sN20, 1200, 3200, 0, cN, 200, 5)
		s.phase("two fires", nil, s.fire("WASPADA", p0, p1), s.fire("WASPADA", p3, p4))
		p2 := s.det(sN21, 600, 1600, 0, cN, 200, 700)
		s.phase("one chain", nil, s.fire("WASPADA", p0, p1, p2, p3, p4))
	}},
	{"siaga_by_count", "5 detections are WASPADA, the 6th makes SIAGA", 40, func(s *spBuild) {
		a := s.cluster(sN20, 0, 0, 0, 5, 0, cN, 200)
		s.phase("five", nil, s.fire("WASPADA", a...))
		b := s.cluster(sN21, 33, 20, 20, 1, 5, cN, 200)
		s.phase("six", nil, s.fire("SIAGA", cat(a, b)...))
	}},
	{"siaga_by_frp", "29.75 MW is WASPADA; an frp update to exactly 30.00 MW makes SIAGA with 2 detections", 10, func(s *spBuild) {
		a := s.det(sN20, 0, 0, 0, cN, 1450, 5)
		b := s.det(sN20, 0, 90, 0, cN, 1525, 5)
		s.phase("29.75 MW", nil, s.fire("WASPADA", a, b))
		b = s.update(b, cN, 1550, 45)
		s.phase("30.00 MW", nil, s.fire("SIAGA", a, b))
	}},
	{"awas_by_count", "a fire grows 5 -> 12 -> 19 -> 20 detections: WASPADA, SIAGA, SIAGA, AWAS", 110, func(s *spBuild) {
		a := s.cluster(sN, 0, 0, 0, 5, 0, cN, 150)
		s.phase("5", nil, s.fire("WASPADA", a...))
		a = cat(a, s.cluster(sN20, 33, 10, 10, 7, 5, cN, 150))
		s.phase("12", nil, s.fire("SIAGA", a...))
		a = cat(a, s.cluster(sN21, 66, 20, 20, 7, 12, cN, 150))
		s.phase("19", nil, s.fire("SIAGA", a...))
		a = cat(a, s.cluster(sN, 100, 30, 30, 1, 19, cH, 150))
		s.phase("20", nil, s.fire("AWAS", a...))
	}},
	{"awas_by_frp", "75 MW in 3 detections is SIAGA; a 4th makes exactly 100.00 MW: AWAS", 40, func(s *spBuild) {
		a := s.cluster(sN20, 0, 0, 0, 3, 0, cH, 2500)
		s.phase("75 MW", nil, s.fire("SIAGA", a...))
		a = cat(a, s.cluster(sN21, 33, 20, 20, 1, 3, cH, 2500))
		s.phase("100 MW", nil, s.fire("AWAS", a...))
	}},
	{"merge_by_bridge", "two fires 1.6 km apart merge when a detection between them arrives", 40, func(s *spBuild) {
		a := s.cluster(sN20, 0, 0, 0, 3, 0, cN, 200)
		c := s.cluster(sN21, 2, 1500, 0, 3, 0, cN, 200)
		s.phase("two fires", nil, s.fire("WASPADA", a...), s.fire("WASPADA", c...))
		br := s.det(sN, 33, 750, 20, cN, 200, 5)
		s.phase("merged", nil, s.fire("SIAGA", cat(a, c, []Det{br})...))
	}},
	{"merge_by_very_late_bridge", "the bridging detection arrives 14 h after acquisition", 40, func(s *spBuild) {
		a := s.cluster(sN20, 0, 0, 0, 3, 0, cN, 200)
		c := s.cluster(sN21, 2, 1500, 0, 3, 0, cN, 200)
		s.phase("two fires", nil, s.fire("WASPADA", a...), s.fire("WASPADA", c...))
		br := s.det(sN, 33, 750, 20, cN, 200, 850)
		s.phase("merged", nil, s.fire("SIAGA", cat(a, c, []Det{br})...))
	}},
	{"split_by_retract", "retracting the bridging detection splits one fire into two", 40, func(s *spBuild) {
		a := s.cluster(sN20, 0, 0, 0, 3, 0, cN, 200)
		c := s.cluster(sN21, 2, 1500, 0, 3, 0, cN, 200)
		br := s.det(sN, 4, 750, 20, cN, 200, 5)
		s.phase("one fire", nil, s.fire("SIAGA", cat(a, c, []Det{br})...))
		s.retract(br, 120)
		s.phase("split", nil, s.fire("WASPADA", a...), s.fire("WASPADA", c...))
	}},
	{"shrink_by_retract", "a 20-detection AWAS fire loses one detection and becomes SIAGA", 10, func(s *spBuild) {
		a := s.cluster(sN20, 0, 0, 0, 20, 0, cN, 100)
		s.phase("20", nil, s.fire("AWAS", a...))
		s.retract(a[7], 90)
		s.phase("19", nil, s.fire("SIAGA", cat(a[:7], a[8:])...))
	}},
	{"vanish_by_retract", "a 3-detection fire loses two detections and is no longer a fire", 10, func(s *spBuild) {
		a := s.cluster(sN21, 0, 0, 0, 3, 0, cN, 300)
		s.phase("fire", nil, s.fire("WASPADA", a...))
		s.retract(a[0], 60)
		s.retract(a[2], 61)
		s.phase("gone", []Det{a[1]})
	}},
	{"promote_creates_fire", "an update raising confidence l -> n creates a fire", 10, func(s *spBuild) {
		a := s.det(sN20, 0, 0, 0, cN, 250, 5)
		b := s.det(sN20, 0, 180, 0, cL, 250, 5)
		s.phase("one single", []Det{a})
		b = s.update(b, cN, 250, 50)
		s.phase("fire", nil, s.fire("WASPADA", a, b))
	}},
	{"demote_destroys_fire", "an update lowering confidence n -> l destroys a fire", 10, func(s *spBuild) {
		a := s.det(sN20, 0, 0, 0, cN, 250, 5)
		b := s.det(sN20, 0, 180, 0, cN, 250, 5)
		s.phase("fire", nil, s.fire("WASPADA", a, b))
		s.update(b, cL, 250, 50)
		s.phase("one single", []Det{a})
	}},
	{"frp_update_lowers_severity", "31 MW SIAGA becomes 24 MW WASPADA after an frp update", 10, func(s *spBuild) {
		a := s.det(sN20, 0, 0, 0, cN, 1200, 5)
		b := s.det(sN20, 0, 90, 0, cN, 1000, 5)
		c := s.det(sN20, 0, 0, 90, cN, 900, 5)
		s.phase("31 MW", nil, s.fire("SIAGA", a, b, c))
		a = s.update(a, cN, 500, 45)
		s.phase("24 MW", nil, s.fire("WASPADA", a, b, c))
	}},
	{"duplicates_counted_once", "the same detections re-sent three times still count once", 10, func(s *spBuild) {
		a := s.det(sN20, 0, 0, 0, cN, 250, 5)
		b := s.det(sN20, 0, 90, 0, cN, 250, 5)
		s.dup(a, 20)
		s.dup(b, 25)
		s.dup(a, 90)
		s.phase("still two", nil, s.fire("WASPADA", a, b))
	}},
	{"update_raises_max_confidence", "an update n -> h changes max_confidence", 10, func(s *spBuild) {
		a := s.det(sN20, 0, 0, 0, cN, 250, 5)
		b := s.det(sN20, 0, 90, 0, cN, 250, 5)
		f := s.fire("WASPADA", a, b)
		f.MaxConfidence = "n"
		s.phase("nominal", nil, f)
		a = s.update(a, cH, 250, 40)
		f = s.fire("WASPADA", a, b)
		f.MaxConfidence = "h"
		s.phase("high", nil, f)
	}},
	{"same_place_next_day", "the same pixel 24 h later is a different detection and too far in time: two singles", 1450, func(s *spBuild) {
		a := s.det(sN20, 0, 0, 0, cN, 250, 5)
		b := s.det(sN20, 1440, 0, 0, cN, 250, 5)
		s.phase("separate", []Det{a, b})
	}},
	{"across_midnight", "23:58 and 00:03 on the next date are 5 minutes apart (acq_time \"2358\" and \"3\")", 1450, func(s *spBuild) {
		start := s.b.cfg.Start
		toMidnight := 1440 - (int32(start.Hour()*60+start.Minute())+s.t)%1440
		if toMidnight < 5 {
			toMidnight += 1440
		}
		a := s.det(sN20, toMidnight-2, 0, 0, cN, 250, 5)
		b := s.det(sN21, toMidnight+3, 90, 0, cN, 250, 5)
		s.phase("linked", nil, s.fire("WASPADA", a, b))
	}},
	{"three_satellites", "one fire seen by all three satellites with slightly different coordinates", 40, func(s *spBuild) {
		a := s.det(sN, 0, 0, 0, cN, 300, 5)
		b := s.det(sN20, 2, 130, 40, cH, 300, 5)
		c := s.det(sN21, 35, -60, 110, cN, 300, 5)
		f := s.fire("WASPADA", a, b, c)
		f.Satellites, f.MaxConfidence = []string{"N", "N20", "N21"}, "h"
		s.phase("one fire", nil, f)
	}},
	{"low_confidence_cluster", "four low-confidence detections close together are ignored entirely", 10, func(s *spBuild) {
		s.cluster(sN20, 0, 0, 0, 4, 0, cL, 300)
		s.phase("nothing", nil)
	}},
}

func (b *builder) buildSpecials() {
	slot := 0
	for inst, base := range []int32{90, 14*60 + 20, 30*60 + 40} {
		for _, def := range specialDefs {
			t := base + int32(b.r.intn(40))
			if t+def.span >= b.spanMin || slot >= spRows*spCols {
				continue
			}
			b.specials = append(b.specials, Special{Name: def.name, Instance: inst, Rule: def.rule})
			s := &spBuild{b: b, idx: len(b.specials) - 1, t: t,
				lat: spLat0 + int32(slot/spCols)*spStep, lon: spLon0 + int32(slot%spCols)*spStep}
			slot++
			def.fn(s)
		}
	}
}
