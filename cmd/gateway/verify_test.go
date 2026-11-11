package main

import (
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"atc/internal/fsm"
)

// The three receivers from docker-compose.yml.
func testReceivers() []*receiver {
	rxs, _ := parseReceivers("receiver-a=20,20;receiver-b=180,40;receiver-c=100,190", 200)
	return rxs
}

func TestParseReceivers(t *testing.T) {
	rxs, err := parseReceivers(" receiver-a=20,20 ; receiver-b = 180, 40;", 150)
	if err != nil || len(rxs) != 2 || rxs[1].ID != "receiver-b" || rxs[1].X != 180 || rxs[1].Y != 40 || rxs[0].Range != 150 {
		t.Fatalf("got %+v, %v", rxs, err)
	}
	for _, bad := range []string{"receiver-a", "receiver-a=20", "=1,2", "r=x,2"} {
		if _, err := parseReceivers(bad, 200); err == nil {
			t.Errorf("%q: want error", bad)
		}
	}
	if newVerifier(defaultVerifyConfig(), nil) != nil {
		t.Fatal("no receivers must mean verification off (nil)")
	}
}

func TestClassify(t *testing.T) {
	rxs := testReceivers()
	all := func(*receiver) bool { return true }
	exact := func(ex, ey float64) map[string]float64 { // ranges a receiver measures to an emitter
		m := map[string]float64{}
		for _, rx := range rxs {
			if d := math.Hypot(ex-rx.X, ey-rx.Y); d <= rx.Range {
				m[rx.ID] = d
			}
		}
		return m
	}

	if d, _, why := classify(60, 120, exact(60, 120), rxs, all, 3); d != decPass {
		t.Errorf("real aircraft: got %v %s", d, why)
	}
	// Ghost: claims (60,120), transmitter is the spoofer at (100,20).
	if d, res, _ := classify(60, 120, exact(100, 20), rxs, all, 3); d != decFail || res < 3 {
		t.Errorf("ghost: got %v residual %.1f", d, res)
	}
	// Claimed position heard by nobody although 3 receivers cover it.
	if d, _, why := classify(60, 120, map[string]float64{}, rxs, all, 3); d != decFail || !strings.Contains(why, "heard nothing") {
		t.Errorf("silent: got %v %s", d, why)
	}
	// One silent receiver out of three is tolerated (packet loss).
	m := exact(60, 120)
	delete(m, "receiver-b")
	if d, _, why := classify(60, 120, m, rxs, all, 3); d != decPass {
		t.Errorf("one missing: got %v %s", d, why)
	}
	// Only receiver-c is up: not enough coverage to decide.
	onlyC := func(r *receiver) bool { return r.ID == "receiver-c" }
	if d, _, _ := classify(60, 120, map[string]float64{"receiver-c": 1000}, rxs, onlyC, 3); d != decNone {
		t.Errorf("one live receiver: got %v, want no decision", d)
	}
}

// sim drives a verifier the way the simulator and receivers do in Docker:
// each tick, the claim goes to the gateway and the emitter position goes to
// every live receiver, which reports a noisy range if it is in coverage.
type sim struct {
	v    *verifier
	rng  *rand.Rand
	now  time.Time
	seq  map[string]uint64
	dead map[string]bool
	sd   float64 // receiver noise, NM
}

func newSim(seed int64, sd float64) *sim {
	return &sim{v: newVerifier(defaultVerifyConfig(), testReceivers()), rng: rand.New(rand.NewSource(seed)),
		now: time.Unix(1_000_000, 0), seq: map[string]uint64{}, dead: map[string]bool{}, sd: sd}
}

type flight struct {
	cs           string
	x, y, vx, vy float64 // claimed track, NM and NM per tick
	emitX, emitY float64
	ghost        bool
}

func (s *sim) tick(fs []*flight) {
	batches := map[string][]observation{}
	for _, f := range fs {
		f.x, f.y = math.Mod(f.x+f.vx+200, 200), math.Mod(f.y+f.vy+200, 200)
		ex, ey := f.x, f.y
		if f.ghost {
			ex, ey = f.emitX, f.emitY
		}
		s.seq[f.cs]++
		s.v.claim(position{Callsign: f.cs, Seq: s.seq[f.cs], X: f.x, Y: f.y}, s.now)
		for _, rx := range s.v.receivers {
			d := math.Hypot(ex-rx.X, ey-rx.Y)
			if s.dead[rx.ID] || d > rx.Range {
				continue
			}
			batches[rx.ID] = append(batches[rx.ID], observation{f.cs, s.seq[f.cs], math.Max(0, d+s.rng.NormFloat64()*s.sd)})
		}
	}
	for _, rx := range s.v.receivers {
		if !s.dead[rx.ID] {
			s.v.observe(observationBatch{ReceiverID: rx.ID, Observations: batches[rx.ID]}, s.now)
		}
	}
	s.now = s.now.Add(time.Second)
	s.v.evaluate(s.now)
}

func fleet(rng *rand.Rand, n int) []*flight {
	var fs []*flight
	for i := 0; i < n; i++ {
		h := rng.Float64() * 2 * math.Pi
		sp := (250 + rng.Float64()*230) / 3600 // NM per 1 s tick
		fs = append(fs, &flight{cs: "ATC" + string(rune('A'+i/26)) + string(rune('A'+i%26)),
			x: rng.Float64() * 200, y: rng.Float64() * 200, vx: sp * math.Sin(h), vy: sp * math.Cos(h)})
	}
	return fs
}

func TestGhostDetectedRealAircraftNot(t *testing.T) {
	s := newSim(1, 0.5)
	fs := fleet(s.rng, 60)
	ghost := &flight{cs: "GHOST01", x: 40, y: 150, vx: 0.1, vy: 0, emitX: 100, emitY: 20, ghost: true}
	fs = append(fs, ghost)

	for i := 0; i < 120; i++ {
		s.tick(fs)
		if i == 4 && s.v.state("GHOST01") != VerSuspect {
			t.Fatalf("ghost not SUSPECT after 5 ticks: %+v", s.v.tracks["GHOST01"])
		}
	}
	for _, f := range fs[:60] {
		if st := s.v.state(f.cs); st == VerSuspect {
			t.Errorf("false positive: %s %+v", f.cs, s.v.tracks[f.cs])
		}
	}
	if s.v.state("ATCAA") != VerVerified {
		t.Errorf("real aircraft should be VERIFIED, got %s", s.v.state("ATCAA"))
	}
	if a := s.v.alerts(); len(a) != 1 || a[0].Callsign != "GHOST01" {
		t.Fatalf("alerts %+v", a)
	}
	if s.v.st.Detected != 1 {
		t.Fatalf("stats %+v", s.v.st)
	}
}

// docker kill receiver-b: coverage drops, nothing turns falsely SUSPECT.
func TestReceiverDownNoFalseSuspect(t *testing.T) {
	s := newSim(2, 0.5)
	fs := fleet(s.rng, 80)
	for i := 0; i < 10; i++ {
		s.tick(fs)
	}
	s.dead["receiver-b"] = true
	for i := 0; i < 60; i++ {
		s.tick(fs)
	}
	unverified := 0
	for _, f := range fs {
		switch s.v.state(f.cs) {
		case VerSuspect:
			t.Errorf("false positive with receiver-b down: %s %+v", f.cs, s.v.tracks[f.cs])
		case VerUnverified:
			unverified++
		}
	}
	for _, rx := range s.v.receiverSnapshot() {
		if rx.ID == "receiver-b" && rx.Up {
			t.Error("receiver-b should be reported down")
		}
	}
	t.Logf("%d of %d aircraft UNVERIFIED with receiver-b down", unverified, len(fs))
}

func TestHysteresis(t *testing.T) {
	v := newVerifier(defaultVerifyConfig(), testReceivers())
	now := time.Unix(0, 0)
	step := func(d decision) string { v.apply("X", d, 0, "", now); return v.state("X") }

	if step(decPass) != VerUnverified || step(decPass) != VerVerified {
		t.Fatal("2 passes should verify")
	}
	if step(decFail) != VerVerified || step(decFail) != VerVerified || step(decFail) != VerSuspect {
		t.Fatal("3 fails in a row should make SUSPECT")
	}
	if step(decNone) != VerSuspect {
		t.Fatal("poor coverage must not clear a SUSPECT")
	}
	for i := 0; i < 4; i++ {
		if step(decPass) != VerSuspect {
			t.Fatalf("cleared after only %d passes", i+1)
		}
	}
	if step(decPass) != VerVerified {
		t.Fatal("5 passes should clear SUSPECT")
	}
	step(decFail)
	step(decFail)
	step(decPass) // streak broken
	if step(decFail) != VerVerified {
		t.Fatal("fails must be consecutive")
	}
}

func TestReportWaitsForReceivers(t *testing.T) {
	v := newVerifier(defaultVerifyConfig(), testReceivers())
	t0 := time.Unix(0, 0)
	for _, rx := range v.receivers { // all three live
		v.observe(observationBatch{ReceiverID: rx.ID}, t0)
	}
	v.claim(position{Callsign: "ATC001", Seq: 1, X: 60, Y: 120}, t0)
	v.observe(observationBatch{ReceiverID: "receiver-a", Observations: []observation{{"ATC001", 1, math.Hypot(40, 100)}}}, t0)

	v.evaluate(t0.Add(500 * time.Millisecond))
	if v.st.Evaluated != 0 {
		t.Fatal("must wait while receivers b and c have not answered")
	}
	v.evaluate(t0.Add(2 * time.Second))
	if v.st.Evaluated != 1 || v.st.Fails != 1 {
		t.Fatalf("after the wait, 2 silent receivers is a fail: %+v", v.st)
	}
	if len(v.pending) != 0 {
		t.Fatal("decided reports must be dropped")
	}
	if err := v.observe(observationBatch{ReceiverID: "rogue"}, t0); err == nil || v.st.Unknown != 1 {
		t.Fatal("unknown receiver must be refused")
	}
}

func TestClearanceRefusedForSuspect(t *testing.T) {
	h := newHarness(t)
	h.g.ver = newVerifier(defaultVerifyConfig(), testReceivers())
	h.own("GHOST01", "sector-1", fsm.Active)
	for i := 0; i < 3; i++ {
		h.g.ver.apply("GHOST01", decFail, 80, "receiver-a range off", time.Now())
	}

	code, c := h.post(t, "GHOST01", "DESCEND FL100")
	if code != http.StatusConflict || !strings.Contains(c.Reason, "verification") {
		t.Fatalf("got %d %+v", code, c)
	}
	if len(h.sectors["sector-1"].applied) != 0 {
		t.Fatal("no clearance may be committed for a SUSPECT aircraft")
	}
}

func TestBufferedClearanceRefusedOnceSuspect(t *testing.T) {
	h := newHarness(t)
	h.g.ver = newVerifier(defaultVerifyConfig(), testReceivers())
	h.own("ATC010", "sector-2", fsm.Active)
	h.setLeader("sector-2", false)
	if code, _ := h.post(t, "ATC010", "HEADING 090"); code != http.StatusAccepted {
		t.Fatalf("want pending, got %d", code)
	}
	h.g.mu.Lock()
	for i := 0; i < 3; i++ {
		h.g.ver.apply("ATC010", decFail, 50, "", time.Now())
	}
	h.g.mu.Unlock()
	h.setLeader("sector-2", true)
	h.g.flushQueue("sector-2")
	if c := h.g.clr.recent[0]; c.Status != ClrRejected || len(h.sectors["sector-2"].applied) != 0 {
		t.Fatalf("buffered clearance for a now-SUSPECT aircraft must be rejected, got %+v", c)
	}
}

func TestObservationsHandler(t *testing.T) {
	g := &gateway{ver: newVerifier(defaultVerifyConfig(), testReceivers())}
	post := func(body string) int {
		rec := httptest.NewRecorder()
		g.observationsHandler(rec, httptest.NewRequest("POST", "/api/observations", strings.NewReader(body)))
		return rec.Code
	}
	if c := post(`{"receiver_id":"receiver-a","observations":[{"callsign":"ATC001","seq":1,"range_nm":12.5}]}`); c != http.StatusAccepted {
		t.Fatalf("got %d", c)
	}
	if c := post(`{"receiver_id":"nope","observations":[]}`); c != http.StatusForbidden {
		t.Fatalf("unknown receiver: got %d", c)
	}
	if c := post(`not json`); c != http.StatusBadRequest {
		t.Fatalf("bad json: got %d", c)
	}
	if g.ver.st.Observations != 1 {
		t.Fatalf("stats %+v", g.ver.st)
	}
}
