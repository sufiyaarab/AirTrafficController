package main

// Ghost-aircraft detection (docs/collins_design.md, section 6).
//
// ADS-B is unauthenticated: an aircraft broadcasts its own position, and a
// spoofer can broadcast a fake aircraft anywhere. The simulator's position
// reports are exactly that kind of self-reported claim. The check here
// compares each claim with something the transmitter cannot fake: the
// distance between the transmitter and several ground receivers.
//
// Each tick the simulator sends every receiver the physical emitter position
// of every aircraft (for a real aircraft the same as its claimed position, for
// a ghost the spoofer's ground station). Each receiver turns that into a
// measured range (plus noise), drops what is out of its coverage, and posts
// the ranges here. The gateway never sees the emitter position, and the
// receivers never see the claimed one.
//
// For one report (callsign, seq) the gateway waits until every receiver that
// should have heard it has answered, or verifyWait has passed, then:
//
//   - fewer than 2 live receivers cover the claimed position: no decision;
//   - any |dist(claimed, receiver) - range| > threshold, or 2 or more
//     receivers that should have heard it are silent: fail;
//   - otherwise: pass.
//
// Hysteresis keeps noise from flickering the state shown to controllers:
// SUSPECT after 3 fails in a row, back to VERIFIED after 5 passes in a row,
// UNVERIFIED -> VERIFIED after 2. With no decision an aircraft goes back to
// UNVERIFIED, except a SUSPECT stays SUSPECT (a ghost must not clear itself
// by flying into poor coverage).
//
// A receiver counts as live while it has posted a batch (even an empty one)
// within receiverTimeout. A dead receiver is never "expected", so killing one
// lowers coverage (more UNVERIFIED) but does not make real aircraft SUSPECT.
//
// The check uses distances only, so the receivers need no shared clock. Real
// systems multilaterate from time differences of arrival with synchronized
// clocks; reporting ranges is a stated simplification of the same idea.

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Verification states shown on the radar.
const (
	VerUnverified = "UNVERIFIED"
	VerVerified   = "VERIFIED"
	VerSuspect    = "SUSPECT"
)

type verifyConfig struct {
	Threshold       float64       // NM; residual above this is a fail (6 sigma of 0.5 NM noise)
	Wait            time.Duration // how long to wait for receivers to report one seq
	ReceiverTimeout time.Duration // receiver is down after this long without a batch
	FailsToSuspect  int
	PassesToClear   int // SUSPECT -> VERIFIED
	PassesToVerify  int // UNVERIFIED -> VERIFIED
}

func defaultVerifyConfig() verifyConfig {
	return verifyConfig{
		Threshold:       3,
		Wait:            2 * time.Second,
		ReceiverTimeout: 3 * time.Second,
		FailsToSuspect:  3,
		PassesToClear:   5,
		PassesToVerify:  2,
	}
}

type receiver struct {
	ID    string  `json:"id"`
	X     float64 `json:"x"`
	Y     float64 `json:"y"`
	Range float64 `json:"range"` // coverage radius, NM
	Up    bool    `json:"up"`
	Last  int64   `json:"last_ms,omitempty"` // unix ms of the last batch
	last  time.Time
}

func (r *receiver) live(now time.Time, timeout time.Duration) bool {
	return !r.last.IsZero() && now.Sub(r.last) <= timeout
}

// report is one (callsign, seq) waiting for its observations.
type report struct {
	claimed bool
	x, y    float64
	ranges  map[string]float64 // receiver ID -> measured range, NM
	first   time.Time          // when the claim or the first observation arrived
}

// track is one aircraft's verification state.
type track struct {
	State    string  `json:"state"`
	Fails    int     `json:"fails"`  // consecutive
	Passes   int     `json:"passes"` // consecutive
	SinceMs  int64   `json:"since_ms"`
	Residual float64 `json:"residual_nm"` // worst residual of the last decision
	Reason   string  `json:"reason,omitempty"`
}

type verifyStats struct {
	Observations int `json:"observations"`
	Evaluated    int `json:"evaluated"`
	Passes       int `json:"passes"`
	Fails        int `json:"fails"`
	NoDecision   int `json:"no_decision"`
	Detected     int `json:"detected"` // transitions into SUSPECT
	Cleared      int `json:"cleared"`  // transitions SUSPECT -> VERIFIED
	Unknown      int `json:"unknown_receiver"`
}

type alert struct {
	Callsign string  `json:"callsign"`
	SinceMs  int64   `json:"since_ms"`
	Residual float64 `json:"residual_nm"`
	Reason   string  `json:"reason"`
}

// verifier is guarded by g.mu. A nil verifier means verification is off
// (no receivers configured): every method is then a no-op.
type verifier struct {
	cfg       verifyConfig
	receivers []*receiver
	byID      map[string]*receiver
	pending   map[string]map[uint64]*report // callsign -> seq -> report
	tracks    map[string]*track
	st        verifyStats
}

func newVerifier(cfg verifyConfig, rxs []*receiver) *verifier {
	if len(rxs) == 0 {
		return nil
	}
	v := &verifier{cfg: cfg, receivers: rxs, byID: map[string]*receiver{},
		pending: map[string]map[uint64]*report{}, tracks: map[string]*track{}}
	for _, r := range rxs {
		v.byID[r.ID] = r
	}
	return v
}

// verifierFromEnv reads RECEIVERS="receiver-a=20,20;receiver-b=180,40",
// RECEIVER_RANGE_NM, VERIFY_THRESHOLD_NM and VERIFY_WAIT_MS.
func verifierFromEnv() *verifier {
	cfg := defaultVerifyConfig()
	if v, err := strconv.ParseFloat(os.Getenv("VERIFY_THRESHOLD_NM"), 64); err == nil && v > 0 {
		cfg.Threshold = v
	}
	if v, err := strconv.Atoi(os.Getenv("VERIFY_WAIT_MS")); err == nil && v > 0 {
		cfg.Wait = time.Duration(v) * time.Millisecond
	}
	rng := 200.0
	if v, err := strconv.ParseFloat(os.Getenv("RECEIVER_RANGE_NM"), 64); err == nil && v > 0 {
		rng = v
	}
	rxs, err := parseReceivers(os.Getenv("RECEIVERS"), rng)
	if err != nil {
		log.Fatalf("RECEIVERS: %v", err)
	}
	v := newVerifier(cfg, rxs)
	if v == nil {
		log.Printf("verification off: RECEIVERS not set")
	} else {
		log.Printf("verification on: %d receivers, range %.0f NM, threshold %.1f NM", len(rxs), rng, cfg.Threshold)
	}
	return v
}

func parseReceivers(s string, rng float64) ([]*receiver, error) {
	var out []*receiver
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, xy, ok := strings.Cut(part, "=")
		xs, ys, ok2 := strings.Cut(xy, ",")
		x, errX := strconv.ParseFloat(strings.TrimSpace(xs), 64)
		y, errY := strconv.ParseFloat(strings.TrimSpace(ys), 64)
		if !ok || !ok2 || errX != nil || errY != nil || strings.TrimSpace(id) == "" {
			return nil, fmt.Errorf("bad entry %q, want id=x,y", part)
		}
		out = append(out, &receiver{ID: strings.TrimSpace(id), X: x, Y: y, Range: rng})
	}
	return out, nil
}

func (v *verifier) entry(callsign string, seq uint64, now time.Time) *report {
	m := v.pending[callsign]
	if m == nil {
		m = map[uint64]*report{}
		v.pending[callsign] = m
	}
	r := m[seq]
	if r == nil {
		r = &report{ranges: map[string]float64{}, first: now}
		m[seq] = r
	}
	return r
}

// claim records the position an aircraft says it is at (from /api/positions).
func (v *verifier) claim(p position, now time.Time) {
	if v == nil || p.Seq == 0 {
		return
	}
	r := v.entry(p.Callsign, p.Seq, now)
	r.claimed, r.x, r.y = true, p.X, p.Y
}

type observation struct {
	Callsign string  `json:"callsign"`
	Seq      uint64  `json:"seq"`
	RangeNM  float64 `json:"range_nm"`
}

type observationBatch struct {
	ReceiverID   string        `json:"receiver_id"`
	Observations []observation `json:"observations"`
}

// observe records one receiver's batch. An empty batch is a heartbeat.
func (v *verifier) observe(b observationBatch, now time.Time) error {
	rx := v.byID[b.ReceiverID]
	if rx == nil {
		v.st.Unknown++
		return fmt.Errorf("unknown receiver %q", b.ReceiverID)
	}
	rx.last = now
	for _, o := range b.Observations {
		if o.Callsign == "" || o.Seq == 0 || o.RangeNM < 0 || math.IsNaN(o.RangeNM) || math.IsInf(o.RangeNM, 0) {
			continue
		}
		v.entry(o.Callsign, o.Seq, now).ranges[rx.ID] = o.RangeNM
		v.st.Observations++
	}
	return nil
}

type decision int

const (
	decNone decision = iota
	decPass
	decFail
)

// classify decides one report against the receivers that are live now.
// It is a pure function of its inputs so it can be tested directly.
func classify(x, y float64, ranges map[string]float64, rxs []*receiver, live func(*receiver) bool, threshold float64) (decision, float64, string) {
	expected, missing := 0, 0
	worst, worstRx := 0.0, ""
	for _, rx := range rxs {
		d := math.Hypot(x-rx.X, y-rx.Y)
		measured, heard := ranges[rx.ID]
		if heard {
			if res := math.Abs(d - measured); res > worst {
				worst, worstRx = res, rx.ID
			}
		}
		if live(rx) && d <= rx.Range {
			expected++
			if !heard {
				missing++
			}
		}
	}
	switch {
	case expected < 2:
		return decNone, worst, fmt.Sprintf("only %d receiver(s) cover the claimed position", expected)
	case worst > threshold:
		return decFail, worst, fmt.Sprintf("%s range off by %.1f NM", worstRx, worst)
	case missing >= 2:
		return decFail, worst, fmt.Sprintf("%d of %d receivers in coverage heard nothing", missing, expected)
	}
	return decPass, worst, ""
}

// complete reports whether every live receiver covering the claim has answered,
// so the report can be decided before verifyWait runs out.
func (v *verifier) complete(r *report, now time.Time) bool {
	for _, rx := range v.receivers {
		if !rx.live(now, v.cfg.ReceiverTimeout) || math.Hypot(r.x-rx.X, r.y-rx.Y) > rx.Range {
			continue
		}
		if _, ok := r.ranges[rx.ID]; !ok {
			return false
		}
	}
	return true
}

// evaluate decides every report that is complete or has waited long enough,
// oldest seq first per aircraft so the hysteresis sees them in order.
func (v *verifier) evaluate(now time.Time) {
	if v == nil {
		return
	}
	for _, rx := range v.receivers {
		rx.Up = rx.live(now, v.cfg.ReceiverTimeout)
		if !rx.last.IsZero() {
			rx.Last = rx.last.UnixMilli()
		}
	}
	live := func(rx *receiver) bool { return rx.Up }
	for cs, m := range v.pending {
		seqs := make([]uint64, 0, len(m))
		for s := range m {
			seqs = append(seqs, s)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		for _, s := range seqs {
			r := m[s]
			waited := now.Sub(r.first) >= v.cfg.Wait
			if !r.claimed {
				// Observations for a report the gateway never got (or not yet).
				if now.Sub(r.first) >= 3*v.cfg.Wait {
					delete(m, s)
				}
				continue
			}
			if !waited && !v.complete(r, now) {
				break // keep order: later seqs wait for this one
			}
			dec, res, why := classify(r.x, r.y, r.ranges, v.receivers, live, v.cfg.Threshold)
			v.apply(cs, dec, res, why, now)
			delete(m, s)
		}
		if len(m) == 0 {
			delete(v.pending, cs)
		}
	}
}

// apply moves one aircraft's state machine by one decision.
func (v *verifier) apply(cs string, dec decision, residual float64, why string, now time.Time) {
	t := v.tracks[cs]
	if t == nil {
		t = &track{State: VerUnverified, SinceMs: now.UnixMilli()}
		v.tracks[cs] = t
	}
	v.st.Evaluated++
	t.Residual = residual
	set := func(state, reason string) {
		if t.State != state {
			log.Printf("verify %s: %s -> %s %s", cs, t.State, state, reason)
			t.State, t.SinceMs = state, now.UnixMilli()
		}
		t.Reason = reason
	}
	switch dec {
	case decNone:
		v.st.NoDecision++
		t.Fails, t.Passes = 0, 0
		if t.State != VerSuspect {
			set(VerUnverified, why)
		}
	case decFail:
		v.st.Fails++
		t.Fails++
		t.Passes = 0
		if t.State == VerSuspect {
			t.Reason = why
		} else if t.Fails >= v.cfg.FailsToSuspect {
			v.st.Detected++
			set(VerSuspect, why)
		}
	case decPass:
		v.st.Passes++
		t.Passes++
		t.Fails = 0
		switch {
		case t.State == VerSuspect && t.Passes >= v.cfg.PassesToClear:
			v.st.Cleared++
			set(VerVerified, "")
		case t.State == VerUnverified && t.Passes >= v.cfg.PassesToVerify:
			set(VerVerified, "")
		}
	}
}

// state returns an aircraft's verification state ("" when verification is off).
func (v *verifier) state(callsign string) string {
	if v == nil {
		return ""
	}
	if t := v.tracks[callsign]; t != nil {
		return t.State
	}
	return VerUnverified
}

func (v *verifier) alerts() []alert {
	if v == nil {
		return nil
	}
	var out []alert
	for cs, t := range v.tracks {
		if t.State == VerSuspect {
			out = append(out, alert{Callsign: cs, SinceMs: t.SinceMs, Residual: t.Residual, Reason: t.Reason})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SinceMs > out[j].SinceMs })
	return out
}

func (v *verifier) receiverSnapshot() []receiver {
	if v == nil {
		return nil
	}
	out := make([]receiver, len(v.receivers))
	for i, r := range v.receivers {
		out[i] = *r
	}
	return out
}

// ---------- HTTP and loop ----------

// observationsHandler: POST /api/observations from a ground receiver.
func (g *gateway) observationsHandler(w http.ResponseWriter, r *http.Request) {
	var b observationBatch
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "bad observation batch: "+err.Error())
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ver == nil {
		writeErr(w, http.StatusNotFound, "verification is off (RECEIVERS not set)")
		return
	}
	if err := g.ver.observe(b, time.Now()); err != nil {
		writeErr(w, http.StatusForbidden, err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (g *gateway) verifyLoop() {
	for range time.Tick(250 * time.Millisecond) {
		g.mu.Lock()
		g.ver.evaluate(time.Now())
		g.mu.Unlock()
	}
}
