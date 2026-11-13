package main

// Tests for elastic.go. The planner tests feed plan() a hand-made view, so they
// need no network. The lifecycle test puts the real replicated state machine
// behind a fake registry endpoint, so the gateway/registry JSON contract and the
// registry's safety rules are exercised together.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"atc/internal/fsm"

	"github.com/hashicorp/raft"
)

// ---------- helpers ----------

func testCfg() elasticConfig {
	return elasticConfig{
		Enabled: true, SplitAt: 40, MergeAt: 20, Sustain: 3,
		Cooldown: 15 * time.Second, DrainTimeout: 30 * time.Second, MinHalf: 25,
		Pool: []string{"standby"},
	}
}

func sec(name, status string, r fsm.Region) fsm.Sector {
	return fsm.Sector{Name: name, Status: status, Region: r}
}

func mapOf(version uint64, secs ...fsm.Sector) fsm.SectorMap {
	m := fsm.SectorMap{Version: version, Sectors: map[string]fsm.Sector{}}
	for _, s := range secs {
		m.Sectors[s.Name] = s
	}
	return m
}

var (
	whole = fsm.Region{MinX: 0, MinY: 100, MaxX: 100, MaxY: 200} // sector-1's quadrant
	left  = fsm.Region{MinX: 0, MinY: 100, MaxX: 50, MaxY: 200}
	right = fsm.Region{MinX: 50, MinY: 100, MaxX: 100, MaxY: 200}
)

func unsplit() fsm.SectorMap {
	return mapOf(5, sec("sector-1", fsm.SectorActive, whole), sec("standby", fsm.SectorStandby, fsm.Region{}))
}

func split(standbyStatus string) fsm.SectorMap {
	return mapOf(9, sec("sector-1", fsm.SectorActive, left), sec("standby", standbyStatus, right))
}

func mkView(m fsm.SectorMap, load map[string]int, now time.Time) view {
	v := view{m: m, load: load, now: now, ready: map[string]bool{}, owned: map[string]int{}, axis: map[string]string{}}
	for name, s := range m.Sectors {
		v.ready[name] = true
		if s.Status == fsm.SectorActive {
			v.axis[name] = "x"
		}
	}
	return v
}

// tickN runs plan n times one second apart and returns the last answer.
func tickN(e *elastic, v view, n int) action {
	var a action
	for i := 0; i < n; i++ {
		v.now = v.now.Add(time.Second)
		a = e.plan(v)
	}
	return a
}

var t0 = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)

// ---------- routing ----------

func TestGeoRoutesBySectorMap(t *testing.T) {
	var g gateway
	if got := g.geoSector(position{X: 10, Y: 10}); got != "sector-3" {
		t.Fatalf("before the map is read the four quadrants apply, got %q", got)
	}

	g.geo.Store(buildGeo(split(fsm.SectorActive)))
	for _, c := range []struct {
		x, y float64
		want string
	}{
		{10, 150, "sector-1"}, {49.9, 150, "sector-1"}, {50, 150, "standby"}, {99, 199, "standby"},
		{100, 100, "tower"}, // the tower circle still wins at the centre
	} {
		if got := g.geoSector(position{X: c.x, Y: c.y, Alt: 5000}); got != c.want {
			t.Errorf("(%v,%v) -> %q, want %q", c.x, c.y, got, c.want)
		}
	}

	// while standby drains, its airspace is attributed to the neighbour
	g.geo.Store(buildGeo(split(fsm.SectorDraining)))
	if got := g.geoSector(position{X: 75, Y: 150}); got != "sector-1" {
		t.Fatalf("draining airspace must route to the neighbour, got %q", got)
	}
	// and a STANDBY cluster has no airspace at all
	g.geo.Store(buildGeo(unsplit()))
	if got := g.geoSector(position{X: 75, Y: 150}); got != "sector-1" {
		t.Fatalf("got %q", got)
	}
}

func TestAdoptMapNeverGoesBackwards(t *testing.T) {
	var g gateway
	g.adoptMap(split(fsm.SectorActive)) // version 9
	g.adoptMap(unsplit())               // version 5: a stale read from a deposed leader
	if g.smap.Version != 9 || g.geoMap().version != 9 {
		t.Fatalf("older map was adopted: %d / %d", g.smap.Version, g.geoMap().version)
	}
}

func TestBestAxis(t *testing.T) {
	r := fsm.Region{MinX: 0, MinY: 0, MaxX: 100, MaxY: 100}
	// everyone in the left strip, spread top to bottom: only a vertical cut separates them
	var strip [][2]float64
	for y := 5.0; y < 100; y += 10 {
		strip = append(strip, [2]float64{10, y})
	}
	if got := bestAxis(r, strip, 25); got != "x" && got != "y" {
		t.Fatal("must pick a cut")
	}
	// all aircraft in the bottom band, spread left to right: a cut on y leaves one half empty, x is balanced
	var band [][2]float64
	for x := 5.0; x < 100; x += 10 {
		band = append(band, [2]float64{x, 10})
	}
	if got := bestAxis(r, band, 25); got != "x" {
		t.Fatalf("balanced cut is x, got %q", got)
	}
	// the mirror image
	var col [][2]float64
	for y := 5.0; y < 100; y += 10 {
		col = append(col, [2]float64{10, y})
	}
	if got := bestAxis(r, col, 25); got != "y" {
		t.Fatalf("balanced cut is y, got %q", got)
	}
	// halves thinner than the minimum are never offered
	if got := bestAxis(fsm.Region{MaxX: 40, MaxY: 100}, col, 25); got != "y" {
		t.Fatalf("a 40 wide sector cannot be cut on x, got %q", got)
	}
	if got := bestAxis(fsm.Region{MaxX: 40, MaxY: 40}, col, 25); got != "" {
		t.Fatalf("too small to cut, got %q", got)
	}
	// tie: cut the longer side
	if got := bestAxis(fsm.Region{MaxX: 200, MaxY: 100}, nil, 25); got != "x" {
		t.Fatalf("got %q", got)
	}
}

// ---------- planner: split ----------

func TestPlanSplitNeedsSustainedLoad(t *testing.T) {
	e := newElastic(testCfg())
	v := mkView(unsplit(), map[string]int{"sector-1": 45}, t0)

	if a := tickN(e, v, 2); a.kind != "" {
		t.Fatalf("two checks are not enough: %+v", a)
	}
	// a dip below the limit restarts the count
	if a := tickN(e, mkView(unsplit(), map[string]int{"sector-1": 39}, t0.Add(2*time.Second)), 1); a.kind != "" {
		t.Fatal("below the limit")
	}
	if a := tickN(e, v, 2); a.kind != "" {
		t.Fatalf("the count must have restarted: %+v", a)
	}
	a := tickN(e, v, 1)
	if a.kind != actSplit || a.sector != "sector-1" || a.child != "standby" || a.axis != "x" {
		t.Fatalf("want split sector-1 -> standby on x, got %+v", a)
	}
}

func TestPlanSplitPicksHottestSector(t *testing.T) {
	e := newElastic(testCfg())
	m := mapOf(5,
		sec("sector-1", fsm.SectorActive, left),
		sec("sector-2", fsm.SectorActive, right),
		sec("standby", fsm.SectorStandby, fsm.Region{}))
	a := tickN(e, mkView(m, map[string]int{"sector-1": 44, "sector-2": 61}, t0), 3)
	if a.kind != actSplit || a.sector != "sector-2" {
		t.Fatalf("the busier sector goes first, got %+v", a)
	}
}

func TestPlanSplitWaitsForASpareAndARoomyEnoughSector(t *testing.T) {
	e := newElastic(testCfg())
	v := mkView(unsplit(), map[string]int{"sector-1": 50}, t0)

	v.ready["standby"] = false // spare cluster has no leader
	if a := tickN(e, v, 5); a.kind != "" {
		t.Fatalf("must not split into a cluster that is not Ready: %+v", a)
	}
	v.ready["standby"] = true
	v.axis["sector-1"] = "" // too small to cut
	if a := tickN(e, v, 5); a.kind != "" {
		t.Fatalf("must not split a sector that is too small: %+v", a)
	}
	v.axis["sector-1"] = "y"
	if a := tickN(e, v, 1); a.kind != actSplit || a.axis != "y" {
		t.Fatalf("once both are fine the split goes ahead on the planner's axis: %+v", a)
	}
}

func TestPlanNeverSplitsWithoutAFreeStandby(t *testing.T) {
	e := newElastic(testCfg())
	// both halves exist already and the only spare is in use
	m := mapOf(9, sec("sector-1", fsm.SectorActive, left), sec("standby", fsm.SectorActive, right))
	if a := tickN(e, mkView(m, map[string]int{"sector-1": 70}, t0), 10); a.kind != "" {
		t.Fatalf("no STANDBY cluster is free: %+v", a)
	}
}

func TestPlanCooldownAndDisabled(t *testing.T) {
	e := newElastic(testCfg())
	v := mkView(unsplit(), map[string]int{"sector-1": 45}, t0)
	if a := tickN(e, v, 3); a.kind != actSplit {
		t.Fatal("setup")
	}
	e.done(t0.Add(3*time.Second), action{kind: actSplit})

	v.now = t0.Add(3 * time.Second)
	if a := tickN(e, v, 10); a.kind != "" { // 4s..13s: inside the 15s cooldown
		t.Fatalf("cooldown ignored: %+v", a)
	}
	v.now = t0.Add(13 * time.Second)             // tickN works on a copy, so move the clock on by hand
	if a := tickN(e, v, 8); a.kind != actSplit { // 14s..21s: cooldown ends at 18s, then 3 checks in a row
		t.Fatalf("should split again after the cooldown: %+v", a)
	}

	off := testCfg()
	off.Enabled = false
	if a := tickN(newElastic(off), mkView(unsplit(), map[string]int{"sector-1": 99}, t0), 10); a.kind != "" {
		t.Fatalf("ELASTIC=off must not act: %+v", a)
	}
}

// ---------- planner: merge ----------

func TestPlanMergeWhenBothHalvesAreQuiet(t *testing.T) {
	e := newElastic(testCfg())
	m := split(fsm.SectorActive)

	if a := tickN(e, mkView(m, map[string]int{"sector-1": 14, "standby": 12}, t0), 5); a.kind != "" {
		t.Fatalf("26 together is above the limit of 20: %+v", a)
	}
	v := mkView(m, map[string]int{"sector-1": 9, "standby": 8}, t0.Add(10*time.Second))
	v.ready["sector-1"] = false
	if a := tickN(e, v, 5); a.kind != "" {
		t.Fatalf("the neighbour that takes the flights must be Ready: %+v", a)
	}
	v.ready["sector-1"] = true
	a := tickN(e, v, 3)
	if a.kind != actDrain || a.sector != "standby" || a.into != "sector-1" {
		t.Fatalf("want drain standby into sector-1, got %+v", a)
	}
}

func TestPlanDoesNotFlapAfterASplit(t *testing.T) {
	// A sector split at 40 aircraft has about 40 between its two halves: far above the merge limit.
	e := newElastic(testCfg())
	m := split(fsm.SectorActive)
	if a := tickN(e, mkView(m, map[string]int{"sector-1": 21, "standby": 20}, t0), 20); a.kind != "" {
		t.Fatalf("flapping: %+v", a)
	}
}

func TestPlanOnlyMergesPoolSectors(t *testing.T) {
	e := newElastic(testCfg())
	m := mapOf(5, sec("sector-1", fsm.SectorActive, left), sec("sector-2", fsm.SectorActive, right))
	if a := tickN(e, mkView(m, map[string]int{"sector-1": 2, "sector-2": 1}, t0), 10); a.kind != "" {
		t.Fatalf("the original sectors are permanent: %+v", a)
	}
}

// ---------- planner: finishing a drain ----------

func TestPlanDrainRunsToTheEnd(t *testing.T) {
	e := newElastic(testCfg())
	v := mkView(split(fsm.SectorDraining), map[string]int{}, t0)

	v.owned["standby"] = 3
	if a := e.plan(v); a.kind != "" {
		t.Fatalf("flights still held, nothing to do: %+v", a)
	}
	v.now = t0.Add(5 * time.Second)
	v.owned["standby"] = 0
	if a := e.plan(v); a.kind != actMerge || a.sector != "standby" || a.into != "sector-1" {
		t.Fatalf("empty: merge, got %+v", a)
	}
}

func TestPlanDrainTimesOutAndSurvivesARestart(t *testing.T) {
	// A fresh controller meeting a DRAINING sector (a gateway that restarted
	// mid-merge) starts the timeout from the first time it sees it.
	e := newElastic(testCfg())
	v := mkView(split(fsm.SectorDraining), map[string]int{}, t0)
	v.owned["standby"] = 2
	if a := e.plan(v); a.kind != "" {
		t.Fatal(a)
	}
	v.now = t0.Add(29 * time.Second)
	if a := e.plan(v); a.kind != "" {
		t.Fatalf("not yet: %+v", a)
	}
	v.now = t0.Add(31 * time.Second)
	if a := e.plan(v); a.kind != actUndrain || a.sector != "standby" {
		t.Fatalf("a drain that cannot finish must be undone: %+v", a)
	}
}

func TestPlanFinishesAMergeEvenWhenDisabled(t *testing.T) {
	off := testCfg()
	off.Enabled = false
	v := mkView(split(fsm.SectorDraining), map[string]int{}, t0)
	if a := newElastic(off).plan(v); a.kind != actMerge {
		t.Fatalf("a half-done merge must be completed: %+v", a)
	}
}

func TestPlanUndrainsWhenNoNeighbourIsLeft(t *testing.T) {
	m := mapOf(9, sec("standby", fsm.SectorDraining, right))
	if a := newElastic(testCfg()).plan(mkView(m, nil, t0)); a.kind != actUndrain {
		t.Fatalf("%+v", a)
	}
}

// ---------- config ----------

func TestConfigClampsMergeThresholdBelowSplit(t *testing.T) {
	t.Setenv("ELASTIC_SPLIT_AT", "40")
	t.Setenv("ELASTIC_MERGE_AT", "35")
	if c := loadElasticConfig(); c.MergeAt != 20 {
		t.Fatalf("merge limit must stay well under the split limit or the halves flap, got %d", c.MergeAt)
	}
	t.Setenv("ELASTIC", "off")
	t.Setenv("ELASTIC_SUSTAIN", "banana")
	c := loadElasticConfig()
	if c.Enabled || c.Sustain != 3 {
		t.Fatalf("got %+v", c)
	}
}

// ---------- lifecycle against the real state machine ----------

// realRegistry is the registry's FSM behind the same three endpoints the node serves.
type realRegistry struct {
	mu  sync.Mutex
	f   *fsm.FSM
	idx uint64
}

func newRealRegistry() *realRegistry { return &realRegistry{f: fsm.New()} }

func (r *realRegistry) apply(c fsm.Command) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.idx++
	b, _ := json.Marshal(c)
	return r.f.Apply(&raft.Log{Index: r.idx, Data: b})
}

func (r *realRegistry) move(op, callsign, from, to string) any {
	v, _ := json.Marshal(fsm.Move{From: from, To: to})
	return r.apply(fsm.Command{Op: op, Key: callsign, Value: v})
}

func (r *realRegistry) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /sectors", func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(r.f.Sectors()) })
	mux.HandleFunc("GET /owners", func(w http.ResponseWriter, _ *http.Request) { json.NewEncoder(w).Encode(r.f.Owners()) })
	mux.HandleFunc("POST /apply", func(w http.ResponseWriter, req *http.Request) {
		var c fsm.Command
		json.NewDecoder(req.Body).Decode(&c)
		res := r.apply(c)
		if err, bad := res.(error); bad {
			w.WriteHeader(http.StatusBadRequest) // what cmd/node does for a rejected command
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"index": r.idx, "result": res})
	})
	return mux
}

func newElasticGateway(cfg elasticConfig) (*gateway, *realRegistry) {
	reg := newRealRegistry()
	g := &gateway{
		clusters: map[string][]string{"registry": {"registry-a"}, "standby": {"standby-a"}, "sector-1": {"sector-1-a"}},
		nodes: map[string]nodeStatus{
			"registry-a": {Up: true, State: "Leader"}, "standby-a": {Up: true, State: "Leader"}, "sector-1-a": {Up: true, State: "Leader"},
		},
		aircraft: map[string]position{}, owners: map[string]fsm.Owner{}, seen: map[string]time.Time{},
		inflight: map[string]bool{}, ready: map[string]readiness{},
		el: newElastic(cfg),
	}
	g.client = &http.Client{Transport: routeTransport{map[string]http.Handler{"registry-a": reg.handler()}}, Timeout: time.Second}
	g.updateReadiness(time.Now())
	return g, reg
}

func (g *gateway) post(h http.HandlerFunc, body string) (int, string) {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest("POST", "/", strings.NewReader(body)))
	return rec.Code, rec.Body.String()
}

func TestElasticLifecycleAgainstTheRealRegistry(t *testing.T) {
	cfg := testCfg()
	cfg.Sustain, cfg.Cooldown = 1, 0
	g, reg := newElasticGateway(cfg)

	// 1. A new registry is seeded with the four quadrants and the spare cluster on STANDBY.
	g.refreshSectors()
	m := reg.f.Sectors()
	if len(m.Sectors) != 5 || m.Sectors["standby"].Status != fsm.SectorStandby || m.Sectors["sector-1"].Region != whole {
		t.Fatalf("seeding: %+v", m)
	}
	if g.geoMap().version != m.Version {
		t.Fatal("gateway did not adopt the seeded map")
	}
	g.refreshSectors() // reading again must not seed twice
	if reg.f.Sectors().Version != m.Version {
		t.Fatal("a second refresh changed the map")
	}

	// 2. 45 aircraft arrive in sector-1's lower band: an x cut balances them.
	now := time.Now()
	for i := 0; i < 45; i++ {
		cs := "AC" + string(rune('A'+i/26)) + string(rune('A'+i%26))
		g.aircraft[cs] = position{Callsign: cs, X: 1 + float64(i)*2.1, Y: 110, Owner: "sector-1", Sector: "sector-1"}
		g.seen[cs] = now
		reg.move("claim", cs, "", "sector-1")
	}
	g.refreshOwners()
	g.elasticTick(time.Now())

	m = reg.f.Sectors()
	if m.Sectors["standby"].Status != fsm.SectorActive || m.Sectors["standby"].Region != right || m.Sectors["sector-1"].Region != left {
		t.Fatalf("expected an x split, got %+v", m.Sectors)
	}
	if got := g.geoSector(position{X: 75, Y: 150}); got != "standby" {
		t.Fatalf("the gateway must route by the new map immediately, got %q", got)
	}
	if g.el.splits != 1 || len(g.el.events) != 1 || g.el.events[0].Kind != actSplit {
		t.Fatalf("event log: %+v", g.el.events)
	}

	// 3. The registry now lets flights be handed to the new sector...
	if res := reg.move("begin", "ACAA", "sector-1", "standby"); res == nil {
		t.Fatal("setup")
	} else if _, bad := res.(error); bad {
		t.Fatalf("handoff into the new half must be allowed: %v", res)
	}
	reg.move("commit", "ACAA", "sector-1", "standby")

	// 4. Merge by hand: the sector drains and its airspace is attributed to the neighbour.
	if code, body := g.post(g.elasticMerge, `{"sector":"standby"}`); code != 200 {
		t.Fatalf("merge request: %d %s", code, body)
	}
	if reg.f.Sectors().Sectors["standby"].Status != fsm.SectorDraining {
		t.Fatal("not draining")
	}
	if got := g.geoSector(position{X: 75, Y: 150}); got != "sector-1" {
		t.Fatalf("draining airspace belongs to the neighbour, got %q", got)
	}
	// ...and the registry refuses new flights for it.
	if _, bad := reg.move("claim", "NEW1", "", "standby").(error); !bad {
		t.Fatal("a DRAINING sector must not accept new flights")
	}

	// 5. While the registry still lists a flight held by standby, the merge waits.
	g.refreshOwners()
	g.elasticTick(time.Now())
	g.elasticTick(time.Now())
	if st := reg.f.Sectors().Sectors["standby"].Status; st != fsm.SectorDraining {
		t.Fatalf("merged with a flight still on board: %s", st)
	}

	// 6. Hand the last flight back; now the merge completes.
	reg.move("begin", "ACAA", "standby", "sector-1")
	reg.move("commit", "ACAA", "standby", "sector-1")
	g.refreshOwners()
	g.elasticTick(time.Now())

	m = reg.f.Sectors()
	if m.Sectors["standby"].Status != fsm.SectorStandby || !m.Sectors["standby"].Region.Empty() || m.Sectors["sector-1"].Region != whole {
		t.Fatalf("after the merge: %+v", m.Sectors)
	}
	if got := g.geoSector(position{X: 75, Y: 150}); got != "sector-1" {
		t.Fatalf("got %q", got)
	}
	if g.el.merges != 1 {
		t.Fatalf("merges = %d", g.el.merges)
	}
	// the rejected attempt in step 5 is on the radar as one error, not one per tick
	errs := 0
	for _, ev := range g.el.events {
		if ev.Kind == "error" {
			errs++
		}
	}
	if errs > 1 {
		t.Fatalf("repeated rejections flood the event list: %+v", g.el.events)
	}
}

func TestManualSplitRefusals(t *testing.T) {
	g, reg := newElasticGateway(testCfg())
	if code, _ := g.post(g.elasticSplit, `{"sector":"sector-1"}`); code != http.StatusServiceUnavailable {
		t.Fatalf("before the map is read: %d", code)
	}
	g.refreshSectors()
	if code, _ := g.post(g.elasticSplit, `{"sector":"nope"}`); code != http.StatusConflict {
		t.Fatalf("unknown sector: %d", code)
	}
	if code, _ := g.post(g.elasticSplit, `{"sector":"standby"}`); code != http.StatusConflict {
		t.Fatalf("a STANDBY cluster is not a sector that can be split: %d", code)
	}
	if code, _ := g.post(g.elasticSplit, `not json`); code != http.StatusBadRequest {
		t.Fatalf("bad body: %d", code)
	}
	g.setLeader("standby", false)
	if code, body := g.post(g.elasticSplit, `{"sector":"sector-1"}`); code != http.StatusConflict || !strings.Contains(body, "STANDBY") {
		t.Fatalf("no Ready spare: %d %s", code, body)
	}
	if reg.f.Sectors().Sectors["sector-1"].Region != whole {
		t.Fatal("a refused split changed the map")
	}
	g.setLeader("standby", true)
	if code, body := g.post(g.elasticSplit, `{"sector":"sector-1","axis":"y"}`); code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	if r := reg.f.Sectors().Sectors["standby"].Region; r.MinY != 150 || r.MinX != 0 {
		t.Fatalf("an explicit y axis must be honoured: %+v", r)
	}
}

func (g *gateway) setLeader(cluster string, up bool) {
	g.mu.Lock()
	id := g.clusters[cluster][0]
	st := "Candidate"
	if up {
		st = "Leader"
	}
	g.nodes[id] = nodeStatus{Up: true, State: st}
	g.mu.Unlock()
	g.updateReadiness(time.Now())
}

func TestPictureShowsSectorsAndLoad(t *testing.T) {
	g, _ := newElasticGateway(testCfg())
	g.refreshSectors()
	g.aircraft["X1"] = position{Callsign: "X1", Owner: "sector-1"}
	g.aircraft["X2"] = position{Callsign: "X2", Owner: "sector-1"}
	pic := g.snapshot()
	var s1 *sector
	for i := range pic.Sectors {
		if pic.Sectors[i].Name == "standby" {
			t.Fatal("an idle STANDBY cluster has no airspace to draw")
		}
		if pic.Sectors[i].Name == "sector-1" {
			s1 = &pic.Sectors[i]
		}
	}
	if s1 == nil || s1.Load != 2 || s1.Status != fsm.SectorActive {
		t.Fatalf("%+v", s1)
	}
	b, _ := json.Marshal(pic.Elastic)
	if !strings.Contains(string(b), `"standby"`) || !strings.Contains(string(b), `"STANDBY"`) {
		t.Fatalf("the elastic panel must list the idle cluster: %s", b)
	}
}

func TestManualSplitNeedsLoginWhenLoginIsOn(t *testing.T) {
	g, reg := newElasticGateway(testCfg())
	g.refreshSectors()
	g.auth = newTestAuth(t)
	if code, _ := g.post(g.elasticSplit, `{"sector":"sector-1"}`); code != http.StatusUnauthorized {
		t.Fatalf("anonymous split with login on: %d", code)
	}
	if code, _ := g.post(g.elasticMerge, `{"sector":"standby"}`); code != http.StatusUnauthorized {
		t.Fatalf("anonymous merge with login on: %d", code)
	}
	if reg.f.Sectors().Sectors["sector-1"].Region != whole {
		t.Fatal("a refused request changed the map")
	}
}
