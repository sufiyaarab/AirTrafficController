package main

// Elastic sectors: the airspace resizes itself as traffic changes.
//
// The registry holds the sector map (internal/fsm/sectors.go). This file is the
// gateway's side of it:
//
//   - geoSector reads the map instead of a fixed list, so the moment the
//     registry commits a split or merge, every aircraft report is routed by the
//     new boundaries;
//   - a load monitor counts the aircraft each sector controls;
//   - a planner decides when to split a busy sector (its spare half goes to an
//     idle STANDBY cluster) and when to merge it back;
//   - an executor sends the decision to the registry as a Raft command.
//
// Moving the aircraft needs no new code. A split changes which sector an aircraft
// is in; the registry still names the old sector as its owner; accept() sees the
// mismatch on the aircraft's next report and runs the normal 3-phase handoff.
// Ownership stays one-sector-per-aircraft throughout, because that is enforced by
// the registry, not by anything here.
//
// A split:   registry "split"   -> new map -> handoffs move the aircraft that are
//                                  now in the child's half.
// A merge:   registry "drain"   -> the sector takes no new flights and its
//                                  airspace is attributed to its neighbour (see
//                                  geoMap.into) -> handoffs empty it ->
//            registry "merge"   -> the registry refuses until it lists no flight
//                                  held by, or heading to, the sector.
// If the drain stalls, "undrain" puts the sector back.
//
// Every decision is a single Raft command, so a gateway crash at any point
// leaves a map the next gateway can read and continue from.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"atc/internal/fsm"
)

// freshAge is how recently an aircraft must have reported to count as load.
const freshAge = 10 * time.Second

// ---------- configuration ----------

type elasticConfig struct {
	Enabled      bool          // ELASTIC=off disables the automatic loop (manual API still works)
	SplitAt      int           // ELASTIC_SPLIT_AT: aircraft one sector controls at or above which it is split
	MergeAt      int           // ELASTIC_MERGE_AT: a pool sector and its neighbour together at or below which they merge
	Sustain      int           // ELASTIC_SUSTAIN: consecutive one-second checks a condition must hold
	Cooldown     time.Duration // ELASTIC_COOLDOWN_S: pause after every split or merge
	DrainTimeout time.Duration // ELASTIC_DRAIN_TIMEOUT_S: give up a merge that cannot empty the sector
	MinHalf      float64       // ELASTIC_MIN_HALF_NM: never cut a sector so that a half is thinner than this
	Pool         []string      // STANDBY_CLUSTERS: spare clusters; these are the sectors the loop merges back
}

func loadElasticConfig() elasticConfig {
	c := elasticConfig{
		Enabled:      !isOff(os.Getenv("ELASTIC")),
		SplitAt:      envInt("ELASTIC_SPLIT_AT", 40),
		MergeAt:      envInt("ELASTIC_MERGE_AT", 20),
		Sustain:      envInt("ELASTIC_SUSTAIN", 3),
		Cooldown:     time.Duration(envInt("ELASTIC_COOLDOWN_S", 15)) * time.Second,
		DrainTimeout: time.Duration(envInt("ELASTIC_DRAIN_TIMEOUT_S", 30)) * time.Second,
		MinHalf:      float64(envInt("ELASTIC_MIN_HALF_NM", 25)),
		Pool:         splitList(envOr("STANDBY_CLUSTERS", "standby")),
	}
	// A split sector's two halves together carry about the load that triggered
	// the split. If merging were allowed at that load, the halves would merge
	// straight back and flap.
	if c.MergeAt > c.SplitAt/2 {
		log.Printf("elastic: ELASTIC_MERGE_AT=%d is too close to ELASTIC_SPLIT_AT=%d, using %d", c.MergeAt, c.SplitAt, c.SplitAt/2)
		c.MergeAt = c.SplitAt / 2
	}
	return c
}

func isOff(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "off", "0", "false", "no":
		return true
	}
	return false
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		log.Printf("elastic: ignoring %s=%q (want a whole number)", k, v)
		return def
	}
	return n
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ---------- the map as the gateway routes with it ----------

// geoMap is an immutable routing table built from one version of the registry's
// sector map. It is swapped in atomically, so geoSector never takes a lock.
type geoMap struct {
	version uint64
	sectors []sector          // every sector that has airspace (ACTIVE and DRAINING)
	into    map[string]string // DRAINING sector -> the neighbour that takes over its airspace
}

func buildGeo(m fsm.SectorMap) *geoMap {
	gm := &geoMap{version: m.Version, into: map[string]string{}}
	for _, name := range m.SectorNames() {
		s := m.Sectors[name]
		if s.Status == fsm.SectorStandby || s.Region.Empty() {
			continue
		}
		r := s.Region
		w, h := r.MaxX-r.MinX, r.MaxY-r.MinY
		gm.sectors = append(gm.sectors, sector{
			Name: name, X: r.MinX, Y: r.MinY, W: w, H: h,
			Box: [4]float64{r.MinX, r.MinY, w, h}, Status: s.Status, region: r,
		})
		if s.Status == fsm.SectorDraining {
			// While a sector drains, its airspace is attributed to the sector that
			// will absorb it. Aircraft there are then "in" the neighbour while the
			// registry still says the draining sector owns them, and the normal
			// handoff moves them across.
			if to, ok := m.MergeTarget(name); ok {
				gm.into[name] = to
			}
		}
	}
	return gm
}

// defaultSeeds is the sector map the registry starts with: the four quadrants.
func defaultSeeds() []fsm.Sector {
	var out []fsm.Sector
	for _, s := range defaultSectors() {
		out = append(out, fsm.Sector{
			Name: s.Name, Status: fsm.SectorActive,
			Region: fsm.Region{MinX: s.X, MinY: s.Y, MaxX: s.X + s.W, MaxY: s.Y + s.H},
		})
	}
	return out
}

// fallbackGeo routes aircraft until the registry's map has been read, and in
// tests. It is the same four quadrants the registry is seeded with, so nothing
// changes when the real map arrives.
var fallbackGeo = func() *geoMap {
	m := fsm.SectorMap{Sectors: map[string]fsm.Sector{}}
	for _, s := range defaultSeeds() {
		m.Sectors[s.Name] = s
	}
	return buildGeo(m)
}()

func (g *gateway) geoMap() *geoMap {
	if gm := g.geo.Load(); gm != nil {
		return gm
	}
	return fallbackGeo
}

// ---------- reading and seeding the registry's map ----------

// refreshSectors reloads the sector map from the registry leader. An empty map
// means the registry is new: seed it with the four quadrants plus the spare
// clusters. Called from the poll loop, next to refreshOwners.
func (g *gateway) refreshSectors() {
	leader := g.leaderOf("registry")
	if leader == "" {
		return
	}
	resp, err := g.client.Get("http://" + leader + ":8080/sectors")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return // a node built before /sectors existed
	}
	var m fsm.SectorMap
	if json.NewDecoder(resp.Body).Decode(&m) != nil {
		return
	}
	if len(m.Sectors) == 0 {
		g.seedRegistry()
		return
	}
	g.adoptMap(m)
}

func (g *gateway) seedRegistry() {
	seeds := defaultSeeds()
	if g.el != nil {
		for _, c := range g.el.cfg.Pool {
			if _, ok := g.clusters[c]; ok {
				seeds = append(seeds, fsm.Sector{Name: c, Status: fsm.SectorStandby})
			}
		}
	}
	m, err := g.sectorOp("sector_init", "", seeds)
	if err != nil {
		log.Printf("elastic: seeding the sector map: %v", err)
		return
	}
	log.Printf("elastic: seeded the registry's sector map (version %d, %d sectors)", m.Version, len(m.Sectors))
	g.adoptMap(m)
}

// adoptMap makes m the map the gateway routes by, unless it is older than the
// one already held (a stale read from a deposed registry leader).
func (g *gateway) adoptMap(m fsm.SectorMap) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.Version < g.smap.Version {
		return
	}
	g.smap = m
	if cur := g.geo.Load(); cur == nil || cur.version != m.Version {
		g.geo.Store(buildGeo(m))
	}
}

// sectorOp commits one sector-map command on the registry leader and returns
// the resulting map.
func (g *gateway) sectorOp(op, key string, payload any) (fsm.SectorMap, error) {
	var out fsm.SectorMap
	leader := g.leaderOf("registry")
	if leader == "" {
		return out, errors.New("registry has no leader")
	}
	val, _ := json.Marshal(payload)
	body, _ := json.Marshal(fsm.Command{Op: op, Key: key, Value: val})
	resp, err := g.client.Post("http://"+leader+":8080/apply", "application/json", bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	var res struct {
		Error  string         `json:"error"`
		Result *fsm.SectorMap `json:"result"`
	}
	json.NewDecoder(resp.Body).Decode(&res)
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("%s: %s", op, res.Error)
	}
	if res.Result == nil {
		return out, fmt.Errorf("%s: registry returned no map", op)
	}
	return *res.Result, nil
}

// ---------- load ----------

// loadsLocked counts the aircraft each sector controls (the registry's owner,
// so an aircraft mid-handoff still counts for its source). Must be called with
// g.mu held.
func (g *gateway) loadsLocked(now time.Time) map[string]int {
	out := map[string]int{}
	for cs, p := range g.aircraft {
		if p.Owner == "" || !g.freshLocked(cs, now) {
			continue
		}
		out[p.Owner]++
	}
	return out
}

func (g *gateway) freshLocked(callsign string, now time.Time) bool {
	seen, ok := g.seen[callsign]
	return !ok || now.Sub(seen) <= freshAge
}

// bestAxis picks the cut that splits a region's aircraft most evenly. "x"
// cuts into left and right halves, "y" into bottom and top. Cuts that would
// leave a half thinner than minHalf are not considered; "" means the region
// cannot be cut at all.
func bestAxis(r fsm.Region, pts [][2]float64, minHalf float64) string {
	w, h := r.MaxX-r.MinX, r.MaxY-r.MinY
	xOK, yOK := w/2 >= minHalf, h/2 >= minHalf
	switch {
	case !xOK && !yOK:
		return ""
	case !yOK:
		return "x"
	case !xOK:
		return "y"
	}
	midX, midY := (r.MinX+r.MaxX)/2, (r.MinY+r.MaxY)/2
	var left, below int
	for _, p := range pts {
		if p[0] < midX {
			left++
		}
		if p[1] < midY {
			below++
		}
	}
	imbX := abs(left - (len(pts) - left))
	imbY := abs(below - (len(pts) - below))
	switch {
	case imbX < imbY:
		return "x"
	case imbY < imbX:
		return "y"
	case w >= h:
		return "x"
	}
	return "y"
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// ---------- the planner ----------

const (
	actSplit   = "split"
	actDrain   = "drain"
	actMerge   = "merge"
	actUndrain = "undrain"
)

// action is one registry command the planner wants sent. The zero value means
// "do nothing".
type action struct {
	kind   string
	sector string
	child  string // split: the STANDBY cluster that takes the new half
	axis   string // split: "x" or "y"
	into   string // merge: the neighbour that absorbs the sector
	reason string
}

// view is everything the planner looks at, copied out under the gateway's lock
// so that plan itself is a plain function of its inputs.
type view struct {
	m     fsm.SectorMap
	load  map[string]int    // aircraft each sector controls
	ready map[string]bool   // cluster has exactly one working leader
	owned map[string]int    // flights the registry lists as held by, or heading to, each sector
	axis  map[string]string // best way to cut each ACTIVE sector; "" if it is too small
	now   time.Time
}

type elasticEvent struct {
	TimeMs int64  `json:"time_ms"`
	Kind   string `json:"kind"` // split, drain, merge, undrain, error
	Sector string `json:"sector"`
	Detail string `json:"detail"`
}

type elastic struct {
	cfg elasticConfig
	rc  sync.Mutex // one reconfiguration at a time: the loop and the manual API both take it

	mu        sync.Mutex // guards everything below
	over      map[string]int
	under     map[string]int
	drainSeen map[string]time.Time
	lastAct   time.Time
	events    []elasticEvent
	splits    int
	merges    int
	undrains  int
}

func newElastic(cfg elasticConfig) *elastic {
	return &elastic{cfg: cfg, over: map[string]int{}, under: map[string]int{}, drainSeen: map[string]time.Time{}}
}

func firstReadyStandby(v view) string {
	for _, name := range v.m.SectorNames() {
		if v.m.Sectors[name].Status == fsm.SectorStandby && v.ready[name] {
			return name
		}
	}
	return ""
}

// plan decides what, if anything, to do next. It keeps only counters between
// calls ("how many checks in a row has this been true"); the map itself is the
// state, so a restarted gateway picks up where the last one stopped.
func (e *elastic) plan(v view) action {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. A merge already under way runs to its end first, even with the loop
	// disabled, so a sector is never left half-merged. One at a time.
	draining := ""
	for _, name := range v.m.SectorNames() {
		if v.m.Sectors[name].Status == fsm.SectorDraining {
			draining = name
			break
		}
	}
	for name := range e.drainSeen {
		if name != draining {
			delete(e.drainSeen, name)
		}
	}
	if draining != "" {
		first, ok := e.drainSeen[draining]
		if !ok {
			first = v.now
			e.drainSeen[draining] = first
		}
		into, hasTarget := v.m.MergeTarget(draining)
		switch {
		case !hasTarget:
			return action{kind: actUndrain, sector: draining, reason: "no neighbour left to take its flights"}
		case v.owned[draining] == 0:
			return action{kind: actMerge, sector: draining, into: into, reason: "every flight has been handed off"}
		case v.now.Sub(first) > e.cfg.DrainTimeout:
			return action{kind: actUndrain, sector: draining,
				reason: fmt.Sprintf("%d flight(s) still held after %s", v.owned[draining], e.cfg.DrainTimeout)}
		}
		return action{}
	}

	if !e.cfg.Enabled || v.now.Sub(e.lastAct) < e.cfg.Cooldown {
		return action{}
	}

	// 2. Split the busiest sector that has been overloaded long enough.
	hottest, hot := "", 0
	for _, name := range v.m.SectorNames() {
		if v.m.Sectors[name].Status != fsm.SectorActive {
			delete(e.over, name)
			continue
		}
		if v.load[name] >= e.cfg.SplitAt {
			e.over[name]++
		} else {
			e.over[name] = 0
		}
		if e.over[name] >= e.cfg.Sustain && v.load[name] > hot {
			hottest, hot = name, v.load[name]
		}
	}
	if hottest != "" {
		// Wait if no spare cluster is free and Ready, or the sector is too small to cut.
		if child, axis := firstReadyStandby(v), v.axis[hottest]; child != "" && axis != "" {
			return action{kind: actSplit, sector: hottest, child: child, axis: axis,
				reason: fmt.Sprintf("%d aircraft, limit %d", hot, e.cfg.SplitAt)}
		}
	}

	// 3. Merge a spare cluster back when it and its neighbour are quiet together.
	for _, name := range e.cfg.Pool {
		if s, ok := v.m.Sectors[name]; !ok || s.Status != fsm.SectorActive {
			delete(e.under, name)
			continue
		}
		into, ok := v.m.MergeTarget(name)
		if !ok {
			delete(e.under, name)
			continue
		}
		if total := v.load[name] + v.load[into]; total <= e.cfg.MergeAt {
			e.under[name]++
		} else {
			e.under[name] = 0
		}
		if e.under[name] >= e.cfg.Sustain && v.ready[into] {
			return action{kind: actDrain, sector: name, into: into,
				reason: fmt.Sprintf("%d + %d aircraft, limit %d", v.load[name], v.load[into], e.cfg.MergeAt)}
		}
	}
	return action{}
}

// done records a successful action and restarts the counters and the cooldown.
func (e *elastic) done(now time.Time, a action) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch a.kind {
	case actSplit:
		e.splits++
		e.lastAct = now
	case actMerge:
		e.merges++
		e.lastAct = now
	case actUndrain:
		e.undrains++
		e.lastAct = now
	}
	e.over, e.under = map[string]int{}, map[string]int{}
}

// record adds an event for the radar, skipping an exact repeat of the newest
// one so that a command rejected every second does not flood the list.
func (e *elastic) record(now time.Time, kind, sector, detail string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if n := len(e.events); n > 0 {
		if last := e.events[0]; last.Kind == kind && last.Sector == sector && last.Detail == detail {
			return
		}
	}
	e.events = append([]elasticEvent{{TimeMs: now.UnixMilli(), Kind: kind, Sector: sector, Detail: detail}}, e.events...)
	if len(e.events) > 20 {
		e.events = e.events[:20]
	}
}

// ---------- the executor ----------

func (g *gateway) elasticLoop() {
	if g.el == nil {
		return
	}
	for range time.Tick(time.Second) {
		g.elasticTick(time.Now())
	}
}

func (g *gateway) elasticTick(now time.Time) {
	g.el.rc.Lock()
	defer g.el.rc.Unlock()
	v, ok := g.elasticView(now)
	if !ok {
		return
	}
	if a := g.el.plan(v); a.kind != "" {
		g.runAction(a, now)
	}
}

// runAction sends one action to the registry and, on success, adopts the map it
// returns so that routing changes at once instead of at the next poll.
func (g *gateway) runAction(a action, now time.Time) error {
	var (
		m   fsm.SectorMap
		err error
	)
	switch a.kind {
	case actSplit:
		m, err = g.sectorOp("split", a.sector, fsm.SplitReq{Child: a.child, Axis: a.axis})
	case actDrain:
		m, err = g.sectorOp("drain", a.sector, nil)
	case actMerge:
		m, err = g.sectorOp("merge", a.sector, fsm.MergeReq{Into: a.into})
	case actUndrain:
		m, err = g.sectorOp("undrain", a.sector, nil)
	default:
		return nil
	}
	if err != nil {
		// Common and harmless: a merge refused because the registry still lists a
		// flight. The planner simply asks again on the next tick.
		g.el.record(now, "error", a.sector, err.Error())
		return err
	}
	g.adoptMap(m)
	g.el.done(now, a)
	detail := a.reason
	switch a.kind {
	case actSplit:
		r := m.Sectors[a.child].Region
		detail = fmt.Sprintf("%s now covers x %.0f-%.0f, y %.0f-%.0f: %s", a.child, r.MinX, r.MaxX, r.MinY, r.MaxY, a.reason)
	case actDrain:
		detail = fmt.Sprintf("draining into %s: %s", a.into, a.reason)
	case actMerge:
		detail = fmt.Sprintf("merged into %s; cluster back on STANDBY: %s", a.into, a.reason)
	}
	g.el.record(now, a.kind, a.sector, detail)
	log.Printf("elastic: %s %s: %s", a.kind, a.sector, detail)
	return nil
}

// elasticView copies what the planner needs out of the gateway. It reports
// false until the registry's map has been read and the registry has a leader.
func (g *gateway) elasticView(now time.Time) (view, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.smap.Sectors) == 0 || !g.readyLocked("registry", now) {
		return view{}, false
	}
	v := view{
		m: g.smap, load: g.loadsLocked(now), now: now,
		ready: map[string]bool{}, owned: map[string]int{}, axis: map[string]string{},
	}
	for c := range g.clusters {
		v.ready[c] = g.readyLocked(c, now)
	}
	for _, o := range g.owners {
		v.owned[o.Sector]++
		if o.Status == fsm.Transferring {
			v.owned[o.Target]++
		}
	}
	pts := map[string][][2]float64{}
	for cs, p := range g.aircraft {
		if p.Owner != "" && g.freshLocked(cs, now) {
			pts[p.Owner] = append(pts[p.Owner], [2]float64{p.X, p.Y})
		}
	}
	minHalf := 0.0
	if g.el != nil {
		minHalf = g.el.cfg.MinHalf
	}
	for name, s := range v.m.Sectors {
		if s.Status == fsm.SectorActive {
			v.axis[name] = bestAxis(s.Region, pts[name], minHalf)
		}
	}
	return v, true
}

// ---------- manual control (demo, tests, the radar's buttons) ----------

type elasticReq struct {
	Sector string `json:"sector"`
	Axis   string `json:"axis,omitempty"`
}

// elasticSplit: POST /api/elastic/split {"sector":"sector-1","axis":"x"}
func (g *gateway) elasticSplit(w http.ResponseWriter, r *http.Request) {
	req, ok := g.elasticRequest(w, r)
	if !ok {
		return
	}
	g.el.rc.Lock()
	defer g.el.rc.Unlock()
	now := time.Now()
	v, ok := g.elasticView(now)
	if !ok {
		jsonError(w, http.StatusServiceUnavailable, "the sector map is not loaded yet, or the registry has no leader")
		return
	}
	if s, ok := v.m.Sectors[req.Sector]; !ok || s.Status != fsm.SectorActive {
		jsonError(w, http.StatusConflict, req.Sector+" is not an ACTIVE sector")
		return
	}
	child := firstReadyStandby(v)
	if child == "" {
		jsonError(w, http.StatusConflict, "no STANDBY cluster is free and Ready")
		return
	}
	axis := req.Axis
	if axis == "" {
		axis = v.axis[req.Sector] // still "" for a sector too small to cut: the registry then cuts the longer side
	}
	a := action{kind: actSplit, sector: req.Sector, child: child, axis: axis, reason: "requested by hand"}
	if err := g.runAction(a, now); err != nil {
		jsonError(w, http.StatusConflict, err.Error())
		return
	}
	writeOK(w, map[string]any{"ok": true, "split": req.Sector, "child": child})
}

// elasticMerge: POST /api/elastic/merge {"sector":"standby"}. Starts the drain;
// the loop finishes the merge once the sector is empty.
func (g *gateway) elasticMerge(w http.ResponseWriter, r *http.Request) {
	req, ok := g.elasticRequest(w, r)
	if !ok {
		return
	}
	g.el.rc.Lock()
	defer g.el.rc.Unlock()
	now := time.Now()
	v, ok := g.elasticView(now)
	if !ok {
		jsonError(w, http.StatusServiceUnavailable, "the sector map is not loaded yet, or the registry has no leader")
		return
	}
	if s, ok := v.m.Sectors[req.Sector]; !ok || s.Status != fsm.SectorActive {
		jsonError(w, http.StatusConflict, req.Sector+" is not an ACTIVE sector")
		return
	}
	into, ok := v.m.MergeTarget(req.Sector)
	if !ok {
		jsonError(w, http.StatusConflict, req.Sector+" has no neighbour that shares a full edge")
		return
	}
	if err := g.runAction(action{kind: actDrain, sector: req.Sector, into: into, reason: "requested by hand"}, now); err != nil {
		jsonError(w, http.StatusConflict, err.Error())
		return
	}
	writeOK(w, map[string]any{"ok": true, "draining": req.Sector, "into": into})
}

func (g *gateway) elasticRequest(w http.ResponseWriter, r *http.Request) (elasticReq, bool) {
	var req elasticReq
	if g.el == nil {
		jsonError(w, http.StatusServiceUnavailable, "elastic sectors are not running")
		return req, false
	}
	c, ok := g.authenticate(w, r) // answers 401 itself; with login off it lets everyone through, like /api/clearance
	if !ok {
		return req, false
	}
	if c != nil && c.Role != RoleSupervisor {
		jsonError(w, http.StatusForbidden, "only a supervisor can split or merge sectors")
		return req, false
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Sector == "" {
		jsonError(w, http.StatusBadRequest, `want {"sector":"sector-1"}`)
		return req, false
	}
	return req, true
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// ---------- what the radar shows ----------

type sectorLoad struct {
	Name   string     `json:"name"`
	Status string     `json:"status"` // ACTIVE, STANDBY or DRAINING
	Load   int        `json:"load"`
	Spare  bool       `json:"spare"` // a spare cluster: the loop splits into it and merges it back
	Box    [4]float64 `json:"box"`   // zero while STANDBY
}

type elasticPicture struct {
	Enabled  bool           `json:"enabled"`
	SplitAt  int            `json:"split_at"`
	MergeAt  int            `json:"merge_at"`
	Version  uint64         `json:"version"`
	Sectors  []sectorLoad   `json:"sectors"`
	Events   []elasticEvent `json:"events"` // newest first
	Splits   int            `json:"splits"`
	Merges   int            `json:"merges"`
	Undrains int            `json:"undrains"`
}

// sectorViewLocked is the list the radar draws: every sector that has airspace,
// with its status and how many aircraft it controls. Must be called with g.mu held.
func (g *gateway) sectorViewLocked(now time.Time) []sector {
	loads := g.loadsLocked(now)
	gm := g.geoMap()
	out := make([]sector, len(gm.sectors))
	for i, s := range gm.sectors {
		s.Load = loads[s.Name]
		out[i] = s
	}
	return out
}

// elasticPictureLocked must be called with g.mu held.
func (g *gateway) elasticPictureLocked(now time.Time) elasticPicture {
	p := elasticPicture{Version: g.smap.Version, Sectors: []sectorLoad{}, Events: []elasticEvent{}}
	loads := g.loadsLocked(now)
	spare := map[string]bool{}
	if g.el != nil {
		for _, c := range g.el.cfg.Pool {
			spare[c] = true
		}
	}
	names := make([]string, 0, len(g.smap.Sectors))
	for n := range g.smap.Sectors {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		s := g.smap.Sectors[n]
		r := s.Region
		p.Sectors = append(p.Sectors, sectorLoad{
			Name: n, Status: s.Status, Load: loads[n], Spare: spare[n],
			Box: [4]float64{r.MinX, r.MinY, r.MaxX - r.MinX, r.MaxY - r.MinY},
		})
	}
	if g.el != nil {
		g.el.mu.Lock()
		p.Enabled, p.SplitAt, p.MergeAt = g.el.cfg.Enabled, g.el.cfg.SplitAt, g.el.cfg.MergeAt
		p.Events = append(p.Events, g.el.events...)
		p.Splits, p.Merges, p.Undrains = g.el.splits, g.el.merges, g.el.undrains
		g.el.mu.Unlock()
	}
	return p
}
