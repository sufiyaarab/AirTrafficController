// Command gateway is the single entry point for the simulator and the
// dashboard. It
//   - tracks every cluster's leader by polling each node's /status,
//   - B-multicasts position reports to the 3 replicas of the sector that
//     controls each aircraft (no Raft on this path),
//   - coordinates the 3-phase handoff through the airspace registry when an
//     aircraft crosses into another sector, and finishes any handoff left
//     half-done (for example after a gateway restart), and
//   - pushes a live picture to the dashboard over a WebSocket.
//
// The sector map is static here. Once the registry holds the versioned
// sector map, geoSector should read it from the registry instead.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"atc/internal/fsm"

	"github.com/gorilla/websocket"
)

type nodeStatus struct {
	ID       string `json:"id"`
	Cluster  string `json:"cluster"`
	State    string `json:"state"`
	LeaderID string `json:"leader_id"`
	Term     string `json:"term"`
	Commit   string `json:"commit_index"`
	Up       bool   `json:"up"`
}

type position struct {
	Callsign string  `json:"callsign"`
	Seq      uint64  `json:"seq"` // per-aircraft sequence number from the simulator (FIFO)
	X        float64 `json:"x"`   // NM east of the airspace origin
	Y        float64 `json:"y"`   // NM north of the airspace origin
	Alt      float64 `json:"alt"` // feet
	Heading  float64 `json:"heading"`
	Speed    float64 `json:"speed"`  // knots
	Sector   string  `json:"sector"` // sector the aircraft is physically in
	Owner    string  `json:"owner"`  // sector that controls it, per the registry
	Status   string  `json:"status"` // ACTIVE / TRANSFERRING, per the registry
}

type sector struct {
	Name       string     `json:"name"`
	X, Y, W, H float64    `json:"-"`
	Box        [4]float64 `json:"box"` // x, y, w, h for the dashboard
}

// job is one ownership change the gateway coordinates.
type job struct {
	op       string // "claim" or "handoff"
	callsign string
	from, to string
}

type stats struct {
	Claims       int     `json:"claims"`
	Handoffs     int     `json:"handoffs"`
	Aborted      int     `json:"aborted"`
	Recovered    int     `json:"recovered"`
	LastMs       float64 `json:"last_ms"`
	AvgMs        float64 `json:"avg_ms"`
	Queued       int     `json:"queued"`
	McastBatches uint64  `json:"multicast_batches"`
	McastDropped uint64  `json:"multicast_dropped"`
	RPCs         int     `json:"rpcs"` // ownership RPCs sent (begin/accept/commit/abort/claim)
}

type gateway struct {
	clusters map[string][]string // cluster -> node IDs
	sectors  []sector
	client   *http.Client
	jobs     chan job
	senders  map[string]chan []position // node ID -> FIFO outbound queue

	mu       sync.Mutex
	nodes    map[string]nodeStatus          // node ID -> last status
	aircraft map[string]position            // callsign -> latest report
	outbox   map[string]map[string]position // controlling sector -> callsign -> report
	owners   map[string]fsm.Owner           // registry's ownership table (cached)
	inflight map[string]bool                // callsigns with a job queued or running
	st       stats
}

func main() {
	g := &gateway{
		clusters: parseClusters(os.Getenv("CLUSTERS")),
		sectors:  defaultSectors(),
		client:   &http.Client{Timeout: time.Second},
		jobs:     make(chan job, 1024),
		senders:  map[string]chan []position{},
		nodes:    map[string]nodeStatus{},
		aircraft: map[string]position{},
		outbox:   map[string]map[string]position{},
		owners:   map[string]fsm.Owner{},
		inflight: map[string]bool{},
	}
	for c, ids := range g.clusters {
		if c == "registry" {
			continue
		}
		for _, id := range ids {
			ch := make(chan []position, 8)
			g.senders[id] = ch
			go g.sendLoop(id, ch)
		}
	}
	go g.pollLoop()
	go g.flushLoop()
	for range 4 {
		go g.worker()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("POST /api/position", g.position)
	mux.HandleFunc("POST /api/positions", g.positions)
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(g.snapshot())
	})
	mux.HandleFunc("GET /ws", g.ws)
	addr := envOr("HTTP_ADDR", ":8080")
	log.Printf("gateway on %s, clusters=%v", addr, g.clusters)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// parseClusters reads "sector-1=a,b,c;sector-2=d,e,f".
func parseClusters(s string) map[string][]string {
	out := map[string][]string{}
	for _, part := range strings.Split(s, ";") {
		name, nodes, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok {
			out[name] = strings.Split(nodes, ",")
		}
	}
	return out
}

// defaultSectors splits a 200 x 200 NM airspace into four quadrants, with the
// tower owning a 15 NM circle in the middle (checked first in geoSector).
func defaultSectors() []sector {
	mk := func(n string, x, y float64) sector {
		return sector{Name: n, X: x, Y: y, W: 100, H: 100, Box: [4]float64{x, y, 100, 100}}
	}
	return []sector{mk("sector-1", 0, 100), mk("sector-2", 100, 100), mk("sector-3", 0, 0), mk("sector-4", 100, 0)}
}

func (g *gateway) geoSector(p position) string {
	if math.Hypot(p.X-100, p.Y-100) <= 15 && p.Alt < 10000 {
		return "tower"
	}
	for _, s := range g.sectors {
		if p.X >= s.X && p.X < s.X+s.W && p.Y >= s.Y && p.Y < s.Y+s.H {
			return s.Name
		}
	}
	return ""
}

// ---------- position path: B-multicast with FIFO sequence numbers ----------

func (g *gateway) position(w http.ResponseWriter, r *http.Request) {
	var p position
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil || p.Callsign == "" {
		http.Error(w, "bad position report", http.StatusBadRequest)
		return
	}
	g.accept(p)
	w.WriteHeader(http.StatusAccepted)
}

// positions takes a batch, so the simulator can send one request per tick.
func (g *gateway) positions(w http.ResponseWriter, r *http.Request) {
	var ps []position
	if err := json.NewDecoder(r.Body).Decode(&ps); err != nil {
		http.Error(w, "bad position batch", http.StatusBadRequest)
		return
	}
	for _, p := range ps {
		if p.Callsign != "" {
			g.accept(p)
		}
	}
	w.WriteHeader(http.StatusAccepted)
}

// accept records a report, queues it for the controlling sector, and starts
// a claim or handoff if the registry's owner no longer matches the airspace.
func (g *gateway) accept(p position) {
	p.Sector = g.geoSector(p)
	g.mu.Lock()
	defer g.mu.Unlock()
	o, owned := g.owners[p.Callsign]
	p.Owner, p.Status = p.Sector, ""
	if owned {
		// While TRANSFERRING the source still controls the aircraft.
		p.Owner, p.Status = o.Sector, o.Status
	}
	g.aircraft[p.Callsign] = p
	if p.Owner != "" {
		if g.outbox[p.Owner] == nil {
			g.outbox[p.Owner] = map[string]position{}
		}
		g.outbox[p.Owner][p.Callsign] = p
	}

	if p.Sector == "" || g.inflight[p.Callsign] {
		return
	}
	switch {
	case !owned:
		g.enqueue(job{op: "claim", callsign: p.Callsign, to: p.Sector})
	case o.Status == fsm.Active && o.Sector != p.Sector:
		g.enqueue(job{op: "handoff", callsign: p.Callsign, from: o.Sector, to: p.Sector})
	}
}

// enqueue must be called with g.mu held.
func (g *gateway) enqueue(j job) {
	select {
	case g.jobs <- j:
		g.inflight[j.callsign] = true
	default: // queue full: the next report will try again
	}
}

// flushLoop hands each sector's batch to the FIFO sender of every replica in
// that sector: one message per replica per tick (B-multicast to the group).
func (g *gateway) flushLoop() {
	for range time.Tick(250 * time.Millisecond) {
		g.mu.Lock()
		batches := g.outbox
		g.outbox = map[string]map[string]position{}
		g.mu.Unlock()
		for cluster, m := range batches {
			batch := make([]position, 0, len(m))
			for _, p := range m {
				batch = append(batch, p)
			}
			for _, id := range g.clusters[cluster] {
				select {
				case g.senders[id] <- batch:
				default: // replica too slow or down: B-multicast does not retry
					g.mu.Lock()
					g.st.McastDropped++
					g.mu.Unlock()
				}
			}
		}
	}
}

// sendLoop is the only sender to one replica, so batches arrive in the order
// they were queued (FIFO per channel).
func (g *gateway) sendLoop(id string, ch chan []position) {
	for batch := range ch {
		body, _ := json.Marshal(batch)
		resp, err := g.client.Post("http://"+id+":8080/track", "application/json", bytes.NewReader(body))
		g.mu.Lock()
		if err != nil {
			g.st.McastDropped++
		} else {
			resp.Body.Close()
			g.st.McastBatches++
		}
		g.mu.Unlock()
	}
}

// ---------- ownership path: 3-phase handoff through the registry ----------

func (g *gateway) worker() {
	for j := range g.jobs {
		start := time.Now()
		var err error
		if j.op == "claim" {
			_, err = g.registry("claim", j.callsign, fsm.Move{To: j.to})
		} else {
			err = g.handoff(j, true)
		}
		g.mu.Lock()
		delete(g.inflight, j.callsign)
		if err == nil {
			if j.op == "claim" {
				g.st.Claims++
			} else {
				ms := float64(time.Since(start).Microseconds()) / 1000
				g.st.AvgMs = (g.st.AvgMs*float64(g.st.Handoffs) + ms) / float64(g.st.Handoffs+1)
				g.st.Handoffs++
				g.st.LastMs = ms
			}
		}
		g.mu.Unlock()
		if err != nil {
			log.Printf("%s %s: %v", j.op, j.callsign, err)
		}
	}
}

// handoff runs the three phases. begin=false resumes a handoff that is
// already TRANSFERRING in the registry (recovery).
//
//  1. registry: ACTIVE(from) -> TRANSFERRING(from -> to)
//  2. target sector commits "accept" in its own Raft log
//  3. registry: TRANSFERRING -> ACTIVE(to)
//
// If phase 2 fails (target has no leader, or refuses), the registry aborts
// back to ACTIVE(from), so the aircraft is never left without an owner.
func (g *gateway) handoff(j job, begin bool) error {
	m := fsm.Move{From: j.from, To: j.to}
	if begin {
		if _, err := g.registry("begin", j.callsign, m); err != nil {
			return fmt.Errorf("begin: %w", err)
		}
	}
	if err := g.applyTo(j.to, "accept", j.callsign, m, nil); err != nil {
		if _, aerr := g.registry("abort", j.callsign, m); aerr != nil {
			return fmt.Errorf("accept failed (%v) and abort failed: %w", err, aerr)
		}
		g.mu.Lock()
		g.st.Aborted++
		g.mu.Unlock()
		return fmt.Errorf("aborted: %w", err)
	}
	if _, err := g.registry("commit", j.callsign, m); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// registry applies an ownership op on the registry leader and caches the result.
func (g *gateway) registry(op, callsign string, m fsm.Move) (fsm.Owner, error) {
	var o fsm.Owner
	if err := g.applyTo("registry", op, callsign, m, &o); err != nil {
		return o, err
	}
	g.mu.Lock()
	g.owners[callsign] = o
	g.mu.Unlock()
	return o, nil
}

func (g *gateway) applyTo(cluster, op, key string, m fsm.Move, out *fsm.Owner) error {
	leader := g.leaderOf(cluster)
	if leader == "" {
		return errors.New(cluster + " has no leader")
	}
	g.mu.Lock()
	g.st.RPCs++
	g.mu.Unlock()
	val, _ := json.Marshal(m)
	body, _ := json.Marshal(fsm.Command{Op: op, Key: key, Value: val})
	resp, err := g.client.Post("http://"+leader+":8080/apply", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var res struct {
		Error  string     `json:"error"`
		Result *fsm.Owner `json:"result"`
	}
	json.NewDecoder(resp.Body).Decode(&res)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s %s: %s", cluster, op, res.Error)
	}
	if out != nil && res.Result != nil {
		*out = *res.Result
	}
	return nil
}

// ---------- leader tracking and recovery ----------

func (g *gateway) pollLoop() {
	for range time.Tick(500 * time.Millisecond) {
		var wg sync.WaitGroup
		for _, ids := range g.clusters {
			for _, id := range ids {
				wg.Add(1)
				go func(id string) {
					defer wg.Done()
					st := nodeStatus{ID: id}
					if resp, err := g.client.Get("http://" + id + ":8080/status"); err == nil {
						json.NewDecoder(resp.Body).Decode(&st)
						resp.Body.Close()
						st.Up = true
					}
					g.mu.Lock()
					g.nodes[id] = st
					g.mu.Unlock()
				}(id)
			}
		}
		wg.Wait()
		g.refreshOwners()
	}
}

// refreshOwners reloads the ownership table from the registry leader and
// resumes any handoff stuck in TRANSFERRING that nobody is working on.
func (g *gateway) refreshOwners() {
	leader := g.leaderOf("registry")
	if leader == "" {
		return
	}
	resp, err := g.client.Get("http://" + leader + ":8080/owners")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var owners map[string]fsm.Owner
	if json.NewDecoder(resp.Body).Decode(&owners) != nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.owners = owners
	for cs, o := range owners {
		if o.Status == fsm.Transferring && !g.inflight[cs] {
			g.inflight[cs] = true
			go func(j job) {
				err := g.handoff(j, false)
				g.mu.Lock()
				delete(g.inflight, j.callsign)
				if err == nil {
					g.st.Recovered++
				}
				g.mu.Unlock()
			}(job{op: "handoff", callsign: cs, from: o.Sector, to: o.Target})
		}
	}
}

// leaderOf returns the leader ID only if that node itself reports Leader.
func (g *gateway) leaderOf(cluster string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, id := range g.clusters[cluster] {
		if st := g.nodes[id]; st.Up && st.State == "Leader" {
			return id
		}
	}
	return ""
}

// ---------- dashboard feed ----------

type picture struct {
	Time     int64                   `json:"time"`
	Sectors  []sector                `json:"sectors"`
	Clusters map[string][]nodeStatus `json:"clusters"`
	Aircraft []position              `json:"aircraft"`
	Handoff  stats                   `json:"handoff"`
}

func (g *gateway) snapshot() picture {
	g.mu.Lock()
	defer g.mu.Unlock()
	pic := picture{Time: time.Now().UnixMilli(), Sectors: g.sectors, Clusters: map[string][]nodeStatus{}, Handoff: g.st}
	pic.Handoff.Queued = len(g.jobs)
	for c, ids := range g.clusters {
		for _, id := range ids {
			pic.Clusters[c] = append(pic.Clusters[c], g.nodes[id])
		}
	}
	for _, p := range g.aircraft {
		pic.Aircraft = append(pic.Aircraft, p)
	}
	return pic
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (g *gateway) ws(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	for range time.Tick(250 * time.Millisecond) {
		if err := conn.WriteJSON(g.snapshot()); err != nil {
			return
		}
	}
}
