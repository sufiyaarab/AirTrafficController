// Command maekawa is the baseline for the handoff comparison: Maekawa's
// voting algorithm for mutual exclusion, with the INQUIRE / FAILED / YIELD
// messages that make it deadlock free (Sanders' fix, as in the course notes).
//
// Each process stands for one control unit (4 sectors + tower = 5). The
// critical section stands for "performing a handoff". Voting sets are rows +
// columns of a grid, so |S_i| = K is about 2*sqrt(N) - 1 and any two sets
// share at least one member.
//
// Like the textbook algorithm it assumes no process fails: a crashed voter
// never answers, so every process whose voting set contains it blocks forever.
// That is the behaviour the comparison is meant to measure.
package main

import (
	"bytes"
	"encoding/json"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type req struct {
	TS int64 `json:"ts"`
	ID int   `json:"id"`
}

// before reports whether a has priority over b (Lamport timestamp, then ID).
func (a req) before(b req) bool { return a.TS < b.TS || (a.TS == b.TS && a.ID < b.ID) }

type msg struct {
	Type string `json:"type"` // REQUEST REPLY RELEASE FAILED INQUIRE YIELD
	From int    `json:"from"`
	TS   int64  `json:"ts"`  // sender's Lamport clock
	Req  req    `json:"req"` // the request this message is about
}

type queued struct {
	r      req
	failed bool // FAILED already sent for this request
}

type proc struct {
	id    int
	peers []string // host:port, index = process ID
	set   []int    // my voting set S_i
	out   map[int]chan msg
	hold  time.Duration
	think time.Duration

	mu    sync.Mutex
	clock int64

	// requester side
	requesting bool
	inCS       bool
	myReq      req
	grants     map[int]bool
	gotFailed  bool
	deferred   map[int]bool // INQUIREs waiting for a FAILED before we yield
	entered    chan struct{}
	waitStart  time.Time

	// voter side
	locked      *req
	queue       []*queued
	inquireSent bool

	// measurements
	sent      map[string]int
	entries   int
	waitTotal time.Duration
	waitMax   time.Duration
	intervals [][2]int64 // [enter, exit] in unix ms, last 500
}

func main() {
	id, _ := strconv.Atoi(os.Getenv("ME_ID"))
	peers := strings.Split(os.Getenv("PEERS"), ",")
	p := &proc{
		id:    id,
		peers: peers,
		set:   gridSet(id, len(peers)),
		out:   map[int]chan msg{},
		hold:  ms(os.Getenv("HOLD_MS"), 50),
		think: ms(os.Getenv("THINK_MS"), 500),
		sent:  map[string]int{},
	}
	for _, j := range p.set {
		ch := make(chan msg, 256)
		p.out[j] = ch
		go p.sendLoop(j, ch)
	}
	http.HandleFunc("POST /msg", p.receive)
	http.HandleFunc("GET /stats", p.stats)
	http.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	go p.workload()
	log.Printf("maekawa %d voting set %v", id, p.set)
	log.Fatal(http.ListenAndServe(env("LISTEN", ":8080"), nil))
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func ms(s string, def int) time.Duration {
	if v, err := strconv.Atoi(s); err == nil {
		return time.Duration(v) * time.Millisecond
	}
	return time.Duration(def) * time.Millisecond
}

// gridSet places processes row by row on a ceil(sqrt(N))-wide grid; S_i is
// everyone in i's row or column. With a partly empty last row, any two sets
// still intersect because the first row is full.
func gridSet(i, n int) []int {
	cols := int(math.Ceil(math.Sqrt(float64(n))))
	var s []int
	for j := range n {
		if j/cols == i/cols || j%cols == i%cols {
			s = append(s, j)
		}
	}
	return s
}

// ---------- messaging: one FIFO sender per destination ----------

func (p *proc) send(to int, typ string, r req) {
	p.clock++
	p.sent[typ]++
	p.out[to] <- msg{Type: typ, From: p.id, TS: p.clock, Req: r}
}

func (p *proc) sendLoop(to int, ch chan msg) {
	client := &http.Client{Timeout: 2 * time.Second}
	for m := range ch {
		body, _ := json.Marshal(m)
		// Retry until delivered: channels are reliable in the model. A crashed
		// peer therefore just never answers, which is what blocks Maekawa.
		for {
			resp, err := client.Post("http://"+p.peers[to]+"/msg", "application/json", bytes.NewReader(body))
			if err == nil {
				resp.Body.Close()
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func (p *proc) receive(w http.ResponseWriter, r *http.Request) {
	var m msg
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	p.mu.Lock()
	p.clock = max(p.clock, m.TS) + 1
	switch m.Type {
	case "REQUEST":
		p.onRequest(m.Req)
	case "RELEASE":
		p.onRelease(m.Req)
	case "YIELD":
		p.onYield(m.Req)
	case "REPLY":
		p.onReply(m.From, m.Req)
	case "FAILED":
		p.onFailed(m.Req)
	case "INQUIRE":
		p.onInquire(m.From, m.Req)
	}
	p.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// ---------- voter side ----------

func (p *proc) onRequest(r req) {
	if p.locked == nil {
		p.locked = &r
		p.send(r.ID, "REPLY", r)
		return
	}
	lower := p.locked.before(r)
	for _, q := range p.queue {
		if q.r.before(r) {
			lower = true
		}
	}
	nq := &queued{r: r}
	p.queue = append(p.queue, nq)
	if lower {
		nq.failed = true
		p.send(r.ID, "FAILED", r)
		return
	}
	// r outranks the holder and everyone waiting: ask the holder to give the
	// vote back, and tell the waiting ones they are no longer first.
	if !p.inquireSent {
		p.inquireSent = true
		p.send(p.locked.ID, "INQUIRE", *p.locked)
	}
	for _, q := range p.queue {
		if q != nq && !q.failed {
			q.failed = true
			p.send(q.r.ID, "FAILED", q.r)
		}
	}
}

func (p *proc) onRelease(r req) {
	if p.locked == nil || *p.locked != r {
		return
	}
	p.locked, p.inquireSent = nil, false
	p.grantNext()
}

func (p *proc) onYield(r req) {
	if p.locked == nil || *p.locked != r {
		return
	}
	p.queue = append(p.queue, &queued{r: r, failed: true})
	p.locked, p.inquireSent = nil, false
	p.grantNext()
}

func (p *proc) grantNext() {
	if len(p.queue) == 0 {
		return
	}
	sort.Slice(p.queue, func(a, b int) bool { return p.queue[a].r.before(p.queue[b].r) })
	next := p.queue[0].r
	p.queue = p.queue[1:]
	p.locked = &next
	p.send(next.ID, "REPLY", next)
}

// ---------- requester side ----------

func (p *proc) current(r req) bool { return p.requesting && r == p.myReq }

func (p *proc) onReply(from int, r req) {
	if !p.current(r) {
		return
	}
	p.grants[from] = true
	if len(p.grants) == len(p.set) && !p.inCS {
		p.inCS = true
		p.deferred = map[int]bool{}
		close(p.entered)
	}
}

func (p *proc) onFailed(r req) {
	if !p.current(r) || p.inCS {
		return
	}
	p.gotFailed = true
	for j := range p.deferred {
		p.yield(j)
	}
	p.deferred = map[int]bool{}
}

func (p *proc) onInquire(from int, r req) {
	if !p.current(r) || p.inCS || !p.grants[from] {
		return // in the CS we keep the vote; RELEASE will free it
	}
	if p.gotFailed {
		p.yield(from)
	} else {
		p.deferred[from] = true
	}
}

func (p *proc) yield(j int) {
	if p.grants[j] {
		delete(p.grants, j)
		p.send(j, "YIELD", p.myReq)
	}
}

// ---------- workload: request, hold, release, think ----------

func (p *proc) workload() {
	time.Sleep(3 * time.Second) // let peers start
	for {
		time.Sleep(time.Duration(rand.ExpFloat64() * float64(p.think)))

		p.mu.Lock()
		p.clock++
		p.myReq = req{TS: p.clock, ID: p.id}
		p.requesting, p.inCS, p.gotFailed = true, false, false
		p.grants, p.deferred = map[int]bool{}, map[int]bool{}
		p.entered = make(chan struct{})
		p.waitStart = time.Now()
		entered := p.entered
		for _, j := range p.set {
			p.send(j, "REQUEST", p.myReq)
		}
		p.mu.Unlock()

		<-entered // no timeout: a crashed voter blocks this forever
		enter := time.Now()
		time.Sleep(p.hold)
		exit := time.Now()

		p.mu.Lock()
		wait := enter.Sub(p.waitStart)
		p.entries++
		p.waitTotal += wait
		p.waitMax = max(p.waitMax, wait)
		p.intervals = append(p.intervals, [2]int64{enter.UnixMilli(), exit.UnixMilli()})
		if len(p.intervals) > 500 {
			p.intervals = p.intervals[1:]
		}
		p.requesting, p.inCS = false, false
		for _, j := range p.set {
			p.send(j, "RELEASE", p.myReq)
		}
		p.mu.Unlock()
	}
}

func (p *proc) stats(w http.ResponseWriter, _ *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	avg := 0.0
	if p.entries > 0 {
		avg = float64(p.waitTotal.Microseconds()) / 1000 / float64(p.entries)
	}
	blockedMs := int64(0)
	if p.requesting && !p.inCS {
		blockedMs = time.Since(p.waitStart).Milliseconds()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id": p.id, "voting_set": p.set, "entries": p.entries, "sent": p.sent,
		"wait_avg_ms": avg, "wait_max_ms": p.waitMax.Milliseconds(),
		"waiting_for_ms": blockedMs, "intervals": p.intervals,
	})
}
