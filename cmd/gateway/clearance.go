package main

// Clearances (docs/collins_design.md, sections 3 and 4).
//
// A clearance is an instruction to one aircraft ("DESCEND FL250"). It is
// committed to the Raft log of the sector that owns the aircraft, so the log
// index is its place in that sector's total order.
//
// Path of one clearance:
//
//  1. Take the aircraft's lock (g.inflight, the same lock the handoff worker
//     uses), so a handoff and a clearance for one aircraft never overlap.
//  2. Read the owner from the registry leader, not from the gateway's cache.
//  3. Refuse if the aircraft is unclaimed or TRANSFERRING, or if ground
//     receivers marked it SUSPECT (verify.go).
//  4. If the owner sector is not Ready, hold the clearance in that sector's
//     buffer (FIFO, 10 s TTL) and answer 202 "pending".
//  5. Otherwise commit it on the owner's leader as
//     set clearance/<callsign>/<id>.
//
// The buffer is flushed every 250 ms. A buffered clearance goes through the
// whole path again when it is flushed, so if the aircraft was handed to
// another sector while it waited, it is rejected rather than committed in the
// wrong sector.
//
// Retrying is safe: the Raft key contains the clearance ID, so if a commit
// timed out but did go through, sending it again writes the same key with the
// same value.
//
// This assumes a single gateway: step 1 only excludes handoffs that this
// gateway coordinates.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"atc/internal/fsm"
)

const (
	clearanceTTL      = 10 * time.Second // how long a buffered clearance may wait
	clearanceQueueCap = 64               // max buffered clearances per cluster
	clearanceRecent   = 30               // how many to keep for the dashboard
	maxInstructionLen = 64
)

// Clearance statuses.
const (
	ClrPending   = "pending"
	ClrCommitted = "committed"
	ClrRejected  = "rejected"
	ClrExpired   = "expired"
)

type clearance struct {
	ID          string `json:"id"`
	Callsign    string `json:"callsign"`
	Instruction string `json:"instruction"`
	IssuedBy    string `json:"issued_by"`
	Sector      string `json:"sector,omitempty"` // owner sector it was (or will be) committed in
	Status      string `json:"status"`
	Reason      string `json:"reason,omitempty"`     // why pending / rejected
	WaitingOn   string `json:"waiting_on,omitempty"` // cluster whose buffer holds it
	Index       uint64 `json:"index,omitempty"`      // Raft log index once committed
	CreatedMs   int64  `json:"created_ms"`
	UpdatedMs   int64  `json:"updated_ms"`
}

type clearanceStats struct {
	Committed int `json:"committed"`
	Rejected  int `json:"rejected"`
	Expired   int `json:"expired"`
	Buffered  int `json:"buffered"` // how many were ever buffered (not the current count)
}

// clearanceState is guarded by g.mu.
type clearanceState struct {
	queues map[string][]*clearance // cluster -> FIFO of pending clearances
	recent []*clearance            // newest last, at most clearanceRecent
	st     clearanceStats
}

// push adds c to the back of cluster's queue.
func (s *clearanceState) push(cluster string, c *clearance) error {
	if s.queues == nil {
		s.queues = map[string][]*clearance{}
	}
	if len(s.queues[cluster]) >= clearanceQueueCap {
		return fmt.Errorf("%s buffer full (%d)", cluster, clearanceQueueCap)
	}
	s.queues[cluster] = append(s.queues[cluster], c)
	return nil
}

// expire removes every queued clearance older than the TTL and marks it expired.
func (s *clearanceState) expire(now time.Time) []*clearance {
	var out []*clearance
	for cluster, q := range s.queues {
		kept := q[:0]
		for _, c := range q {
			if now.Sub(time.UnixMilli(c.CreatedMs)) > clearanceTTL {
				c.Status, c.Reason, c.WaitingOn, c.UpdatedMs = ClrExpired, "waited too long for "+cluster, "", now.UnixMilli()
				s.st.Expired++
				out = append(out, c)
			} else {
				kept = append(kept, c)
			}
		}
		if len(kept) == 0 {
			delete(s.queues, cluster)
		} else {
			s.queues[cluster] = kept
		}
	}
	return out
}

// remember keeps c in the dashboard's recent list (by pointer, so later
// status changes show up).
func (s *clearanceState) remember(c *clearance) {
	s.recent = append(s.recent, c)
	if len(s.recent) > clearanceRecent {
		s.recent = s.recent[len(s.recent)-clearanceRecent:]
	}
}

func (s *clearanceState) queued() map[string]int {
	out := map[string]int{}
	for c, q := range s.queues {
		out[c] = len(q)
	}
	return out
}

// ---------- outcome of one attempt ----------

type outcomeKind int

const (
	outCommitted outcomeKind = iota
	outRejected              // final: will never succeed as sent
	outBusy                  // aircraft locked by a handoff right now
	outNotReady              // a cluster it needs has no usable leader: buffer it
)

type outcome struct {
	kind    outcomeKind
	cluster string // for outNotReady: which cluster to wait on
	reason  string
	code    int    // HTTP status for outRejected
	sector  string // owner sector, once known
	index   uint64 // Raft log index, for outCommitted
}

// tryClearance makes one attempt to commit c. It does not hold g.mu while
// talking to the network, and it does not modify c (a buffered c is also read
// by the dashboard snapshot); the caller applies the outcome under g.mu.
func (g *gateway) tryClearance(c *clearance) outcome {
	g.mu.Lock()
	if g.inflight[c.Callsign] {
		g.mu.Unlock()
		return outcome{kind: outBusy, reason: "handoff in progress"}
	}
	if g.ver.state(c.Callsign) == VerSuspect {
		// Checked under the same lock as the commit path, so a buffered
		// clearance is also refused if its aircraft turned SUSPECT while waiting.
		g.mu.Unlock()
		return outcome{kind: outRejected, code: http.StatusConflict, reason: c.Callsign + " failed position verification (SUSPECT)"}
	}
	g.inflight[c.Callsign] = true
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		delete(g.inflight, c.Callsign)
		g.mu.Unlock()
	}()

	o, owned, err := g.ownerFromRegistry(c.Callsign)
	if err != nil {
		return outcome{kind: outNotReady, cluster: "registry", reason: "registry: " + err.Error()}
	}
	switch {
	case !owned:
		return outcome{kind: outRejected, code: http.StatusNotFound, reason: c.Callsign + " is not claimed by any sector"}
	case o.Status == fsm.Transferring:
		return outcome{kind: outBusy, reason: fmt.Sprintf("%s is TRANSFERRING %s -> %s, clearances frozen", c.Callsign, o.Sector, o.Target)}
	case c.Sector != "" && c.Sector != o.Sector:
		// It waited in the buffer and was handed off meanwhile.
		return outcome{kind: outRejected, code: http.StatusConflict,
			reason: fmt.Sprintf("ownership changed while pending: %s -> %s", c.Sector, o.Sector)}
	}
	sector := o.Sector

	g.mu.Lock()
	ready := g.readyLocked(o.Sector, time.Now())
	g.mu.Unlock()
	if !ready {
		return outcome{kind: outNotReady, cluster: o.Sector, reason: o.Sector + " not ready", sector: sector}
	}

	val, _ := json.Marshal(map[string]any{
		"id": c.ID, "callsign": c.Callsign, "instruction": c.Instruction,
		"issued_by": c.IssuedBy, "sector": sector, "issued_at_ms": c.CreatedMs,
	})
	idx, err := g.applyCommand(o.Sector, fsm.Command{Op: "set", Key: "clearance/" + c.Callsign + "/" + c.ID, Value: val})
	var rej *rejectedError
	switch {
	case errors.As(err, &rej):
		return outcome{kind: outRejected, code: http.StatusBadRequest, reason: rej.msg, sector: sector}
	case err != nil:
		// Leader moved or died between the Ready check and the write. Retrying
		// is safe because the key is idempotent.
		return outcome{kind: outNotReady, cluster: o.Sector, reason: o.Sector + ": " + err.Error(), sector: sector}
	}
	return outcome{kind: outCommitted, sector: sector, index: idx}
}

// ownerFromRegistry reads the ownership record from the registry leader.
// The gateway's cached copy can be up to one poll (500 ms) old, which is too
// stale to authorize a write.
func (g *gateway) ownerFromRegistry(callsign string) (fsm.Owner, bool, error) {
	leader := g.leaderOf("registry")
	if leader == "" {
		return fsm.Owner{}, false, errors.New("no leader")
	}
	resp, err := g.client.Get("http://" + leader + ":8080/owners")
	if err != nil {
		return fsm.Owner{}, false, err
	}
	defer resp.Body.Close()
	var owners map[string]fsm.Owner
	if err := json.NewDecoder(resp.Body).Decode(&owners); err != nil {
		return fsm.Owner{}, false, err
	}
	o, ok := owners[callsign]
	return o, ok, nil
}

// rejectedError means the node committed the command but the state machine
// refused it, so retrying will not help.
type rejectedError struct{ msg string }

func (e *rejectedError) Error() string { return e.msg }

// applyCommand commits any command on cluster's leader and returns its log index.
func (g *gateway) applyCommand(cluster string, cmd fsm.Command) (uint64, error) {
	leader := g.leaderOf(cluster)
	if leader == "" {
		return 0, errors.New("no leader")
	}
	g.mu.Lock()
	g.st.RPCs++
	g.mu.Unlock()
	body, _ := json.Marshal(cmd)
	resp, err := g.client.Post("http://"+leader+":8080/apply", "application/json", strings.NewReader(string(body)))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var res struct {
		Error string `json:"error"`
		Index uint64 `json:"index"`
	}
	json.NewDecoder(resp.Body).Decode(&res)
	switch resp.StatusCode {
	case http.StatusOK:
		return res.Index, nil
	case http.StatusBadRequest:
		return 0, &rejectedError{res.Error}
	default: // 421 not leader, 503 commit failed or timed out
		return 0, fmt.Errorf("%d %s", resp.StatusCode, res.Error)
	}
}

// ---------- HTTP ----------

type clearanceRequest struct {
	Callsign    string `json:"callsign"`
	Instruction string `json:"instruction"`
	IssuedBy    string `json:"issued_by"` // until login exists; then taken from the token
}

// clearanceHandler: POST /api/clearance
//
//	200 committed   202 pending   400 bad input   404 unclaimed
//	409 busy or ownership changed   503 buffer full
func (g *gateway) clearanceHandler(w http.ResponseWriter, r *http.Request) {
	var req clearanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad JSON: "+err.Error())
		return
	}
	c, err := newClearance(req, time.Now())
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	out := g.tryClearance(c)
	now := time.Now().UnixMilli()

	g.mu.Lock()
	defer g.mu.Unlock()
	c.UpdatedMs = now
	if out.sector != "" {
		c.Sector = out.sector
	}
	code := http.StatusOK
	switch out.kind {
	case outCommitted:
		c.Status, c.Index = ClrCommitted, out.index
		g.clr.st.Committed++
	case outRejected:
		c.Status, c.Reason, code = ClrRejected, out.reason, out.code
		g.clr.st.Rejected++
	case outBusy:
		// A handoff finishes in milliseconds; the controller retries.
		c.Status, c.Reason, code = ClrRejected, out.reason+", try again", http.StatusConflict
		g.clr.st.Rejected++
	case outNotReady:
		if err := g.clr.push(out.cluster, c); err != nil {
			c.Status, c.Reason, code = ClrRejected, err.Error(), http.StatusServiceUnavailable
			g.clr.st.Rejected++
		} else {
			c.Status, c.Reason, c.WaitingOn, code = ClrPending, out.reason, out.cluster, http.StatusAccepted
			g.clr.st.Buffered++
		}
	}
	g.clr.remember(c)
	log.Printf("clearance %s %s %q: %s %s", c.ID, c.Callsign, c.Instruction, c.Status, c.Reason)
	writeJSONStatus(w, code, *c)
}

func newClearance(req clearanceRequest, now time.Time) (*clearance, error) {
	cs := strings.ToUpper(strings.TrimSpace(req.Callsign))
	ins := strings.ToUpper(strings.Join(strings.Fields(req.Instruction), " "))
	by := strings.TrimSpace(req.IssuedBy)
	switch {
	case cs == "":
		return nil, errors.New("callsign is required")
	case ins == "":
		return nil, errors.New("instruction is required")
	case len(ins) > maxInstructionLen:
		return nil, fmt.Errorf("instruction longer than %d characters", maxInstructionLen)
	}
	if by == "" {
		by = "anonymous"
	}
	return &clearance{
		ID: newID(), Callsign: cs, Instruction: ins, IssuedBy: by,
		Status: ClrPending, CreatedMs: now.UnixMilli(), UpdatedMs: now.UnixMilli(),
	}, nil
}

func newID() string {
	b := make([]byte, 6)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSONStatus(w, code, map[string]string{"error": msg})
}

func writeJSONStatus(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// ---------- buffer flush ----------

// clearanceLoop expires old buffered clearances and retries the rest, in
// order, for every cluster that is Ready again.
func (g *gateway) clearanceLoop() {
	for range time.Tick(250 * time.Millisecond) {
		now := time.Now()
		g.mu.Lock()
		for _, c := range g.clr.expire(now) {
			log.Printf("clearance %s %s: expired", c.ID, c.Callsign)
		}
		var ready []string
		for cluster := range g.clr.queues {
			if g.readyLocked(cluster, now) {
				ready = append(ready, cluster)
			}
		}
		g.mu.Unlock()
		for _, cluster := range ready {
			g.flushQueue(cluster)
		}
	}
}

// flushQueue retries cluster's buffered clearances front to back. It stops
// at the first one that has to keep waiting, so they commit in the order
// they were sent.
func (g *gateway) flushQueue(cluster string) {
	for {
		g.mu.Lock()
		q := g.clr.queues[cluster]
		if len(q) == 0 {
			delete(g.clr.queues, cluster)
			g.mu.Unlock()
			return
		}
		c := q[0]
		g.mu.Unlock()

		out := g.tryClearance(c)

		g.mu.Lock()
		if out.kind == outBusy || (out.kind == outNotReady && out.cluster == cluster) {
			c.Reason = out.reason // still waiting here; try again next tick
			if out.sector != "" {
				c.Sector = out.sector
			}
			g.mu.Unlock()
			return
		}
		g.clr.queues[cluster] = g.clr.queues[cluster][1:]
		c.UpdatedMs = time.Now().UnixMilli()
		if out.sector != "" {
			c.Sector = out.sector
		}
		switch out.kind {
		case outCommitted:
			c.Status, c.Reason, c.WaitingOn, c.Index = ClrCommitted, "", "", out.index
			g.clr.st.Committed++
		case outRejected:
			c.Status, c.Reason, c.WaitingOn = ClrRejected, out.reason, ""
			g.clr.st.Rejected++
		case outNotReady: // now blocked on a different cluster: move it there
			if err := g.clr.push(out.cluster, c); err != nil {
				c.Status, c.Reason, c.WaitingOn = ClrRejected, err.Error(), ""
				g.clr.st.Rejected++
			} else {
				c.Reason, c.WaitingOn = out.reason, out.cluster
			}
		}
		log.Printf("clearance %s %s: %s %s (flushed from %s buffer)", c.ID, c.Callsign, c.Status, c.Reason, cluster)
		g.mu.Unlock()
	}
}

// clearanceSnapshotLocked copies recent clearances (newest first) for the
// dashboard. Must be called with g.mu held.
func (g *gateway) clearanceSnapshotLocked() []clearance {
	out := make([]clearance, 0, len(g.clr.recent))
	for i := len(g.clr.recent) - 1; i >= 0; i-- {
		out = append(out, *g.clr.recent[i])
	}
	return out
}
