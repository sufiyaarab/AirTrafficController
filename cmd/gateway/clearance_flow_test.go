package main

// End-to-end tests of the clearance path against fake nodes. The gateway's
// HTTP client is given a transport that routes "http://<node>:8080/..." to
// in-memory handlers, so no network or Docker is needed.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"atc/internal/fsm"
)

type fakeCluster struct {
	mu      sync.Mutex
	owners  map[string]fsm.Owner // registry only
	applied []fsm.Command        // sectors only
	down    bool                 // leader unreachable
}

type routeTransport struct{ nodes map[string]http.Handler }

func (t routeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	h, ok := t.nodes[r.URL.Hostname()]
	if !ok {
		return nil, &net404{r.URL.Hostname()}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec.Result(), nil
}

type net404 struct{ host string }

func (e *net404) Error() string { return "dial tcp: lookup " + e.host + ": no such host" }

func (f *fakeCluster) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /owners", func(w http.ResponseWriter, _ *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		json.NewEncoder(w).Encode(f.owners)
	})
	mux.HandleFunc("POST /apply", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.down {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": "leadership lost"})
			return
		}
		var c fsm.Command
		json.NewDecoder(r.Body).Decode(&c)
		f.applied = append(f.applied, c)
		json.NewEncoder(w).Encode(map[string]any{"index": 100 + len(f.applied)})
	})
	return mux
}

type harness struct {
	g        *gateway
	registry *fakeCluster
	sectors  map[string]*fakeCluster
}

// newHarness: a registry and two sectors, one leader each, all Ready.
func newHarness(t *testing.T) *harness {
	h := &harness{
		registry: &fakeCluster{owners: map[string]fsm.Owner{}},
		sectors:  map[string]*fakeCluster{"sector-1": {}, "sector-2": {}},
	}
	nodes := map[string]http.Handler{"registry-a": h.registry.handler()}
	h.g = &gateway{
		clusters: map[string][]string{"registry": {"registry-a"}},
		nodes:    map[string]nodeStatus{"registry-a": {Up: true, State: "Leader"}},
		inflight: map[string]bool{},
		ready:    map[string]readiness{},
	}
	for name, f := range h.sectors {
		id := name + "-a"
		nodes[id] = f.handler()
		h.g.clusters[name] = []string{id}
		h.g.nodes[id] = nodeStatus{Up: true, State: "Leader"}
	}
	h.g.client = &http.Client{Transport: routeTransport{nodes}, Timeout: time.Second}
	h.g.updateReadiness(time.Now())
	return h
}

func (h *harness) own(callsign, sector, status string) {
	h.registry.mu.Lock()
	h.registry.owners[callsign] = fsm.Owner{Callsign: callsign, Sector: sector, Status: status}
	h.registry.mu.Unlock()
}

// setLeader marks cluster's only node up/leader or not, and recomputes Ready.
func (h *harness) setLeader(cluster string, up bool) {
	h.g.mu.Lock()
	id := h.g.clusters[cluster][0]
	if up {
		h.g.nodes[id] = nodeStatus{Up: true, State: "Leader"}
	} else {
		h.g.nodes[id] = nodeStatus{Up: true, State: "Candidate"}
	}
	h.g.mu.Unlock()
	h.g.updateReadiness(time.Now())
}

func (h *harness) post(t *testing.T, callsign, instruction string) (int, clearance) {
	t.Helper()
	body := `{"callsign":"` + callsign + `","instruction":"` + instruction + `","issued_by":"test"}`
	rec := httptest.NewRecorder()
	h.g.clearanceHandler(rec, httptest.NewRequest("POST", "/api/clearance", strings.NewReader(body)))
	var c clearance
	json.NewDecoder(rec.Body).Decode(&c)
	return rec.Code, c
}

func TestClearanceCommitsToOwnerSector(t *testing.T) {
	h := newHarness(t)
	h.own("ATC001", "sector-2", fsm.Active)

	code, c := h.post(t, "ATC001", "descend fl250")
	if code != http.StatusOK || c.Status != ClrCommitted || c.Sector != "sector-2" || c.Index == 0 {
		t.Fatalf("got %d %+v", code, c)
	}
	if n := len(h.sectors["sector-2"].applied); n != 1 {
		t.Fatalf("sector-2 got %d commands", n)
	}
	if n := len(h.sectors["sector-1"].applied); n != 0 {
		t.Fatal("clearance must only go to the owner sector")
	}
	cmd := h.sectors["sector-2"].applied[0]
	if cmd.Op != "set" || cmd.Key != "clearance/ATC001/"+c.ID {
		t.Fatalf("wrong command %+v", cmd)
	}
}

func TestClearanceRejections(t *testing.T) {
	h := newHarness(t)
	h.own("ATC002", "sector-1", fsm.Transferring)

	if code, c := h.post(t, "ATC999", "CLIMB FL300"); code != http.StatusNotFound || c.Status != ClrRejected {
		t.Errorf("unclaimed: got %d %+v", code, c)
	}
	if code, c := h.post(t, "ATC002", "CLIMB FL300"); code != http.StatusConflict || !strings.Contains(c.Reason, "TRANSFERRING") {
		t.Errorf("transferring: got %d %+v", code, c)
	}

	h.own("ATC003", "sector-1", fsm.Active)
	h.g.mu.Lock()
	h.g.inflight["ATC003"] = true // a handoff is running for it
	h.g.mu.Unlock()
	if code, _ := h.post(t, "ATC003", "CLIMB FL300"); code != http.StatusConflict {
		t.Errorf("locked by handoff: got %d", code)
	}
	if n := len(h.sectors["sector-1"].applied); n != 0 {
		t.Fatalf("nothing should have been committed, got %d", n)
	}
}

func TestClearanceBufferedUntilSectorReady(t *testing.T) {
	h := newHarness(t)
	h.own("ATC004", "sector-2", fsm.Active)
	h.own("ATC005", "sector-2", fsm.Active)
	h.setLeader("sector-2", false)

	code1, c1 := h.post(t, "ATC004", "HEADING 090")
	code2, _ := h.post(t, "ATC005", "HEADING 180")
	if code1 != http.StatusAccepted || code2 != http.StatusAccepted || c1.WaitingOn != "sector-2" {
		t.Fatalf("want both pending on sector-2, got %d %d %+v", code1, code2, c1)
	}
	h.g.flushQueue("sector-2") // still not Ready: must stay queued
	if q := h.g.clr.queued()["sector-2"]; q != 2 {
		t.Fatalf("want 2 queued, got %d", q)
	}

	h.setLeader("sector-2", true)
	h.g.flushQueue("sector-2")
	applied := h.sectors["sector-2"].applied
	if len(applied) != 2 {
		t.Fatalf("want 2 committed after recovery, got %d", len(applied))
	}
	if !strings.HasPrefix(applied[0].Key, "clearance/ATC004/") || !strings.HasPrefix(applied[1].Key, "clearance/ATC005/") {
		t.Fatal("buffered clearances must commit in the order they were sent")
	}
	if h.g.clr.st.Committed != 2 || len(h.g.clr.queued()) != 0 {
		t.Fatalf("stats %+v queues %v", h.g.clr.st, h.g.clr.queued())
	}
}

func TestBufferedClearanceRejectedAfterHandoff(t *testing.T) {
	h := newHarness(t)
	h.own("ATC006", "sector-2", fsm.Active)
	h.setLeader("sector-2", false)
	if code, _ := h.post(t, "ATC006", "DESCEND FL100"); code != http.StatusAccepted {
		t.Fatalf("want pending, got %d", code)
	}

	// While it waits, the aircraft is handed to sector-1.
	h.own("ATC006", "sector-1", fsm.Active)
	h.setLeader("sector-2", true)
	h.g.flushQueue("sector-2")

	c := h.g.clr.recent[0]
	if c.Status != ClrRejected || !strings.Contains(c.Reason, "ownership changed") {
		t.Fatalf("want rejected for ownership change, got %+v", c)
	}
	if len(h.sectors["sector-1"].applied)+len(h.sectors["sector-2"].applied) != 0 {
		t.Fatal("a clearance for the old sector must not be committed anywhere")
	}
}

func TestLeaderLostDuringWriteIsBuffered(t *testing.T) {
	h := newHarness(t)
	h.own("ATC007", "sector-1", fsm.Active)
	h.sectors["sector-1"].down = true // Ready says yes, but the write fails

	code, c := h.post(t, "ATC007", "CLIMB FL350")
	if code != http.StatusAccepted || c.WaitingOn != "sector-1" {
		t.Fatalf("want buffered after a failed write, got %d %+v", code, c)
	}
	h.sectors["sector-1"].down = false
	h.g.flushQueue("sector-1")
	if h.g.clr.recent[0].Status != ClrCommitted {
		t.Fatalf("want committed on retry, got %+v", h.g.clr.recent[0])
	}
}

func TestRegistryDownBuffersOnRegistry(t *testing.T) {
	h := newHarness(t)
	h.own("ATC008", "sector-1", fsm.Active)
	h.setLeader("registry", false)

	code, c := h.post(t, "ATC008", "CLIMB FL200")
	if code != http.StatusAccepted || c.WaitingOn != "registry" {
		t.Fatalf("want pending on registry, got %d %+v", code, c)
	}
	h.setLeader("registry", true)
	h.g.flushQueue("registry")
	if got := h.g.clr.recent[0]; got.Status != ClrCommitted || got.Sector != "sector-1" {
		t.Fatalf("want committed in sector-1, got %+v", got)
	}
}

// The dashboard snapshot reads buffered clearances while the flush updates
// them; run both at once so `go test -race` can catch unguarded access.
func TestSnapshotWhileFlushing(t *testing.T) {
	h := newHarness(t)
	h.setLeader("sector-2", false)
	for i := 0; i < 20; i++ {
		cs := "ATC1" + string(rune('A'+i))
		h.own(cs, "sector-2", fsm.Active)
		h.post(t, cs, "HEADING 270")
	}
	h.setLeader("sector-2", true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			h.g.mu.Lock()
			_ = h.g.clearanceSnapshotLocked()
			h.g.mu.Unlock()
		}
	}()
	h.g.flushQueue("sector-2")
	<-done
	if h.g.clr.st.Committed != 20 {
		t.Fatalf("want 20 committed, got %d", h.g.clr.st.Committed)
	}
}
