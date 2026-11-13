// Command node runs one replica of a Raft cluster. The same binary is used
// for every sector, the tower, the airspace registry and the standby cluster;
// only the environment differs (see docker-compose.yml).
//
// Convention: NODE_ID is also the container hostname, so any node can turn a
// leader ID into the leader's HTTP address (<id>:8080).
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"atc/internal/fsm"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	id := env("NODE_ID", "node-1")
	cluster := env("CLUSTER", "dev")
	raftBind := env("RAFT_BIND", ":7000")
	raftAdvertise := env("RAFT_ADVERTISE", id+":7000")
	httpAddr := env("HTTP_ADDR", ":8080")
	dataDir := env("DATA_DIR", "/data")
	peers := env("PEERS", id+"="+raftAdvertise) // "a=a:7000,b=b:7000,c=c:7000"

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatal(err)
	}

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)

	// Bolt-backed log and stable store, so a killed node restarted with
	// `docker start` rejoins from its own log instead of starting empty.
	store, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		log.Fatal(err)
	}
	snaps, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}
	adv, err := resolve(raftAdvertise)
	if err != nil {
		log.Fatalf("resolve %s: %v", raftAdvertise, err)
	}
	trans, err := raft.NewTCPTransport(raftBind, adv, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatal(err)
	}

	state := fsm.New()
	r, err := raft.NewRaft(cfg, state, store, store, snaps, trans)
	if err != nil {
		log.Fatal(err)
	}

	// Every replica bootstraps with the same peer list. Only the first boot
	// succeeds; after that the log already holds a configuration.
	var servers []raft.Server
	for _, p := range strings.Split(peers, ",") {
		pid, paddr, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok {
			log.Fatalf("bad PEERS entry %q", p)
		}
		servers = append(servers, raft.Server{ID: raft.ServerID(pid), Address: raft.ServerAddress(paddr)})
	}
	if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
		log.Fatal(err)
	}

	s := &server{id: id, cluster: cluster, raft: r, fsm: state, tracks: map[string]track{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /status", s.status)
	mux.HandleFunc("GET /state", s.state)
	mux.HandleFunc("POST /apply", s.apply)
	mux.HandleFunc("GET /owners", s.owners)
	mux.HandleFunc("GET /sectors", s.sectors)
	mux.HandleFunc("POST /track", s.track)
	mux.HandleFunc("GET /tracks", s.tracksHandler)
	mux.HandleFunc("GET /metrics", s.metrics)
	log.Printf("node %s (cluster %s) raft=%s http=%s", id, cluster, raftAdvertise, httpAddr)
	log.Fatal(http.ListenAndServe(httpAddr, mux))
}

// resolve retries because Docker's DNS entry can lag container start.
func resolve(addr string) (*net.TCPAddr, error) {
	var err error
	for range 30 {
		var a *net.TCPAddr
		if a, err = net.ResolveTCPAddr("tcp", addr); err == nil {
			return a, nil
		}
		time.Sleep(time.Second)
	}
	return nil, err
}

type server struct {
	id, cluster string
	raft        *raft.Raft
	fsm         *fsm.FSM

	// Position tracks arrive by B-multicast, not through Raft (see track).
	mu        sync.Mutex
	tracks    map[string]track
	delivered uint64
	dropped   uint64
}

type track struct {
	Seq    uint64          `json:"seq"`
	Report json.RawMessage `json:"report"`
}

type status struct {
	ID          string `json:"id"`
	Cluster     string `json:"cluster"`
	State       string `json:"state"`
	LeaderID    string `json:"leader_id"`
	Term        string `json:"term"`
	CommitIndex string `json:"commit_index"`
	AppliedIdx  string `json:"applied_index"`
	LastContact string `json:"last_contact"`
}

func (s *server) status(w http.ResponseWriter, _ *http.Request) {
	st := s.raft.Stats()
	_, leader := s.raft.LeaderWithID()
	writeJSON(w, http.StatusOK, status{
		ID: s.id, Cluster: s.cluster, State: s.raft.State().String(), LeaderID: string(leader),
		Term: st["term"], CommitIndex: st["commit_index"], AppliedIdx: st["applied_index"], LastContact: st["last_contact"],
	})
}

func (s *server) state(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.fsm.All())
}

func (s *server) owners(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.fsm.Owners())
}

// sectors returns the registry's sector map (empty on every other cluster).
// Like /owners, read it from the leader when it must be current.
func (s *server) sectors(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.fsm.Sectors())
}

// track receives one B-multicast batch of position reports from the gateway.
// Delivery is FIFO per aircraft: each report carries the simulator's sequence
// number, and a report older than one already delivered is dropped, so a
// target never jumps backwards. Nothing is retransmitted (B-multicast): a
// lost report is simply replaced by the next one.
func (s *server) track(w http.ResponseWriter, req *http.Request) {
	var batch []json.RawMessage
	if err := json.NewDecoder(req.Body).Decode(&batch); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, raw := range batch {
		var hdr struct {
			Callsign string `json:"callsign"`
			Seq      uint64 `json:"seq"`
		}
		if json.Unmarshal(raw, &hdr) != nil || hdr.Callsign == "" {
			continue
		}
		if last, ok := s.tracks[hdr.Callsign]; ok && hdr.Seq <= last.Seq {
			s.dropped++
			continue
		}
		s.tracks[hdr.Callsign] = track{Seq: hdr.Seq, Report: raw}
		s.delivered++
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) tracksHandler(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, http.StatusOK, s.tracks)
}

// apply appends a command to the log. Only the leader accepts writes; a
// follower answers 421 with the leader's ID so the caller can retry there.
func (s *server) apply(w http.ResponseWriter, req *http.Request) {
	if s.raft.State() != raft.Leader {
		_, leader := s.raft.LeaderWithID()
		writeJSON(w, http.StatusMisdirectedRequest, map[string]string{"error": "not leader", "leader_id": string(leader)})
		return
	}
	var c fsm.Command
	if err := json.NewDecoder(req.Body).Decode(&c); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	b, _ := json.Marshal(c)
	f := s.raft.Apply(b, 2*time.Second)
	if err := f.Error(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	if err, ok := f.Response().(error); ok && err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"index": f.Index(), "result": f.Response()})
}

// metrics is a minimal Prometheus text exposition, enough for Grafana
// failover and commit-rate graphs without pulling in the client library.
func (s *server) metrics(w http.ResponseWriter, _ *http.Request) {
	st := s.raft.Stats()
	isLeader := 0
	if s.raft.State() == raft.Leader {
		isLeader = 1
	}
	labels := fmt.Sprintf(`{node=%q,cluster=%q}`, s.id, s.cluster)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "atc_raft_is_leader%s %d\n", labels, isLeader)
	fmt.Fprintf(w, "atc_raft_state%s %d\n", labels, s.raft.State())
	for _, k := range []string{"term", "commit_index", "applied_index", "last_log_index"} {
		fmt.Fprintf(w, "atc_raft_%s%s %s\n", k, labels, st[k])
	}
	s.mu.Lock()
	fmt.Fprintf(w, "atc_tracks_delivered_total%s %d\n", labels, s.delivered)
	fmt.Fprintf(w, "atc_tracks_dropped_total%s %d\n", labels, s.dropped)
	s.mu.Unlock()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
