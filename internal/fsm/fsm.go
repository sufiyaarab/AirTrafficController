// Package fsm is the replicated state machine that every Raft cluster
// (sector, tower, registry) runs. Each committed log entry is a Command.
//
// Three kinds of state live here:
//   - a generic key/value map ("set", "delete"), used for clearances and for a
//     target sector's "accept" record during a handoff;
//   - flight ownership ("claim", "begin", "commit", "abort"), used by the
//     airspace registry. This is the mutual exclusion: only the sector named in
//     an ACTIVE record may control that flight;
//   - the sector map ("sector_init", "split", "drain", "undrain", "merge"), also
//     used by the airspace registry. See sectors.go.
package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// Command is one entry in the Raft log.
type Command struct {
	Op    string          `json:"op"`
	Key   string          `json:"key"`   // flight callsign for ownership ops, sector name for sector ops
	Value json.RawMessage `json:"value"` // op-specific payload
}

// Ownership states.
const (
	Active       = "ACTIVE"
	Transferring = "TRANSFERRING"
)

// Owner is the registry's record for one flight.
type Owner struct {
	Callsign string `json:"callsign"`
	Status   string `json:"status"`           // ACTIVE or TRANSFERRING
	Sector   string `json:"sector"`           // current holder (the source while TRANSFERRING)
	Target   string `json:"target,omitempty"` // set only while TRANSFERRING
	Index    uint64 `json:"index"`            // log index of the last change
}

// Move is the payload of claim/begin/accept/commit/abort.
type Move struct {
	From string `json:"from,omitempty"`
	To   string `json:"to"`
}

// FSM holds the committed state. Reads are local and may be stale on
// followers; route reads that must be linearizable through the leader.
type FSM struct {
	mu      sync.RWMutex
	state   map[string]json.RawMessage
	owners  map[string]Owner
	sectors map[string]Sector // sector map, registry only (see sectors.go)
	mapVer  uint64            // log index of the last sector-map change
}

func New() *FSM {
	return &FSM{
		state:   map[string]json.RawMessage{},
		owners:  map[string]Owner{},
		sectors: map[string]Sector{},
	}
}

// Apply returns either an error (the command was committed but rejected, and
// changed nothing) or the resulting Owner for ownership ops, or the resulting
// SectorMap for sector ops.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return fmt.Errorf("bad command: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch c.Op {
	case "set":
		f.state[c.Key] = c.Value
		return nil
	case "delete":
		delete(f.state, c.Key)
		return nil
	case "sector_init", "split", "drain", "undrain", "merge":
		return f.applySector(l, c)
	}

	var m Move
	if err := json.Unmarshal(c.Value, &m); err != nil {
		return fmt.Errorf("bad %s payload: %w", c.Op, err)
	}
	cur, owned := f.owners[c.Key]
	next := cur
	switch c.Op {
	case "claim": // first sighting: no owner yet
		if owned {
			return fmt.Errorf("%s already owned by %s", c.Key, cur.Sector)
		}
		if err := f.canReceive(m.To); err != nil {
			return err
		}
		next = Owner{Callsign: c.Key, Status: Active, Sector: m.To}
	case "begin": // phase 1: ACTIVE(from) -> TRANSFERRING(from -> to)
		if !owned || cur.Status != Active || cur.Sector != m.From {
			return fmt.Errorf("cannot begin %s %s->%s: %+v", c.Key, m.From, m.To, cur)
		}
		if err := f.canReceive(m.To); err != nil {
			return err
		}
		next.Status, next.Target = Transferring, m.To
	case "accept": // phase 2, committed in the TARGET sector's own log
		f.state["accepted/"+c.Key] = c.Value
		return nil
	case "commit": // phase 3: TRANSFERRING(from -> to) -> ACTIVE(to)
		if !owned || cur.Status != Transferring || cur.Sector != m.From || cur.Target != m.To {
			return fmt.Errorf("cannot commit %s %s->%s: %+v", c.Key, m.From, m.To, cur)
		}
		next = Owner{Callsign: c.Key, Status: Active, Sector: m.To}
	case "abort": // TRANSFERRING(from -> to) -> ACTIVE(from)
		if !owned || cur.Status != Transferring || cur.Sector != m.From {
			return fmt.Errorf("cannot abort %s: %+v", c.Key, cur)
		}
		next.Status, next.Target = Active, ""
	default:
		return fmt.Errorf("unknown op %q", c.Op)
	}
	next.Index = l.Index
	f.owners[c.Key] = next
	return next
}

// All returns a copy of the key/value state.
func (f *FSM) All() map[string]json.RawMessage {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]json.RawMessage, len(f.state))
	for k, v := range f.state {
		out[k] = v
	}
	return out
}

// Owners returns a copy of the ownership table.
func (f *FSM) Owners() map[string]Owner {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]Owner, len(f.owners))
	for k, v := range f.owners {
		out[k] = v
	}
	return out
}

type snapshotData struct {
	State      map[string]json.RawMessage `json:"state"`
	Owners     map[string]Owner           `json:"owners"`
	Sectors    map[string]Sector          `json:"sectors"`    // absent in snapshots written before the sector map existed
	MapVersion uint64                     `json:"mapVersion"` // ditto
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	sm := f.Sectors()
	return &snapshot{snapshotData{
		State:      f.All(),
		Owners:     f.Owners(),
		Sectors:    sm.Sectors,
		MapVersion: sm.Version,
	}}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var d snapshotData
	if err := json.NewDecoder(rc).Decode(&d); err != nil {
		return err
	}
	if d.State == nil {
		d.State = map[string]json.RawMessage{}
	}
	if d.Owners == nil {
		d.Owners = map[string]Owner{}
	}
	if d.Sectors == nil {
		d.Sectors = map[string]Sector{}
	}
	f.mu.Lock()
	f.state, f.owners, f.sectors, f.mapVer = d.State, d.Owners, d.Sectors, d.MapVersion
	f.mu.Unlock()
	return nil
}

type snapshot struct{ d snapshotData }

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.d); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
