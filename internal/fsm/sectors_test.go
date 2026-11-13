package fsm

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

// applyAny commits a command with any payload (apply in fsm_test.go only takes a Move).
func applyAny(f *FSM, i uint64, op, key string, payload interface{}) interface{} {
	v, _ := json.Marshal(payload)
	b, _ := json.Marshal(Command{Op: op, Key: key, Value: v})
	return f.Apply(&raft.Log{Index: i, Data: b})
}

func mustMap(t *testing.T, r interface{}) SectorMap {
	t.Helper()
	if err, bad := r.(error); bad {
		t.Fatalf("expected success, got: %v", err)
	}
	return r.(SectorMap)
}

func mustFail(t *testing.T, r interface{}, why string) {
	t.Helper()
	if _, bad := r.(error); !bad {
		t.Fatalf("expected rejection: %s", why)
	}
}

// seeded returns an FSM with one 100x100 sector and one idle standby cluster.
func seeded(t *testing.T) *FSM {
	t.Helper()
	f := New()
	mustMap(t, applyAny(f, 1, "sector_init", "", []Sector{
		{Name: "sector-1", Status: SectorActive, Region: Region{0, 0, 100, 100}},
		{Name: "spare", Status: SectorStandby},
	}))
	return f
}

func TestSectorInit(t *testing.T) {
	f := seeded(t)
	mustFail(t, applyAny(f, 2, "sector_init", "", []Sector{{Name: "x", Status: SectorStandby}}), "second init")

	g := New()
	mustFail(t, applyAny(g, 1, "sector_init", "", []Sector{
		{Name: "a", Status: SectorActive, Region: Region{0, 0, 60, 100}},
		{Name: "b", Status: SectorActive, Region: Region{50, 0, 100, 100}},
	}), "overlapping sectors")
	if len(g.Sectors().Sectors) != 0 {
		t.Fatal("a rejected init must leave the map empty")
	}
}

func TestSplit(t *testing.T) {
	f := seeded(t)
	m := mustMap(t, applyAny(f, 2, "split", "sector-1", SplitReq{Child: "spare", Axis: "x"}))
	s1, sp := m.Sectors["sector-1"], m.Sectors["spare"]
	if s1.Region != (Region{0, 0, 50, 100}) || sp.Region != (Region{50, 0, 100, 100}) || sp.Status != SectorActive {
		t.Fatalf("bad split: %+v / %+v", s1, sp)
	}
	if m.Version != 2 {
		t.Fatalf("version %d, want 2", m.Version)
	}
	// every point belongs to exactly one sector, including the border itself
	for _, p := range [][2]float64{{10, 10}, {49.9, 99}, {50, 10}, {99, 0}} {
		n := 0
		for _, s := range m.Sectors {
			if s.Region.Contains(p[0], p[1]) {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("point %v is in %d sectors", p, n)
		}
	}
	mustFail(t, applyAny(f, 3, "split", "sector-1", SplitReq{Child: "spare"}), "child no longer STANDBY")
	mustFail(t, applyAny(f, 4, "split", "nope", SplitReq{Child: "spare"}), "unknown parent")
}

func TestSplitDefaultsToLongerSide(t *testing.T) {
	f := New()
	mustMap(t, applyAny(f, 1, "sector_init", "", []Sector{
		{Name: "wide", Status: SectorActive, Region: Region{0, 0, 200, 100}},
		{Name: "spare", Status: SectorStandby},
	}))
	m := mustMap(t, applyAny(f, 2, "split", "wide", SplitReq{Child: "spare"}))
	if m.Sectors["wide"].Region != (Region{0, 0, 100, 100}) {
		t.Fatalf("a 200x100 sector should be cut on x: %+v", m.Sectors["wide"].Region)
	}
}

func TestSplitAxisY(t *testing.T) {
	f := seeded(t)
	m := mustMap(t, applyAny(f, 2, "split", "sector-1", SplitReq{Child: "spare", Axis: "y"}))
	if m.Sectors["sector-1"].Region != (Region{0, 0, 100, 50}) || m.Sectors["spare"].Region != (Region{0, 50, 100, 100}) {
		t.Fatalf("bad y split: %+v", m.Sectors)
	}
	mustFail(t, applyAny(seeded(t), 2, "split", "sector-1", SplitReq{Child: "spare", Axis: "z"}), "bad axis")
}

func TestMergeNeedsDrainAndNoFlights(t *testing.T) {
	f := seeded(t)
	mustMap(t, applyAny(f, 2, "split", "sector-1", SplitReq{Child: "spare", Axis: "x"}))

	// a flight is held by the sector we want to release
	apply(f, 3, "claim", "AC9", Move{To: "spare"})

	mustFail(t, applyAny(f, 4, "merge", "spare", MergeReq{Into: "sector-1"}), "merge without draining first")
	mustMap(t, applyAny(f, 5, "drain", "spare", nil))

	// a draining sector takes no new flights, by claim or by handoff
	mustFail(t, apply(f, 6, "claim", "AC7", Move{To: "spare"}), "claim into DRAINING sector")
	apply(f, 7, "claim", "AC8", Move{To: "sector-1"})
	mustFail(t, apply(f, 8, "begin", "AC8", Move{From: "sector-1", To: "spare"}), "handoff into DRAINING sector")

	mustFail(t, applyAny(f, 9, "merge", "spare", MergeReq{Into: "sector-1"}), "merge while AC9 still held")

	// drain the flight out with the normal 3-phase handoff, then merge
	if _, ok := apply(f, 10, "begin", "AC9", Move{From: "spare", To: "sector-1"}).(Owner); !ok {
		t.Fatal("draining sector must still be able to hand flights OUT")
	}
	mustFail(t, applyAny(f, 11, "merge", "spare", MergeReq{Into: "sector-1"}), "merge while AC9 is mid-handoff")
	apply(f, 12, "commit", "AC9", Move{From: "spare", To: "sector-1"})

	m := mustMap(t, applyAny(f, 13, "merge", "spare", MergeReq{Into: "sector-1"}))
	if m.Sectors["sector-1"].Region != (Region{0, 0, 100, 100}) {
		t.Fatalf("regions did not rejoin: %+v", m.Sectors["sector-1"].Region)
	}
	if sp := m.Sectors["spare"]; sp.Status != SectorStandby || !sp.Region.Empty() {
		t.Fatalf("spare must be back on STANDBY with no region: %+v", sp)
	}
	// and it can be split into again
	mustMap(t, applyAny(f, 14, "split", "sector-1", SplitReq{Child: "spare"}))
}

func TestDrainNeedsNeighbourAndUndrain(t *testing.T) {
	f := seeded(t)
	mustFail(t, applyAny(f, 2, "drain", "sector-1", nil), "only sector, nobody to take its flights")
	mustMap(t, applyAny(f, 3, "split", "sector-1", SplitReq{Child: "spare"}))
	mustMap(t, applyAny(f, 4, "drain", "spare", nil))
	mustFail(t, applyAny(f, 5, "split", "spare", SplitReq{Child: "x"}), "cannot split a DRAINING sector")
	mustMap(t, applyAny(f, 6, "undrain", "spare", nil))
	if _, ok := apply(f, 7, "claim", "AC1", Move{To: "spare"}).(Owner); !ok {
		t.Fatal("after undrain the sector takes flights again")
	}
	mustFail(t, applyAny(f, 8, "undrain", "spare", nil), "undrain a sector that is not DRAINING")
}

func TestMergeNeedsSharedEdge(t *testing.T) {
	f := New()
	mustMap(t, applyAny(f, 1, "sector_init", "", []Sector{
		{Name: "a", Status: SectorActive, Region: Region{0, 0, 50, 50}},
		{Name: "b", Status: SectorActive, Region: Region{50, 0, 100, 40}}, // shorter: no full edge with a
	}))
	mustFail(t, applyAny(f, 2, "drain", "b", nil), "no neighbour sharing a full edge")
}

func TestOwnershipUnchangedWithoutSectorMap(t *testing.T) {
	// sectors the map does not know (tower, or an unseeded registry) behave as before
	f := New()
	if _, ok := apply(f, 1, "claim", "AC1", Move{To: "tower"}).(Owner); !ok {
		t.Fatal("claim into an unknown sector must still work")
	}
}

type sink struct{ bytes.Buffer }

func (s *sink) ID() string    { return "test" }
func (s *sink) Cancel() error { return nil }
func (s *sink) Close() error  { return nil }

func TestSnapshotKeepsSectorMap(t *testing.T) {
	f := seeded(t)
	mustMap(t, applyAny(f, 2, "split", "sector-1", SplitReq{Child: "spare"}))
	apply(f, 3, "claim", "AC1", Move{To: "spare"})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var s sink
	if err := snap.Persist(&s); err != nil {
		t.Fatal(err)
	}

	g := New()
	if err := g.Restore(io.NopCloser(&s.Buffer)); err != nil {
		t.Fatal(err)
	}
	if a, b := f.Sectors(), g.Sectors(); a.Version != b.Version || len(b.Sectors) != 2 || a.Sectors["spare"] != b.Sectors["spare"] {
		t.Fatalf("map lost in snapshot:\n before %+v\n after  %+v", a, b)
	}
	if g.Owners()["AC1"].Sector != "spare" {
		t.Fatal("ownership lost in snapshot")
	}
}

func TestRestoreOldSnapshotWithoutSectorMap(t *testing.T) {
	// a snapshot written before this change has no "sectors" field
	old := `{"state":{"k":"1"},"owners":{"AC1":{"callsign":"AC1","status":"ACTIVE","sector":"s1","index":3}}}`
	f := New()
	if err := f.Restore(io.NopCloser(bytes.NewBufferString(old))); err != nil {
		t.Fatal(err)
	}
	if f.Sectors().Sectors == nil || len(f.Sectors().Sectors) != 0 || f.Owners()["AC1"].Sector != "s1" {
		t.Fatal("old snapshot should restore with an empty sector map")
	}
}

func TestMergeTarget(t *testing.T) {
	f := seeded(t)
	if _, ok := f.Sectors().MergeTarget("sector-1"); ok {
		t.Fatal("a lone sector has nobody to merge into")
	}
	mustMap(t, applyAny(f, 2, "split", "sector-1", SplitReq{Child: "spare", Axis: "x"}))
	m := f.Sectors()
	if to, ok := m.MergeTarget("spare"); !ok || to != "sector-1" {
		t.Fatalf("spare should merge into sector-1, got %q %v", to, ok)
	}
	if to, ok := m.MergeTarget("sector-1"); !ok || to != "spare" {
		t.Fatalf("and the other way round, got %q %v", to, ok)
	}
	if _, ok := m.MergeTarget("nope"); ok {
		t.Fatal("unknown sector")
	}

	// two neighbours share a full edge with "a": the lowest name wins, every time
	g := New()
	mustMap(t, applyAny(g, 1, "sector_init", "", []Sector{
		{Name: "a", Status: SectorActive, Region: Region{0, 0, 50, 50}},
		{Name: "c", Status: SectorActive, Region: Region{0, 50, 50, 100}},
		{Name: "b", Status: SectorActive, Region: Region{50, 0, 100, 50}},
	}))
	if to, _ := g.Sectors().MergeTarget("a"); to != "b" {
		t.Fatalf("lowest-named neighbour should win, got %q", to)
	}
	// a DRAINING neighbour is not a valid target
	mustMap(t, applyAny(g, 2, "drain", "b", nil))
	if to, _ := g.Sectors().MergeTarget("a"); to != "c" {
		t.Fatalf("a draining sector cannot absorb others, got %q", to)
	}
}
