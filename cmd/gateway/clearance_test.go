package main

import (
	"strings"
	"testing"
	"time"
)

func TestNewClearanceNormalizes(t *testing.T) {
	now := time.Unix(1000, 0)
	c, err := newClearance(clearanceRequest{Callsign: " atc042 ", Instruction: "  descend   fl250 "}, now)
	if err != nil {
		t.Fatal(err)
	}
	if c.Callsign != "ATC042" || c.Instruction != "DESCEND FL250" || c.IssuedBy != "anonymous" {
		t.Fatalf("not normalized: %+v", c)
	}
	if c.ID == "" || c.Status != ClrPending || c.CreatedMs != now.UnixMilli() {
		t.Fatalf("bad defaults: %+v", c)
	}
}

func TestNewClearanceRejectsBadInput(t *testing.T) {
	bad := []clearanceRequest{
		{Callsign: "", Instruction: "CLIMB FL300"},
		{Callsign: "ATC001", Instruction: "   "},
		{Callsign: "ATC001", Instruction: strings.Repeat("X", maxInstructionLen+1)},
	}
	for _, req := range bad {
		if _, err := newClearance(req, time.Now()); err == nil {
			t.Errorf("accepted %+v", req)
		}
	}
}

func TestClearanceIDsAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := newID()
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
	}
}

func mk(id string, created time.Time) *clearance {
	return &clearance{ID: id, Callsign: "ATC" + id, Status: ClrPending, CreatedMs: created.UnixMilli()}
}

func TestQueueKeepsOrderAndExpires(t *testing.T) {
	var s clearanceState
	t0 := time.Unix(1000, 0)
	s.push("sector-2", mk("1", t0))
	s.push("sector-2", mk("2", t0.Add(5*time.Second)))
	s.push("registry", mk("3", t0))

	// Nothing is older than the TTL yet.
	if got := s.expire(t0.Add(clearanceTTL)); len(got) != 0 {
		t.Fatalf("expired too early: %d", len(got))
	}

	// 1 and 3 are now too old; 2 is not.
	got := s.expire(t0.Add(clearanceTTL + time.Millisecond))
	if len(got) != 2 || s.st.Expired != 2 {
		t.Fatalf("want 2 expired, got %d (stat %d)", len(got), s.st.Expired)
	}
	for _, c := range got {
		if c.Status != ClrExpired {
			t.Fatalf("not marked expired: %+v", c)
		}
	}
	if q := s.queues["sector-2"]; len(q) != 1 || q[0].ID != "2" {
		t.Fatalf("wrong survivor: %+v", q)
	}
	if _, ok := s.queues["registry"]; ok {
		t.Fatal("empty queue should be removed")
	}
	if n := s.queued(); n["sector-2"] != 1 || len(n) != 1 {
		t.Fatalf("queued() = %v", n)
	}
}

func TestQueueCap(t *testing.T) {
	var s clearanceState
	now := time.Now()
	for i := 0; i < clearanceQueueCap; i++ {
		if err := s.push("sector-1", mk("x", now)); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	if err := s.push("sector-1", mk("y", now)); err == nil {
		t.Fatal("push past the cap should fail")
	}
	if err := s.push("sector-3", mk("z", now)); err != nil {
		t.Fatal("caps are per cluster")
	}
}

func TestRecentIsBounded(t *testing.T) {
	var s clearanceState
	for i := 0; i < clearanceRecent+5; i++ {
		s.remember(&clearance{ID: string(rune('a' + i%26))})
	}
	if len(s.recent) != clearanceRecent {
		t.Fatalf("recent has %d, want %d", len(s.recent), clearanceRecent)
	}
}
