package fsm

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
)

func apply(f *FSM, i uint64, op, key string, m Move) interface{} {
	v, _ := json.Marshal(m)
	b, _ := json.Marshal(Command{Op: op, Key: key, Value: v})
	return f.Apply(&raft.Log{Index: i, Data: b})
}

func TestHandoffStates(t *testing.T) {
	f := New()
	if _, ok := apply(f, 1, "claim", "AC1", Move{To: "s1"}).(Owner); !ok {
		t.Fatal("claim failed")
	}
	if _, ok := apply(f, 2, "claim", "AC1", Move{To: "s2"}).(error); !ok {
		t.Fatal("second claim must be rejected: two owners")
	}
	if _, ok := apply(f, 3, "begin", "AC1", Move{From: "s2", To: "s3"}).(error); !ok {
		t.Fatal("begin from a sector that is not the owner must be rejected")
	}
	o := apply(f, 4, "begin", "AC1", Move{From: "s1", To: "s2"}).(Owner)
	if o.Status != Transferring || o.Sector != "s1" || o.Target != "s2" {
		t.Fatalf("after begin: %+v", o)
	}
	if _, ok := apply(f, 5, "begin", "AC1", Move{From: "s1", To: "s3"}).(error); !ok {
		t.Fatal("a second handoff must not start while one is in progress")
	}
	o = apply(f, 6, "commit", "AC1", Move{From: "s1", To: "s2"}).(Owner)
	if o.Status != Active || o.Sector != "s2" {
		t.Fatalf("after commit: %+v", o)
	}
	apply(f, 7, "begin", "AC1", Move{From: "s2", To: "s4"})
	o = apply(f, 8, "abort", "AC1", Move{From: "s2", To: "s4"}).(Owner)
	if o.Status != Active || o.Sector != "s2" {
		t.Fatalf("after abort: %+v", o)
	}
}
