package main

import (
	"testing"
	"time"
)

func TestComputeReadiness(t *testing.T) {
	ids := []string{"s-a", "s-b", "s-c"}
	cases := []struct {
		name   string
		nodes  map[string]nodeStatus
		ready  bool
		leader string
		reason string
	}{
		{"one leader", map[string]nodeStatus{
			"s-a": {Up: true, State: "Follower"}, "s-b": {Up: true, State: "Leader"}, "s-c": {Up: true, State: "Follower"},
		}, true, "s-b", ""},
		{"election in progress", map[string]nodeStatus{
			"s-a": {Up: true, State: "Candidate"}, "s-b": {Up: true, State: "Follower"}, "s-c": {Up: false},
		}, false, "", "no leader"},
		{"leader is down", map[string]nodeStatus{
			// a node that is not answering is never counted, whatever it last said
			"s-a": {Up: false, State: "Leader"}, "s-b": {Up: true, State: "Follower"}, "s-c": {Up: true, State: "Follower"},
		}, false, "", "no leader"},
		{"partition: old and new leader", map[string]nodeStatus{
			"s-a": {Up: true, State: "Leader"}, "s-b": {Up: true, State: "Leader"}, "s-c": {Up: true, State: "Follower"},
		}, false, "", "2 leaders"},
		{"never polled", map[string]nodeStatus{}, false, "", "no leader"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ready, leader, reason := computeReadiness(ids, tc.nodes)
			if ready != tc.ready || leader != tc.leader || reason != tc.reason {
				t.Fatalf("got (%v, %q, %q), want (%v, %q, %q)", ready, leader, reason, tc.ready, tc.leader, tc.reason)
			}
		})
	}
}

func newTestGateway() *gateway {
	return &gateway{
		clusters: map[string][]string{"sector-1": {"s-a", "s-b", "s-c"}},
		nodes: map[string]nodeStatus{
			"s-a": {Up: true, State: "Leader"}, "s-b": {Up: true, State: "Follower"}, "s-c": {Up: true, State: "Follower"},
		},
		ready: map[string]readiness{},
	}
}

func TestUpdateReadinessTransitions(t *testing.T) {
	g := newTestGateway()
	t0 := time.Unix(1000, 0)

	g.updateReadiness(t0)
	if !g.readyLocked("sector-1", t0) {
		t.Fatal("want Ready with one leader")
	}
	if g.ready["sector-1"].SinceMs != t0.UnixMilli() {
		t.Fatal("since_ms should be set on the first poll")
	}

	// Same state on the next poll: since_ms must not move.
	g.updateReadiness(t0.Add(500 * time.Millisecond))
	if g.ready["sector-1"].SinceMs != t0.UnixMilli() {
		t.Fatal("since_ms changed without a state change")
	}

	// Leader killed.
	g.nodes["s-a"] = nodeStatus{Up: false}
	t1 := t0.Add(time.Second)
	g.updateReadiness(t1)
	if g.readyLocked("sector-1", t1) {
		t.Fatal("want NotReady after the leader dies")
	}
	if r := g.ready["sector-1"]; r.Reason != "no leader" || r.SinceMs != t1.UnixMilli() {
		t.Fatalf("unexpected readiness %+v", r)
	}

	// New leader elected.
	g.nodes["s-c"] = nodeStatus{Up: true, State: "Leader"}
	t2 := t1.Add(1500 * time.Millisecond)
	g.updateReadiness(t2)
	if !g.readyLocked("sector-1", t2) || g.ready["sector-1"].Leader != "s-c" {
		t.Fatalf("want Ready with leader s-c, got %+v", g.ready["sector-1"])
	}
}

func TestReadyGoesStale(t *testing.T) {
	g := newTestGateway()
	t0 := time.Unix(1000, 0)
	g.updateReadiness(t0)

	if !g.readyLocked("sector-1", t0.Add(readyMaxAge)) {
		t.Fatal("should still be Ready exactly at the age limit")
	}
	late := t0.Add(readyMaxAge + time.Millisecond)
	if g.readyLocked("sector-1", late) {
		t.Fatal("should not trust a poll older than readyMaxAge")
	}
	if r := g.readySnapshotLocked(late)["sector-1"]; r.Ready || r.Reason != "stale poll" {
		t.Fatalf("dashboard should show stale, got %+v", r)
	}
	if g.readyLocked("unknown", t0) {
		t.Fatal("an unknown cluster is never Ready")
	}
}
