package main

// Sector Ready flag (docs/collins_design.md, section 3).
//
// A cluster is Ready when, in the gateway's latest poll, exactly one of its
// nodes is up and reports itself Leader, and that poll is recent.
//
//   - During an election there is no leader yet: NotReady ("no leader").
//   - During a partition the old leader can keep believing it leads while the
//     majority side elects a new one, so two nodes may report Leader for a
//     moment: NotReady ("2 leaders"), because the gateway cannot tell which
//     one will accept writes.
//   - If polling itself stalls, the last answer goes out of date:
//     NotReady ("stale poll").
//
// Ready is a hint, not a guarantee: a leader can fail right after a poll.
// Everything that writes to a cluster must still handle failure (the handoff
// already aborts back to the source if the target's accept fails).

import (
	"fmt"
	"log"
	"time"
)

// readyMaxAge is how old the last poll may be before Ready is no longer
// trusted: three poll intervals of 500 ms.
const readyMaxAge = 1500 * time.Millisecond

type readiness struct {
	Ready   bool      `json:"ready"`
	Leader  string    `json:"leader,omitempty"`
	Reason  string    `json:"reason,omitempty"` // why NotReady
	SinceMs int64     `json:"since_ms"`         // unix ms when Ready last changed
	checked time.Time // when this was computed (not sent to the dashboard)
}

// computeReadiness decides Ready for one cluster from its nodes' last status.
func computeReadiness(ids []string, nodes map[string]nodeStatus) (ready bool, leader, reason string) {
	leaders := 0
	for _, id := range ids {
		if st := nodes[id]; st.Up && st.State == "Leader" {
			leaders++
			leader = id
		}
	}
	switch leaders {
	case 1:
		return true, leader, ""
	case 0:
		return false, "", "no leader"
	default:
		return false, "", fmt.Sprintf("%d leaders", leaders)
	}
}

// updateReadiness recomputes every cluster's flag after a poll and logs each
// change. The NotReady -> Ready log line includes how long the cluster was
// without a leader, as seen by the gateway: the failover time for the report.
func (g *gateway) updateReadiness(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for c, ids := range g.clusters {
		ready, leader, reason := computeReadiness(ids, g.nodes)
		prev, seen := g.ready[c]
		next := readiness{Ready: ready, Leader: leader, Reason: reason, SinceMs: prev.SinceMs, checked: now}
		if !seen || prev.Ready != ready {
			next.SinceMs = now.UnixMilli()
			switch {
			case !seen && ready:
				log.Printf("ready: %s READY leader=%s", c, leader)
			case !seen:
				log.Printf("ready: %s NOT READY (%s)", c, reason)
			case ready:
				log.Printf("ready: %s READY leader=%s after %.2fs not ready", c, leader, since(prev, now))
			default:
				log.Printf("ready: %s NOT READY (%s) after %.2fs ready", c, reason, since(prev, now))
			}
		} else if ready && prev.Leader != leader {
			// Leader changed between two polls without a NotReady poll in between.
			log.Printf("ready: %s leader %s -> %s", c, prev.Leader, leader)
		}
		g.ready[c] = next
	}
}

func since(r readiness, now time.Time) float64 {
	return float64(now.UnixMilli()-r.SinceMs) / 1000
}

// readyLocked reports whether cluster c is Ready now. Must be called with g.mu held.
func (g *gateway) readyLocked(c string, now time.Time) bool {
	r, ok := g.ready[c]
	return ok && r.Ready && now.Sub(r.checked) <= readyMaxAge
}

// readySnapshotLocked copies the flags for the dashboard, marking any that
// are out of date. Must be called with g.mu held.
func (g *gateway) readySnapshotLocked(now time.Time) map[string]readiness {
	out := make(map[string]readiness, len(g.ready))
	for c, r := range g.ready {
		if r.Ready && now.Sub(r.checked) > readyMaxAge {
			r.Ready, r.Leader, r.Reason = false, "", "stale poll"
		}
		out[c] = r
	}
	return out
}
