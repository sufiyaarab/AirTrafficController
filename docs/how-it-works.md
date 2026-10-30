# How the scaffold works

What each file is for, how it maps onto SOFE4790U, and how mutual exclusion
works, including the Maekawa comparison.

## Why these pieces exist

The proposal needs three things to be true on demo day: many replicated
controller clusters, live traffic flowing through them, and a way to break
them on purpose and watch them recover. Each script covers one of those.

| File | What it does | Why it exists |
|---|---|---|
| `cmd/node/main.go` | One Raft replica. Joins its cluster, elects a leader, accepts writes only on the leader (`/apply`), replicates them. Also receives position tracks by multicast (`/track`) outside Raft. Saves its log to disk so a killed node rejoins. | Every sector, the tower, the registry and the standby are 3 copies of this program with different settings. One program, 21 containers. |
| `internal/fsm/fsm.go` | The state Raft keeps identical on all 3 replicas: flight ownership (`claim`, `begin`, `commit`, `abort`), the target's `accept` record, and a key/value map for clearances. `fsm_test.go` checks the handoff rules. | Raft guarantees every replica applies the same commands in the same order. |
| `cmd/gateway/main.go` | Finds each cluster's leader; B-multicasts each tick's positions to the 3 replicas of the controlling sector; runs the 3-phase handoff when an aircraft crosses a boundary; finishes any handoff left in `TRANSFERRING`. | Clients never need to know who the leader is (transparency). The gateway is the handoff coordinator (team choice). |
| `cmd/maekawa/main.go` | Maekawa's algorithm with grid voting sets and INQUIRE / FAILED / YIELD. 5 processes, one per control unit. | Baseline for the section 5 comparison (team choice). |
| `scripts/compare.py` | Runs both side by side, crashes one Maekawa voter and the registry leader, prints messages per entry and blocking, saves JSON to `results/`. | The experiment for the report. |
| `sim/sim.py` | Flies N aircraft across a 200 x 200 NM airspace; one batch per second, each report with a per-aircraft sequence number. | Load for the system. Mashal extends it with scenarios and conflicts. |
| `sim/receiver.py` | Placeholder ground receiver. | Reserves the 3 containers for Collins's spoofing cross-check. |
| `dashboard/` | The radar. Aircraft turn amber while `TRANSFERRING`; a panel shows handoff and multicast counters. | Makes failover and handoffs visible in the demo. |
| `scripts/chaos.sh` | Kills, pauses, partitions, delays or drops packets on a node; `kill-leader` crashes whichever node is leader. | Fault injection for the demo and experiments. |
| `deploy/go.Dockerfile` | Builds all Go programs in a Go image, copies them into a small Alpine image. | One image for every Go container; nobody installs Go. |
| `docker-compose.yml` | Starts everything with one command; `--profile maekawa` adds the baseline. | "About 20 nodes with a single command", as the proposal says. |
| `proto/atc.proto` | Draft gRPC messages. | Interface contract if the team moves off HTTP/JSON; not compiled yet. |

## General setup in course terms

- **Replication + consensus:** each sector is a 3-node Raft cluster. It
  tolerates 1 failed node (majority of 3 = 2).
- **Failure detection:** Raft heartbeats and election timeouts detect a dead
  leader; the gateway also polls every node every 500 ms.
- **Total ordering:** the Raft log index is a total order of every committed
  command (handoffs, clearances), so all replicas agree on what happened first.
- **Multicast (team choice: B-multicast + FIFO):** positions are not put
  through Raft. Each tick the gateway sends one batch to each of the 3
  replicas of the controlling sector (3 messages per sector per tick). Each
  replica keeps the newest report per aircraft and drops any report with a
  sequence number it has already passed, so a target never jumps backwards.
  No retransmission: if the gateway dies mid-send, replicas can disagree until
  the next report one second later.
- **Logical clocks:** the Raft term number works like a logical clock for
  leadership; a message from an old term is rejected.
- **Transparency:** the simulator and radar only talk to the gateway; which
  node is leader is hidden from them.
- **Fault tolerance testing:** `chaos.sh` produces crash, hang, partition and
  slow-network failures.

## Mutual exclusion

The critical section is "being the controller of aircraft X". Safety: two
sectors never control X at once. Liveness: a handoff always finishes.

### What runs: Raft registry, 3-phase handoff, gateway coordinates

The registry cluster is the "central server" of the central-server algorithm,
made fault tolerant by Raft (survives 1 of 3 crashing). Ownership lives in its
replicated log.

1. **begin** (registry commit): `ACTIVE(from)` -> `TRANSFERRING(from -> to)`.
   Rejected unless `from` is the current owner, so two handoffs of the same
   aircraft can never overlap.
2. **accept** (commit in the target sector's own Raft log). If the target has
   no leader (lost its majority) the gateway sends **abort**:
   `TRANSFERRING` -> `ACTIVE(from)`, and retries on the next report.
3. **commit** (registry commit): `TRANSFERRING` -> `ACTIVE(to)`.

While `TRANSFERRING`, the source sector still controls the aircraft, so there
is always exactly one controller. If the gateway or the registry leader
crashes mid-handoff, the `TRANSFERRING` entry is still in the log; the gateway
sees it on its next poll and resumes from phase 2 (`recovered` counter).

Cost per handoff: 3 RPCs = 6 messages, plus Raft replication of 3 commits
(about 4 messages each with 3 replicas) = about 18 messages, 3 round trips.

Tested in Docker: handoffs complete in about 7 ms; with 2 of sector-2's 3
nodes killed, handoffs into sector-2 abort and the aircraft stays with its
source; once the nodes return, the handoffs go through. A handoff left in
`TRANSFERRING` by hand was finished by the gateway within one poll.

### Baseline: Maekawa (grid voting sets, INQUIRE / FAILED / YIELD)

5 processes on a 3-wide grid; each voting set is the process's row plus
column, so K is 3 or 4 (formula 2*sqrt(N) - 1 = 3.5) and every two sets share a
member. Deadlock is avoided with INQUIRE / FAILED / YIELD.

| Process | Voting set |
|---|---|
| 0 | 0, 1, 2, 3 |
| 1 | 0, 1, 2, 4 |
| 2 | 0, 1, 2 |
| 3 | 0, 3, 4 |
| 4 | 1, 3, 4 |

### First comparison run (`scripts/compare.py --seconds 80 --crash-after 40`)

|  | Maekawa | Raft registry |
|---|---|---|
| Entries / handoffs, 40 s before crash | 390 | 3 |
| Messages per entry, before crash | 11.2 (measured) | 18 (6 measured + 12 Raft, estimated) |
| Entries / handoffs, 40 s after crash | 4 | 6 |
| Processes blocked after crash | 4 of 4 survivors | 0 |
| Safety violations | 0 | 0 |

The crash was one Maekawa voter (`maekawa-2`) and the registry leader. Every
surviving Maekawa process blocked, including process 4 whose voting set does
not contain 2, because its voters' votes were held by blocked requesters.
The registry elected a new leader and kept handing off. Maekawa is cheaper per
entry; the registry is the one that stays live. The workloads differ
(Maekawa requests continuously, handoffs only happen at boundaries), so
compare cost per entry and post-crash behaviour, not totals.
