# Air Traffic Controller but Different

SOFE4790U Distributed Systems, Fall 2026. Simulated air traffic control where
every airspace sector is a 3-node Raft cluster.

## What you need

- **Docker Desktop** (or Docker Engine + Compose v2). That is all you need to
  build and run everything. The Go compiler, Node and Python all run inside
  the build containers (`golang:1.26-alpine`, `node:22-alpine`, `python:3.12-slim`).
- Optional, for editing with autocomplete and running `go test` / `go vet`
  outside Docker: Go 1.26+. Optional for dashboard hot reload: Node 22.

## Run it

```bash
docker compose up --build            # first build takes a few minutes
docker compose --profile maekawa up --build   # also start the Maekawa baseline
```

- Radar dashboard: http://localhost:3000
- Gateway state (JSON): http://localhost:8080/api/state
- With metrics: `docker compose --profile metrics up --build`, then
  Prometheus on http://localhost:9090 and Grafana on http://localhost:3001

Stop with `docker compose down`. That also wipes the Raft logs, because they
live in each container's filesystem; `docker kill` / `docker start` keep them.

## What starts

| Containers | Count | What it is |
|---|---|---|
| `sector-1..4-{a,b,c}` | 12 | Four airspace sectors, 3 Raft replicas each |
| `tower-{a,b,c}` | 3 | Airport tower (15 NM circle in the middle, below 10,000 ft) |
| `registry-{a,b,c}` | 3 | Airspace registry: flight ownership + sector map |
| `standby-{a,b,c}` | 3 | Warm standby cluster for sector splits |
| `gateway` | 1 | Multicasts positions to the controlling sector, coordinates 3-phase handoffs, feeds the dashboard |
| `simulator` | 1 | Python aircraft simulator (`AIRCRAFT`, `TICK`, `SEED`) |
| `receiver-{a,b,c}` | 3 | Ground receivers (placeholders for the spoofing cross-check) |
| `dashboard` | 1 | React radar served by nginx, proxies `/api` and `/ws` to the gateway |
| `maekawa-0..4` | 5 | Maekawa baseline, only with `--profile maekawa` |
| `prometheus`, `grafana` | 2 | Only with `--profile metrics` |

All 21 Raft nodes run the same Go image (`deploy/go.Dockerfile`, multi-stage:
full Go toolchain to build, small Alpine image to run). `NODE_ID`, `CLUSTER`
and `PEERS` decide which cluster a node belongs to.

## Fault injection

```bash
scripts/chaos.sh status                  # leader of every cluster
scripts/chaos.sh kill-leader sector-1    # crash the leader, watch a new one get elected
scripts/chaos.sh start sector-1-b        # bring the old leader back; it catches up from its log
scripts/chaos.sh partition sector-2-b    # disconnect a node from the network
scripts/chaos.sh heal sector-2-b
scripts/chaos.sh pause sector-3-a        # freeze a node (slow, not dead)
scripts/chaos.sh delay sector-3-a 200    # 200 ms latency with tc netem
scripts/chaos.sh clear sector-3-a
```

## Compare the mutual exclusion approaches

```bash
docker compose --profile maekawa up -d --build
python3 scripts/compare.py --seconds 80 --crash-after 40
```

Crashes one Maekawa voter and the registry leader halfway, prints messages per
entry and blocking for both, and saves JSON to `results/`. See
[docs/how-it-works.md](docs/how-it-works.md) for what the numbers mean.

## Layout

```
cmd/node/          Raft replica (hashicorp/raft + raft-boltdb), HTTP: /status /state /apply /metrics
cmd/gateway/       gateway: leader tracking, position multicast, handoff coordinator, WebSocket feed
cmd/maekawa/       Maekawa baseline (grid voting sets, INQUIRE/FAILED/YIELD)
internal/fsm/      replicated state machine: ownership states + key/value
docs/              how-it-works.md: every file, the course mapping, the comparison
proto/atc.proto    DRAFT gRPC contract (not compiled yet)
sim/               simulator.py, receiver.py
dashboard/         React + Vite radar
deploy/            go.Dockerfile, prometheus.yml, grafana datasource
scripts/          chaos.sh (fault injection), compare.py (Maekawa vs registry)
```

## Current state and next steps

Working now: all clusters elect leaders and replicate; positions reach the
controlling sector's replicas by B-multicast with FIFO sequence numbers;
handoffs run in 3 phases through the registry (begin, target accepts, commit),
abort when the target has no majority, and resume after a crash; the Maekawa
baseline and comparison script run; the radar shows aircraft (amber while
transferring), sectors, every node's role/term, and handoff counters.

Not built yet (owner from the proposal):
- Clearances (Sufiya)
- Elastic split/merge using the standby cluster, sector map in the registry (Issaq)
- gRPC in place of HTTP/JSON, mTLS, controller login, receiver cross-check (Collins)
- Scenario files, conflict/stress modes, replay (Mashal)
- Safety-invariant panel, Grafana dashboards, more experiments (Chimidike)
