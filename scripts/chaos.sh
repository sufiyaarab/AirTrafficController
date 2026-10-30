#!/usr/bin/env bash
# Fault injection for the demo and experiments. Run from the repo root.
#
#   scripts/chaos.sh status                 leader of every cluster
#   scripts/chaos.sh kill-leader sector-1   crash the current leader of a cluster
#   scripts/chaos.sh kill  sector-1-a       crash one node (docker kill = SIGKILL)
#   scripts/chaos.sh start sector-1-a       bring it back; it rejoins from its own log
#   scripts/chaos.sh pause sector-1-a       freeze it (looks like a hang, not a crash)
#   scripts/chaos.sh unpause sector-1-a
#   scripts/chaos.sh partition sector-1-a   cut it off the network
#   scripts/chaos.sh heal sector-1-a
#   scripts/chaos.sh delay sector-1-a 200   add 200 ms latency (tc netem)
#   scripts/chaos.sh loss  sector-1-a 20    drop 20% of packets
#   scripts/chaos.sh clear sector-1-a       remove delay/loss
set -euo pipefail

GATEWAY=${GATEWAY:-http://localhost:8080}
NET=${NET:-atc_atc}

cid() { docker compose ps -aq "$1"; }
ts() { date +%H:%M:%S.%3N; }

leaders() {
  curl -fs "$GATEWAY/api/state" | python3 -c '
import json, sys
for c, nodes in sorted(json.load(sys.stdin)["clusters"].items()):
    lead = [n["id"] for n in nodes if n.get("up") and n.get("state") == "Leader"]
    down = [n["id"] for n in nodes if not n.get("up")]
    name = lead[0] if lead else "NONE"
    print(f"{c:10} leader={name:12} down={down}")'
}

leader_of() {
  curl -fs "$GATEWAY/api/state" | python3 -c '
import json, sys
nodes = json.load(sys.stdin)["clusters"][sys.argv[1]]
print(next(n["id"] for n in nodes if n.get("up") and n.get("state") == "Leader"))' "$1"
}

cmd=${1:-status}; target=${2:-}
case "$cmd" in
  status)      leaders ;;
  kill-leader) l=$(leader_of "$target"); echo "$(ts) killing leader $l of $target"; docker kill "$(cid "$l")" >/dev/null ;;
  kill)        echo "$(ts) kill $target";    docker kill "$(cid "$target")" >/dev/null ;;
  start)       echo "$(ts) start $target";   docker start "$(cid "$target")" >/dev/null ;;
  pause)       echo "$(ts) pause $target";   docker pause "$(cid "$target")" >/dev/null ;;
  unpause)     echo "$(ts) unpause $target"; docker unpause "$(cid "$target")" >/dev/null ;;
  partition)   echo "$(ts) partition $target"; docker network disconnect "$NET" "$(cid "$target")" ;;
  heal)        echo "$(ts) heal $target";    docker network connect --alias "$target" "$NET" "$(cid "$target")" ;;
  delay)       docker compose exec -T "$target" tc qdisc replace dev eth0 root netem delay "${3:-200}ms" ;;
  loss)        docker compose exec -T "$target" tc qdisc replace dev eth0 root netem loss "${3:-20}%" ;;
  clear)       docker compose exec -T "$target" tc qdisc del dev eth0 root || true ;;
  *) sed -n '2,16p' "$0"; exit 1 ;;
esac
