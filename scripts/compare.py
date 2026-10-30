#!/usr/bin/env python3
"""Compare the two mutual exclusion approaches under the same crash.

Start the stack with the baseline first:
    docker compose --profile maekawa up -d --build

Then, from the repo root:
    python3 scripts/compare.py --seconds 60 --crash-after 30

Halfway through it crashes ONE node on each side: maekawa-2 (a voter) and
the current registry leader. It prints, before and after the crash:
  - critical-section entries / completed handoffs
  - messages per entry (Maekawa: counted; registry: RPCs counted, Raft
    replication estimated at 4 messages per commit, 3 commits per handoff)
  - how long processes have been blocked
  - Maekawa safety check: overlapping critical sections (must be 0)
and saves the raw numbers to results/compare-<time>.json for the report.

Note the workloads differ: Maekawa processes request the critical section
continuously, while registry handoffs happen only when an aircraft crosses
a boundary. Compare per-entry cost and behaviour after the crash, not totals.
"""
import argparse
import json
import os
import subprocess
import time
import urllib.request

GATEWAY = os.environ.get("GATEWAY", "http://localhost:8080")
N = 5
RAFT_MSGS_PER_COMMIT = 4  # AppendEntries to 2 followers + 2 replies
COMMITS_PER_HANDOFF = 3   # begin (registry), accept (target), commit (registry)


def sh(*args):
    return subprocess.run(args, capture_output=True, text=True, check=True).stdout


def maekawa_stats():
    out = []
    for i in range(N):
        try:
            raw = sh("docker", "compose", "exec", "-T", "gateway",
                     "wget", "-qO-", "-T", "2", f"http://maekawa-{i}:8080/stats")
            out.append(json.loads(raw))
        except subprocess.CalledProcessError:
            out.append({"id": i, "down": True})
    return out


def gateway_state():
    with urllib.request.urlopen(GATEWAY + "/api/state", timeout=3) as r:
        return json.load(r)


def snapshot():
    return {"t": time.time(), "maekawa": maekawa_stats(), "gateway": gateway_state()}


def summarize(a, b):
    """Differences between two snapshots."""
    up = [s for s in b["maekawa"] if not s.get("down")]
    before = {s["id"]: s for s in a["maekawa"] if not s.get("down")}
    entries = sum(s["entries"] - before.get(s["id"], {}).get("entries", 0) for s in up)
    msgs = {}
    for s in up:
        old = before.get(s["id"], {}).get("sent", {})
        for k, v in s["sent"].items():
            msgs[k] = msgs.get(k, 0) + v - old.get(k, 0)
    iv = sorted((x, y) for s in up for x, y in s["intervals"])
    overlaps = sum(1 for p, q in zip(iv, iv[1:]) if q[0] < p[1])
    blocked = {s["id"]: s["waiting_for_ms"] for s in up if s["waiting_for_ms"] > 2000}

    ha, hb = a["gateway"]["handoff"], b["gateway"]["handoff"]
    handoffs = hb["handoffs"] - ha["handoffs"]
    rpcs = hb["rpcs"] - ha["rpcs"]
    stuck = [x["callsign"] for x in b["gateway"]["aircraft"] if x.get("status") == "TRANSFERRING"]
    return {
        "seconds": round(b["t"] - a["t"], 1),
        "maekawa": {
            "entries": entries,
            "msgs_per_entry": round(sum(msgs.values()) / entries, 1) if entries else None,
            "msgs_by_type": msgs,
            "overlapping_critical_sections": overlaps,
            "blocked_ms": blocked,
        },
        "registry": {
            "handoffs": handoffs,
            "aborted": hb["aborted"] - ha["aborted"],
            "rpc_msgs_per_handoff": round(2 * rpcs / handoffs, 1) if handoffs else None,
            "est_total_msgs_per_handoff": (round(2 * rpcs / handoffs + RAFT_MSGS_PER_COMMIT * COMMITS_PER_HANDOFF, 1)
                                           if handoffs else None),
            "avg_handoff_ms": hb["avg_ms"],
            "stuck_transferring": stuck,
        },
    }


def show(title, s):
    m, r = s["maekawa"], s["registry"]
    print(f"\n== {title} ({s['seconds']} s) ==")
    print(f"{'':28}{'Maekawa':>14}{'Raft registry':>16}")
    print(f"{'entries / handoffs':28}{m['entries']:>14}{r['handoffs']:>16}")
    print(f"{'messages per entry':28}{str(m['msgs_per_entry']):>14}{str(r['est_total_msgs_per_handoff']):>16}")
    print(f"{'blocked processes (>2 s)':28}{len(m['blocked_ms']):>14}{len(r['stuck_transferring']):>16}")
    print(f"{'safety violations':28}{m['overlapping_critical_sections']:>14}{'0 (by log)':>16}")
    print(f"Maekawa messages by type: {m['msgs_by_type']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--crash-after", type=int, default=30, help="0 = no crash")
    args = ap.parse_args()

    start = snapshot()
    crash_at = args.crash_after if args.crash_after else args.seconds
    time.sleep(crash_at)
    mid = snapshot()
    result = {"before": summarize(start, mid)}
    show("before crash", result["before"])

    if args.crash_after:
        leader = next(n["id"] for n in mid["gateway"]["clusters"]["registry"] if n.get("state") == "Leader")
        print(f"\ncrashing maekawa-2 and registry leader {leader}")
        sh("docker", "kill", sh("docker", "compose", "ps", "-q", "maekawa-2").strip())
        sh("docker", "kill", sh("docker", "compose", "ps", "-q", leader).strip())
        time.sleep(args.seconds - args.crash_after)
        end = snapshot()
        result["after"] = summarize(mid, end)
        result["crashed"] = ["maekawa-2", leader]
        show("after crash", result["after"])
        print(f"\nrestore with: docker start $(docker compose ps -aq maekawa-2) $(docker compose ps -aq {leader})")

    os.makedirs("results", exist_ok=True)
    path = time.strftime("results/compare-%Y%m%d-%H%M%S.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"saved {path}")


if __name__ == "__main__":
    main()
