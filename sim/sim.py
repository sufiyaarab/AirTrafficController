"""Traffic simulator: flies N aircraft across a 200 x 200 NM airspace and
sends their positions to the gateway once per tick, as one batch.

Starting point only. Scenario files, conflict/stress modes and replay of
recorded traffic plug in where make_fleet() and step() are.

Config (environment):
  GATEWAY   base URL of the gateway       (default http://gateway:8080)
  AIRCRAFT  number of aircraft            (default 100)
  TICK      seconds between reports       (default 1.0)
  SEED      random seed, for repeatable runs
"""
import json
import math
import os
import random
import time
import urllib.error
import urllib.request

SIZE = 200.0  # NM

GATEWAY = os.environ.get("GATEWAY", "http://gateway:8080")
AIRCRAFT = int(os.environ.get("AIRCRAFT", "100"))
TICK = float(os.environ.get("TICK", "1.0"))
SEED = os.environ.get("SEED")


def make_aircraft(i, rng):
    return {
        "callsign": f"ATC{i:03d}",
        "x": rng.uniform(0, SIZE),
        "y": rng.uniform(0, SIZE),
        "alt": rng.choice(range(5000, 39001, 1000)),
        "heading": rng.uniform(0, 360),
        "speed": rng.uniform(250, 480),
    }


def make_fleet(rng):
    return [make_aircraft(i, rng) for i in range(AIRCRAFT)]


def step(ac, dt):
    ac["seq"] = ac.get("seq", 0) + 1  # per-aircraft sequence number: replicas use it for FIFO delivery
    nm = ac["speed"] * dt / 3600.0
    rad = math.radians(ac["heading"])
    ac["x"] = (ac["x"] + nm * math.sin(rad)) % SIZE  # wrap at the edge for now
    ac["y"] = (ac["y"] + nm * math.cos(rad)) % SIZE


def send(fleet):
    body = json.dumps(fleet).encode()
    req = urllib.request.Request(GATEWAY + "/api/positions", data=body,
                                 headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=2).read()


def main():
    rng = random.Random(SEED)
    fleet = make_fleet(rng)
    print(f"simulating {len(fleet)} aircraft, tick {TICK}s -> {GATEWAY}", flush=True)
    while True:
        start = time.monotonic()
        for ac in fleet:
            step(ac, TICK)
        try:
            send(fleet)
        except (urllib.error.URLError, OSError) as e:
            print("gateway unreachable:", e, flush=True)
        time.sleep(max(0.0, TICK - (time.monotonic() - start)))


if __name__ == "__main__":
    main()
