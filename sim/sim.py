
"""Aircraft simulator with JSON scenarios, recording, and replay."""

import json
import math
import os
import random
import time
import urllib.error
import urllib.request
from pathlib import Path

from recording import record_frame, replay_frames

SIZE = 200.0  # Airspace width and height in nautical miles

GATEWAY = os.environ.get("GATEWAY", "http://gateway:8080")
AIRCRAFT = int(os.environ.get("AIRCRAFT", "100"))
TICK = float(os.environ.get("TICK", "1.0"))
SEED = os.environ.get("SEED")
SCENARIO = os.environ.get("SCENARIO", "").strip()

# Simulation modes: live, record, replay
MODE = os.environ.get("MODE", "live").strip().lower()
RECORD_FILE = os.environ.get("RECORD_FILE", "recordings/traffic.jsonl")
REPLAY_FILE = os.environ.get("REPLAY_FILE", "recordings/traffic.jsonl")

AIRCRAFT_FIELDS = ("callsign", "x", "y", "alt", "heading", "speed")


def resolve_path(filename):
    """Resolve relative files inside the simulator's /app directory."""
    path = Path(filename)
    return path if path.is_absolute() else Path(__file__).resolve().parent / path


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


def load_scenario(filename):
    """Load and validate aircraft from a JSON scenario file."""
    path = resolve_path(filename)

    with path.open("r", encoding="utf-8") as file:
        scenario = json.load(file)

    if not isinstance(scenario, dict) or not isinstance(scenario.get("aircraft"), list):
        raise ValueError("Scenario JSON must contain an 'aircraft' list")
    if not scenario["aircraft"]:
        raise ValueError("Scenario must contain at least one aircraft")

    fleet = []
    callsigns = set()

    for i, aircraft in enumerate(scenario["aircraft"], start=1):
        if not isinstance(aircraft, dict) or any(field not in aircraft for field in AIRCRAFT_FIELDS):
            raise ValueError(f"Aircraft {i} is missing required fields")

        callsign = aircraft["callsign"]
        if not isinstance(callsign, str) or not callsign.strip():
            raise ValueError(f"Aircraft {i} needs a valid callsign")
        if callsign in callsigns:
            raise ValueError(f"Duplicate callsign: {callsign}")
        callsigns.add(callsign)

        for field in AIRCRAFT_FIELDS[1:]:
            value = aircraft[field]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f"{callsign}: '{field}' must be a finite number")

        if not (0 <= aircraft["x"] < SIZE and 0 <= aircraft["y"] < SIZE):
            raise ValueError(f"{callsign}: x and y must be within the airspace")
        if aircraft["alt"] <= 0 or aircraft["speed"] < 0:
            raise ValueError(f"{callsign}: invalid altitude or speed")
        if not 0 <= aircraft["heading"] < 360:
            raise ValueError(f"{callsign}: heading must be from 0 to below 360")

        fleet.append({field: aircraft[field] for field in AIRCRAFT_FIELDS})

    print(f"loaded scenario '{scenario.get('name', path.name)}' with {len(fleet)} aircraft", flush=True)
    return fleet


def step(ac, dt):
    """Advance aircraft and increase its FIFO sequence number."""
    ac["seq"] = ac.get("seq", 0) + 1
    nm = ac["speed"] * dt / 3600.0
    rad = math.radians(ac["heading"])
    ac["x"] = (ac["x"] + nm * math.sin(rad)) % SIZE
    ac["y"] = (ac["y"] + nm * math.cos(rad)) % SIZE


def send(fleet):
    body = json.dumps(fleet).encode()
    req = urllib.request.Request(
        GATEWAY + "/api/positions",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=2).read()


def send_safely(fleet):
    try:
        send(fleet)
    except (urllib.error.URLError, OSError) as error:
        print("gateway unreachable:", error, flush=True)


def run_live():
    """Generate positions and optionally record every generated frame."""
    rng = random.Random(SEED)

    if SCENARIO:
        try:
            fleet = load_scenario(SCENARIO)
        except (OSError, ValueError) as error:
            raise SystemExit(f"Unable to load scenario: {error}") from error
    else:
        fleet = make_fleet(rng)

    print(f"simulating {len(fleet)} aircraft, tick {TICK}s -> {GATEWAY}", flush=True)

    recorder = None

    try:
        if MODE == "record":
            path = resolve_path(RECORD_FILE)
            path.parent.mkdir(parents=True, exist_ok=True)
            recorder = path.open("w", encoding="utf-8")
            print(f"recording aircraft frames to {path}", flush=True)

        tick_number = 0

        while True:
            start = time.monotonic()

            for ac in fleet:
                step(ac, TICK)

            tick_number += 1

            if recorder is not None:
                record_frame(recorder, tick_number, fleet)

            send_safely(fleet)

            time.sleep(max(0.0, TICK - (time.monotonic() - start)))

    finally:
        if recorder is not None:
            recorder.close()


def run_replay():
    """Send recorded frames without recalculating aircraft movement."""
    path = resolve_path(REPLAY_FILE)
    print(f"replaying aircraft frames from {path}", flush=True)

    frame_count = 0

    try:
        for frame in replay_frames(path):
            start = time.monotonic()

            send_safely(frame["aircraft"])
            frame_count += 1

            time.sleep(max(0.0, TICK - (time.monotonic() - start)))

    except (OSError, ValueError) as error:
        raise SystemExit(f"Unable to replay recording: {error}") from error

    print(f"replay complete: {frame_count} frames sent", flush=True)


def main():
    if TICK <= 0 or not math.isfinite(TICK):
        raise SystemExit("TICK must be a positive, finite number")

    if MODE not in ("live", "record", "replay"):
        raise SystemExit("MODE must be live, record, or replay")

    if MODE == "replay":
        run_replay()
    else:
        run_live()


if __name__ == "__main__":
    main()
