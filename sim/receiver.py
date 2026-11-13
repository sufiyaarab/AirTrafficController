"""Ground receiver: one ADS-B ground station at a fixed position.

Each tick the simulator POSTs to /broadcast the physical position of every
transmitter it is simulating:

    [{"callsign": "ATC001", "seq": 42, "emitter_x": 61.2, "emitter_y": 118.0}, ...]

For a real aircraft the emitter is where the aircraft is. For a spoofed
("ghost") aircraft it is the spoofer's ground station, while the position the
ghost claims to the gateway follows a fake track. The receiver only uses the
emitter position to compute what it would physically measure: the range to
the transmitter, plus Gaussian noise. It drops anything outside its coverage
and posts the ranges to the gateway:

    POST /api/observations
    {"receiver_id": "receiver-a",
     "observations": [{"callsign": "ATC001", "seq": 42, "range_nm": 104.9}, ...]}

It never sees the claimed position, and the gateway never sees the emitter
position, so the gateway's check is a real cross-check (cmd/gateway/verify.go).

An empty batch is sent at least once a second as a heartbeat, so the gateway
can tell a receiver that hears nothing from one that is down.

Environment: RECEIVER_ID, RECEIVER_X, RECEIVER_Y (NM), RECEIVER_RANGE_NM
(coverage radius, default 200), RECEIVER_NOISE_NM (sigma, default 0.5),
RECEIVER_PORT (default 9000), GATEWAY.
"""
import json
import math
import os
import queue
import random
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RID = os.environ.get("RECEIVER_ID", "rx")
X = float(os.environ.get("RECEIVER_X", "0"))
Y = float(os.environ.get("RECEIVER_Y", "0"))
RANGE = float(os.environ.get("RECEIVER_RANGE_NM", "200"))
NOISE = float(os.environ.get("RECEIVER_NOISE_NM", "0.5"))
PORT = int(os.environ.get("RECEIVER_PORT", "9000"))
GATEWAY = os.environ.get("GATEWAY", "http://gateway:8080")
HEARTBEAT = 1.0  # seconds

rng = random.Random(os.environ.get("SEED"))
outbox = queue.Queue(maxsize=16)


def measure(broadcast):
    """Turn one simulator broadcast into this receiver's range observations."""
    out = []
    for b in broadcast:
        try:
            cs, seq = str(b["callsign"]), int(b["seq"])
            d = math.hypot(float(b["emitter_x"]) - X, float(b["emitter_y"]) - Y)
        except (KeyError, TypeError, ValueError):
            continue
        if d > RANGE:
            continue  # out of coverage: this station hears nothing
        out.append({"callsign": cs, "seq": seq, "range_nm": round(max(0.0, d + rng.gauss(0, NOISE)), 3)})
    return out


def post(observations):
    body = json.dumps({"receiver_id": RID, "observations": observations}).encode()
    req = urllib.request.Request(
        GATEWAY + "/api/observations", data=body, headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req, timeout=2).read()


def sender():
    """Single sender so batches reach the gateway in order; heartbeats when idle."""
    while True:
        try:
            obs = outbox.get(timeout=HEARTBEAT)
        except queue.Empty:
            obs = []
        try:
            post(obs)
        except (urllib.error.URLError, OSError) as error:
            print(f"{RID}: gateway unreachable: {error}", flush=True)
            time.sleep(0.5)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/broadcast":
            self.send_error(404)
            return
        try:
            n = int(self.headers.get("Content-Length", "0"))
            broadcast = json.loads(self.rfile.read(n))
            if not isinstance(broadcast, list):
                raise ValueError("want a list")
        except ValueError as error:
            self.send_error(400, str(error))
            return
        self.send_response(202)
        self.end_headers()
        try:
            outbox.put_nowait(measure(broadcast))
        except queue.Full:
            print(f"{RID}: gateway too slow, dropped a tick", flush=True)

    def do_GET(self):
        if self.path != "/healthz":
            self.send_error(404)
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass  # one line per tick would drown the logs


if __name__ == "__main__":
    threading.Thread(target=sender, daemon=True).start()
    print(f"receiver {RID} at ({X}, {Y}) NM, range {RANGE} NM, noise {NOISE} NM, port {PORT}", flush=True)
    ThreadingHTTPServer(("", PORT), Handler).serve_forever()
