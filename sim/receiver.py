"""Ground receiver (placeholder).

Each receiver container stands for one ADS-B ground station at a fixed
position (RECEIVER_X, RECEIVER_Y in NM). The cross-check feature will have
receivers report the arrival time of each aircraft broadcast so the gateway
can multilaterate the true position and flag aircraft whose reported position
disagrees with it (spoofed / "ghost" aircraft).

For now it only announces itself so the container exists in the topology.
"""
import os
import time

RID = os.environ.get("RECEIVER_ID", "rx")
X = float(os.environ.get("RECEIVER_X", "0"))
Y = float(os.environ.get("RECEIVER_Y", "0"))

if __name__ == "__main__":
    print(f"receiver {RID} at ({X}, {Y}) NM: waiting for the cross-check protocol", flush=True)
    while True:
        time.sleep(3600)
