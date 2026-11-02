
"""Save and replay aircraft simulation frames using JSON Lines."""

import json
from pathlib import Path


def record_frame(file, tick, aircraft):
    """Save one simulation tick to an open recording file."""
    frame = {
        "tick": tick,
        "aircraft": aircraft,
    }

    file.write(json.dumps(frame, allow_nan=False) + "\n")
    file.flush()


def replay_frames(filename):
    """Read recorded aircraft frames in their original order."""
    path = Path(filename)
    previous_tick = 0

    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                frame = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON on line {line_number}"
                ) from error

            if not isinstance(frame, dict):
                raise ValueError(
                    f"Invalid frame on line {line_number}"
                )

            tick = frame.get("tick")
            aircraft = frame.get("aircraft")

            if type(tick) is not int or tick <= previous_tick:
                raise ValueError(
                    f"Invalid tick on line {line_number}"
                )

            if not isinstance(aircraft, list):
                raise ValueError(
                    f"Missing aircraft list on line {line_number}"
                )

            if any(not isinstance(ac, dict) for ac in aircraft):
                raise ValueError(
                    f"Invalid aircraft on line {line_number}"
                )

            previous_tick = tick
            yield frame
