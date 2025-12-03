#!/usr/bin/env python3
"""
Sweep over multiple experiment settings by calling run_pipeline.py.

You can tune the RATES / CORES / TRIGGERS / DURATION as needed.
"""

import subprocess
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
RUN_PIPELINE = BASE_DIR / "run_pipeline.py"

# ------------------ GRID CONFIG ------------------

# Seconds between producer events
RATES = [1.0, 0.5, 0.2]      # adjust as you like

# Number of cores for Spark local[*]
CORES = [1, 8]               # adjust as you like

# Trigger intervals
TRIGGERS = ["2 seconds"]     # e.g. ["1 second", "2 seconds", "5 seconds"]

# How long each experiment should run (seconds)
DURATION = 45

# Cool-down between experiments
COOLDOWN = 5


def run_one(rate: float, cores: int, trigger: str):
    trig_clean = trigger.replace(" ", "")
    exp_name = f"rate_{rate}_cores_{cores}_trig_{trig_clean}"

    cmd = [
        "python3",
        str(RUN_PIPELINE),
        "--rate",
        str(rate),
        "--spark-cores",
        str(cores),
        "--trigger",
        trigger,
        "--duration",
        str(DURATION),
        "--experiment-name",
        exp_name,
    ]

    print(f"\n{'=' * 80}")
    print(f"RUNNING EXPERIMENT: rate={rate}, cores={cores}, trigger={trigger}")
    print(f"{'=' * 80}")
    print(f"[Runner] Command: {' '.join(cmd)}\n", flush=True)

    subprocess.run(cmd, check=True)
    print(f"[Runner] Finished {exp_name}")
    print(f"[Runner] Cooling down {COOLDOWN} seconds...\n", flush=True)
    time.sleep(COOLDOWN)


def main():
    for rate in RATES:
        for cores in CORES:
            for trig in TRIGGERS:
                run_one(rate, cores, trig)

    print("\n[Runner] All experiments completed.")


if __name__ == "__main__":
    main()
