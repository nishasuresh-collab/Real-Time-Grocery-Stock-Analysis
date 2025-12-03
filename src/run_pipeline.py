#!/usr/bin/env python3
"""
Run one end-to-end experiment:

- Starts the GroZi producer (realtime_grozi_pipeline.py) to generate JSON detections.
- Starts the PySpark streaming job (pyspark_stream_processor.py) via spark-submit.
- Each run writes into its own folder under ../experiments/.

Designed to work on WSL with paths under /mnt/c/...
"""

import argparse
import subprocess
import time
from datetime import datetime
from pathlib import Path
import sys
import os


# -------------- PATHS --------------

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

# Scripts
PRODUCER_SCRIPT = BASE_DIR / "realtime_grozi_pipeline.py"
SPARK_SCRIPT = BASE_DIR / "pyspark_stream_processor.py"

# Experiments root
EXPERIMENTS_ROOT = PROJECT_ROOT / "experiments"
EXPERIMENTS_ROOT.mkdir(parents=True, exist_ok=True)


# -------------- HELPERS --------------

def log(msg: str):
    """Print nicely and flush (so logs appear immediately)."""
    print(f"[Pipeline] {msg}", flush=True)


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def make_experiment_dir(args: argparse.Namespace) -> Path:
    """
    Create a unique experiment directory:

    ../experiments/<experiment_name>_<timestamp>/
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.experiment_name:
        base_name = args.experiment_name
    else:
        trig_sanitized = args.trigger.replace(" ", "")
        base_name = f"rate_{args.rate}_cores_{args.spark_cores}_trig_{trig_sanitized}"

    exp_dir = EXPERIMENTS_ROOT / f"{base_name}_{ts}"
    ensure_dir(exp_dir)
    return exp_dir


def build_producer_cmd(args, json_dir):
    cmd = [
        "python3",
        "-m",
        "src.realtime_grozi_pipeline",
        "--output-dir",
        str(json_dir),
        "--duration",
        str(args.duration),
        "--rate",
        str(args.rate),
    ]
    return cmd



def build_spark_cmd(
    args: argparse.Namespace,
    json_dir: Path,
    checkpoint_dir: Path,
    batch_metrics_path: Path,
) -> list[str]:
    """
    Build the spark-submit command that runs pyspark_stream_processor.py.
    """
    cmd = [
        "spark-submit",
        "--master",
        f"local[{args.spark_cores}]",
        str(SPARK_SCRIPT),
        "--input-dir",
        str(json_dir),
        "--checkpoint-dir",
        str(checkpoint_dir),
        "--metrics-log",
        str(batch_metrics_path),
        "--trigger",
        args.trigger,
        "--duration",
        str(args.duration),
    ]
    return cmd


def start_process(cmd: list[str], logfile: Path) -> subprocess.Popen:
    """Start a subprocess and tee stdout/stderr to a logfile."""
    log(f"Starting process: {' '.join(cmd)}")
    log(f"Logging to: {logfile}")

    logfile.parent.mkdir(parents=True, exist_ok=True)
    f = open(logfile, "w", encoding="utf-8")

    # MUST run from project root, not src/
    proc = subprocess.Popen(
        cmd,
        stdout=f,
        stderr=subprocess.STDOUT,
        cwd=str(PROJECT_ROOT),   # <-- FIXED
    )

    return proc


def stop_process(proc: subprocess.Popen, name: str, timeout: float = 10.0):
    """Gracefully terminate a process, then kill if needed."""
    if proc is None:
        return
    if proc.poll() is not None:
        return

    log(f"Stopping {name}...")
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
        log(f"{name} stopped gracefully.")
    except subprocess.TimeoutExpired:
        log(f"{name} did not stop in time; killing.")
        proc.kill()
        proc.wait(timeout=5)


# -------------- MAIN --------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run a single GroZi streaming experiment.")
    p.add_argument("--rate", type=float, required=True,
                   help="Seconds between images in the producer (e.g., 1.0)")
    p.add_argument("--spark-cores", type=int, default=4,
                   help="Number of cores for Spark local[*].")
    p.add_argument("--trigger", type=str, default="2 seconds",
                   help='Structured Streaming trigger interval, e.g. "2 seconds".')
    p.add_argument("--duration", type=int, default=45,
                   help="Total run time in seconds.")
    p.add_argument("--experiment-name", type=str, default=None,
                   help="Optional base name for the experiment folder.")
    return p.parse_args()


def main():
    args = parse_args()

    # 1) Create experiment folder structure
    exp_dir = make_experiment_dir(args)
    json_dir = ensure_dir(exp_dir / "json_stream")
    checkpoint_dir = ensure_dir(exp_dir / "spark_checkpoint")
    logs_dir = ensure_dir(exp_dir / "logs")
    batch_metrics_path = exp_dir / "batch_metrics.log"

    log(f"Experiment directory: {exp_dir}")
    log(f"JSON stream directory: {json_dir}")
    log(f"Spark checkpoint directory: {checkpoint_dir}")
    log(f"Batch metrics: {batch_metrics_path}")

    # 2) Build commands
    producer_cmd = build_producer_cmd(args, json_dir)
    spark_cmd = build_spark_cmd(args, json_dir, checkpoint_dir, batch_metrics_path)

    # 3) Start both processes
    producer_proc = None
    spark_proc = None

    try:
        producer_proc = start_process(producer_cmd, logs_dir / "producer.log")
        # small delay so producer starts writing JSONs before Spark connects
        time.sleep(3)

        spark_proc = start_process(spark_cmd, logs_dir / "spark.log")

        log(f"Pipeline running for {args.duration} seconds...")
        time.sleep(args.duration)

    except KeyboardInterrupt:
        log("KeyboardInterrupt received; stopping pipeline early.")
    finally:
        stop_process(producer_proc, "Producer")
        stop_process(spark_proc, "Spark")
        log("All processes stopped.")
        log(f"Results available in: {exp_dir}")


if __name__ == "__main__":
    main()
