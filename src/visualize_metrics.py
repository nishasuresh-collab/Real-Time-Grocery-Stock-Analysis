#!/usr/bin/env python3
"""
visualize_metrics.py

Parse batch_metrics.log lines like:
2025-11-10 21:41:33 | Batch 0 | Detections: 10 | AvgConf: 0.9995 | AvgLatencyMs: 269588500.0 | UniqueProducts: 1

…and render separate charts for:
- Detections per batch
- Average confidence
- Average latency (seconds) w/ optional threshold line and p95 annotation
- Unique products per batch

Usage:
  python visualize_metrics.py --log ./spark_metrics_logs/batch_metrics.log --save ./charts
  python visualize_metrics.py --log ./spark_metrics_logs/batch_metrics.log

Notes:
- If --save is provided, PNG files are saved there; otherwise charts show interactively.
- Latency is plotted in seconds (AvgLatencyMs / 1000).
"""

import argparse
import os
import re
from datetime import datetime
from typing import List, Optional, Tuple

import matplotlib.pyplot as plt

try:
    import pandas as pd
except ImportError:
    raise SystemExit("Please `pip install pandas matplotlib` to use this visualizer.")

LINE_RE = re.compile(
    r"""
    ^\s*
    (?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})     # timestamp
    \s*\|\s*Batch\s+(?P<batch>\d+)                    # Batch N
    \s*\|\s*Detections:\s*(?P<det>\d+)               # Detections: X
    \s*\|\s*AvgConf:\s*(?P<conf>[0-9.]+)             # AvgConf: float
    \s*\|\s*AvgLatencyMs:\s*(?P<lat_ms>[0-9.]+)      # AvgLatencyMs: float
    \s*\|\s*UniqueProducts:\s*(?P<uniq>\d+)          # UniqueProducts: Y
    \s*$
    """,
    re.VERBOSE,
)

HEADER_RE = re.compile(r"^\s*Stream started at\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s*$")


def parse_log(path: str) -> "pd.DataFrame":
    """Parse the metrics log into a DataFrame."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Log file not found: {path}")

    records: List[Tuple[datetime, int, int, float, float, int]] = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or HEADER_RE.match(line) or line.startswith("="):
                continue
            m = LINE_RE.match(line)
            if not m:
                # Skip lines that don't match the exact metric format
                continue
            ts = datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S")
            batch = int(m.group("batch"))
            det = int(m.group("det"))
            conf = float(m.group("conf"))
            lat_ms = float(m.group("lat_ms"))
            uniq = int(m.group("uniq"))
            records.append((ts, batch, det, conf, lat_ms, uniq))

    if not records:
        raise ValueError("No metric lines parsed. Check the log format or path.")

    df = pd.DataFrame(
        records,
        columns=["timestamp", "batch", "detections", "avg_conf", "avg_latency_ms", "unique_products"],
    ).sort_values("batch", kind="mergesort")

    # Helpful derivative columns
    df["avg_latency_sec"] = df["avg_latency_ms"] / 1000.0
    return df.reset_index(drop=True)


def _ensure_outdir(outdir: Optional[str]) -> None:
    if outdir:
        os.makedirs(outdir, exist_ok=True)


def _save_or_show(fig: "plt.Figure", outdir: Optional[str], name: str) -> None:
    if outdir:
        fp = os.path.join(outdir, f"{name}.png")
        fig.savefig(fp, bbox_inches="tight", dpi=150)
        plt.close(fig)
        print(f"Saved: {fp}")
    else:
        fig.show()


def plot_detections(df: "pd.DataFrame", outdir: Optional[str]) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(df["batch"], df["detections"], marker="o")
    ax.set_title("Detections per Batch")
    ax.set_xlabel("Batch")
    ax.set_ylabel("Detections")
    ax.grid(True, linestyle="--", alpha=0.4)
    _save_or_show(fig, outdir, "detections_per_batch")


def plot_confidence(df: "pd.DataFrame", outdir: Optional[str]) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(df["batch"], df["avg_conf"], marker="o")
    ax.set_title("Average Confidence per Batch")
    ax.set_xlabel("Batch")
    ax.set_ylabel("Avg Confidence")
    ax.set_ylim(0.0, 1.0)
    ax.grid(True, linestyle="--", alpha=0.4)
    _save_or_show(fig, outdir, "avg_confidence_per_batch")


def _quantile(series: "pd.Series", q: float) -> float:
    try:
        return float(series.quantile(q))
    except Exception:
        return float("nan")


def plot_latency(
    df: "pd.DataFrame",
    outdir: Optional[str],
    threshold_sec: Optional[float] = None,
    annotate_p95: bool = True,
) -> None:
    fig, ax = plt.subplots(figsize=(9, 4.8))
    ax.plot(df["batch"], df["avg_latency_sec"], marker="o")
    ax.set_title("Average E2E Latency per Batch (seconds)")
    ax.set_xlabel("Batch")
    ax.set_ylabel("Avg Latency (sec)")
    ax.grid(True, linestyle="--", alpha=0.4)

    # Optional threshold line
    if threshold_sec and threshold_sec > 0:
        ax.axhline(threshold_sec, linestyle="--", linewidth=1.5, label=f"Threshold: {threshold_sec:.2f}s")

    # Annotate p95 latency for quick health view
    if annotate_p95:
        p95 = _quantile(df["avg_latency_sec"], 0.95)
        if p95 == p95:  # not NaN
            ax.axhline(p95, linestyle=":", linewidth=1.5, label=f"p95: {p95:.2f}s")

    # Highlight points above threshold (if provided)
    if threshold_sec and threshold_sec > 0:
        over = df[df["avg_latency_sec"] > threshold_sec]
        if not over.empty:
            ax.scatter(over["batch"], over["avg_latency_sec"], s=60, marker="x", label="Above threshold")

    if ax.get_legend_handles_labels()[0]:
        ax.legend()

    _save_or_show(fig, outdir, "avg_latency_seconds")


def plot_unique_products(df: "pd.DataFrame", outdir: Optional[str]) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar(df["batch"], df["unique_products"])
    ax.set_title("Unique Products per Batch")
    ax.set_xlabel("Batch")
    ax.set_ylabel("Unique Products")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    _save_or_show(fig, outdir, "unique_products_per_batch")


def main():
    parser = argparse.ArgumentParser(description="Visualize Spark streaming batch metrics.")
    parser.add_argument(
        "--log",
        required=True,
        help="Path to batch_metrics.log (e.g., ./spark_metrics_logs/batch_metrics.log)",
    )
    parser.add_argument(
        "--save",
        default=None,
        help="Directory to save PNGs. If omitted, figures display interactively.",
    )
    parser.add_argument(
        "--latency-threshold-sec",
        type=float,
        default=None,
        help="Optional latency threshold in seconds to draw on the latency chart.",
    )
    args = parser.parse_args()

    _ensure_outdir(args.save)
    df = parse_log(args.log)

    # Basic console summary
    print("\nParsed metrics:")
    print(df.tail().to_string(index=False))
    print(
        f"\nBatches: {df['batch'].nunique()} | "
        f"Detections total: {int(df['detections'].sum())} | "
        f"Avg conf (mean): {df['avg_conf'].mean():.4f} | "
        f"Latency p50/p95 (sec): {df['avg_latency_sec'].median():.2f}/{_quantile(df['avg_latency_sec'], 0.95):.2f} | "
        f"Unique products max: {int(df['unique_products'].max())}"
    )

    # Plots
    plot_detections(df, args.save)
    plot_confidence(df, args.save)
    plot_latency(df, args.save, threshold_sec=args.latency_threshold_sec)
    plot_unique_products(df, args.save)

    if not args.save:
        # Keep windows open when run from double-click, etc.
        print("\nClose the plot windows to exit.")
        plt.show()


if __name__ == "__main__":
    main()
