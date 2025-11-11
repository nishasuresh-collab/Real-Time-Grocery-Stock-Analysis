import os
import re
from datetime import datetime
import matplotlib.pyplot as plt

METRICS_LOG_FILE = "spark_metrics_logs/batch_metrics.log"
OUTPUT_PLOT_DIR = "spark_metrics_plots"

# Example line parsed by regex below:
# 2025-11-10 21:41:33 | Batch 0 | Detections: 10 | AvgConf: 0.9995 | AvgLatencyMs: 269588500.0 | UniqueProducts: 1
LINE_RE = re.compile(
    r"""^(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+\|\s+Batch\s+(?P<batch>\d+)\s+\|
        \s*Detections:\s*(?P<det>\d+)\s+\|\s*
        AvgConf:\s*(?P<conf>[0-9]*\.?[0-9]+)\s+\|\s*
        AvgLatencyMs:\s*(?P<lat>[0-9]*\.?[0-9]+)\s+\|\s*
        UniqueProducts:\s*(?P<uniq>\d+)\s*$
    """,
    re.X,
)

def parse_metrics_log(log_file: str):
    """Parse the batch metrics log file into dict of lists."""
    if not os.path.exists(log_file):
        print(f"[Error] Log file not found: {log_file}")
        return None

    timestamps = []
    batch_ids = []
    detections = []
    confidences = []
    latencies_ms = []
    unique_products = []

    with open(log_file, "r") as f:
        for raw in f:
            line = raw.strip()
            # skip the header and separator lines
            if not line or line.startswith("Stream started") or set(line) == {"="}:
                continue

            m = LINE_RE.match(line)
            if not m:
                # silently skip non-matching lines
                continue

            ts = datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S")
            timestamps.append(ts)
            batch_ids.append(int(m.group("batch")))
            detections.append(int(m.group("det")))
            confidences.append(float(m.group("conf")))
            latencies_ms.append(float(m.group("lat")))
            unique_products.append(int(m.group("uniq")))

    if not batch_ids:
        print("[Warning] No parsed batch entries found.")
        return None

    return {
        "timestamps": timestamps,
        "batch_ids": batch_ids,
        "detections": detections,
        "confidences": confidences,
        "latencies_ms": latencies_ms,
        "unique_products": unique_products,
    }

def _save_line_plot(x, y, xlabel, ylabel, title, outpath):
    plt.figure(figsize=(10, 4.5))
    plt.plot(x, y, marker="o")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def _save_hist(y, bins, xlabel, title, outpath):
    plt.figure(figsize=(10, 4.5))
    plt.hist(y, bins=bins)
    plt.xlabel(xlabel)
    plt.ylabel("Count")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def create_visualizations(metrics):
    """Create and save visualizations for detections, confidence, and latency."""
    os.makedirs(OUTPUT_PLOT_DIR, exist_ok=True)

    batches = metrics["batch_ids"]
    times = metrics["timestamps"]
    dets = metrics["detections"]
    confs = metrics["confidences"]
    lats_ms = metrics["latencies_ms"]
    uniq = metrics["unique_products"]

    # Quick latency stats
    try:
        import numpy as np
        lat_arr = np.array(lats_ms, dtype=float)
        stats = {
            "count": lat_arr.size,
            "mean_ms": float(lat_arr.mean()),
            "p50_ms": float(np.percentile(lat_arr, 50)),
            "p90_ms": float(np.percentile(lat_arr, 90)),
            "p95_ms": float(np.percentile(lat_arr, 95)),
            "max_ms": float(lat_arr.max()),
        }
        print("\n[Latency Stats]")
        for k, v in stats.items():
            print(f"  {k}: {v:,.2f}")
    except Exception as e:
        print(f"[Warn] Could not compute latency stats: {e}")

    # Plots by batch id
    _save_line_plot(
        batches, dets, "Batch ID", "Detections",
        "Detections per Batch",
        os.path.join(OUTPUT_PLOT_DIR, "detections_per_batch.png")
    )

    _save_line_plot(
        batches, confs, "Batch ID", "Avg Confidence",
        "Average Confidence per Batch",
        os.path.join(OUTPUT_PLOT_DIR, "avg_conf_per_batch.png")
    )

    _save_line_plot(
        batches, lats_ms, "Batch ID", "Avg Latency (ms)",
        "Average End-to-End Latency per Batch (ms)",
        os.path.join(OUTPUT_PLOT_DIR, "avg_latency_ms_per_batch.png")
    )

    # Time-based latency plot (if timestamps present)
    if times and len(times) == len(lats_ms):
        _save_line_plot(
            times, lats_ms, "Time", "Avg Latency (ms)",
            "Average End-to-End Latency over Time (ms)",
            os.path.join(OUTPUT_PLOT_DIR, "avg_latency_ms_over_time.png")
        )

    # Histogram of latency
    _save_hist(
        lats_ms, bins=10, xlabel="Avg Latency (ms)",
        title="Distribution of Average End-to-End Latency (per batch)",
        outpath=os.path.join(OUTPUT_PLOT_DIR, "avg_latency_ms_histogram.png")
    )

    # Top-N slowest batches (by avg latency)
    pairs = sorted(zip(batches, lats_ms), key=lambda t: t[1], reverse=True)
    top_n = min(5, len(pairs))
    print("\n[Top Slow Batches by AvgLatencyMs]")
    for i in range(top_n):
        b, l = pairs[i]
        print(f"  Batch {b}: {l:,.2f} ms")

    print(f"\n[Done] Plots saved to: {os.path.abspath(OUTPUT_PLOT_DIR)}")

def main():
    print("\n" + "="*60)
    print("PySpark Streaming Metrics Visualizer")
    print("="*60 + "\n")

    print(f"[Info] Reading metrics from: {METRICS_LOG_FILE}")
    metrics = parse_metrics_log(METRICS_LOG_FILE)
    if metrics:
        create_visualizations(metrics)
    else:
        print("[Error] Failed to parse metrics. Make sure the streaming pipeline has run.")

if __name__ == "__main__":
    main()
