import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict

# -------------------------------------------------------------
# LOAD METRICS
# -------------------------------------------------------------
def load_batch_metrics(metrics_path):
    if not os.path.exists(metrics_path):
        return None

    rows = []
    with open(metrics_path, "r") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("batch_id"):
                continue

            parts = line.split(",")

            # EXPECTED format:
            # 0: batch_id
            # 1: timestamp (ignored)
            # 2: num_rows
            # 3: unique_products
            # 4: avg_conf (optional)
            # 5: avg_latency_ms (optional)
            # 6: processing_time_ms

            if len(parts) != 7:
                continue

            try:
                batch_id = int(parts[0])
                num_rows = float(parts[2])
                unique_products = float(parts[3])
                avg_conf = float(parts[4]) if parts[4] else None
                avg_latency = float(parts[5]) if parts[5] else None
                proc_time = float(parts[6])

                rows.append({
                    "batch_id": batch_id,
                    "num_rows": num_rows,
                    "unique_products": unique_products,
                    "avg_conf": avg_conf,
                    "latency": avg_latency,
                    "processing_time": proc_time,
                })
            except ValueError:
                continue

    if not rows:
        return None

    df = pd.DataFrame(rows)

    # ---- COMPUTE THROUGHPUT (rows/sec) ----
    df["throughput"] = df["num_rows"] / (df["processing_time"] / 1000)

    return df


# -------------------------------------------------------------
# PLOTTING HELPERS
# -------------------------------------------------------------
def plot_line(df, x, y, title, xlabel, ylabel, savepath):
    plt.figure(figsize=(12, 6))
    plt.plot(df[x], df[y], marker='o')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(savepath, dpi=120)
    plt.close()


def plot_bars(categories, values, title, xlabel, ylabel, savepath):
    plt.figure(figsize=(10, 6))
    bars = plt.bar(categories, values, color='steelblue', edgecolor='black')

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, axis='y', alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                 f'{height:.2f}', ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    plt.savefig(savepath, dpi=120)
    plt.close()


# -------------------------------------------------------------
# PER-EXPERIMENT PLOTS
# -------------------------------------------------------------
def plot_experiment(exp_name, exp_path, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    df = load_batch_metrics(os.path.join(exp_path, "batch_metrics.log"))
    if df is None:
        return None

    # ---- Throughput ----
    plot_line(
        df, "batch_id", "throughput",
        f"Throughput Per Batch - {exp_name}",
        "Batch ID", "Throughput (rows/sec)",
        os.path.join(out_dir, "throughput.png")
    )

    # ---- Processing Time ----
    plot_line(
        df, "batch_id", "processing_time",
        f"Processing Time Per Batch - {exp_name}",
        "Batch ID", "Processing Time (ms)",
        os.path.join(out_dir, "processing_time.png")
    )

    # ---- Summary Stats ----
    stats = {
        "Total Rows": df["num_rows"].sum(),
        "Avg Throughput": df["throughput"].mean(),
        "Avg Proc Time (ms)": df["processing_time"].mean(),
        "Avg Unique Products": df["unique_products"].mean(),
    }

    plt.figure(figsize=(10, 6))
    plt.bar(stats.keys(), stats.values(), color=['blue', 'green', 'orange', 'purple'])
    plt.title(f"Summary Stats - {exp_name}")
    plt.ylabel("Value")
    plt.grid(True, axis='y')

    for i, (k, v) in enumerate(stats.items()):
        plt.text(i, v, f"{v:.2f}", ha='center', va='bottom')

    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "summary_stats.png"))
    plt.close()

    return df


# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exp-dir", required=True)
    parser.add_argument("--plots-dir", default="plots")
    parser.add_argument("--date", default=None)
    parser.add_argument("--latest", action="store_true")
    args = parser.parse_args()

    exp_root = args.exp_dir
    all_dirs = sorted(d for d in os.listdir(exp_root)
                      if os.path.isdir(os.path.join(exp_root, d)))

    # ---- filter by date ----
    if args.date:
        exp_list = [d for d in all_dirs if args.date in d]
    else:
        exp_list = all_dirs

    # ---- filter latest ----
    if args.latest:
        exp_map = {}
        for name in exp_list:
            parts = name.split("_")
            for p in parts:
                if len(p) == 8 and p.isdigit():
                    prefix = name[:name.index(p)-1]
                    exp_map.setdefault(prefix, []).append(name)

        exp_list = [max(v) for v in exp_map.values()]

    print(f"[INFO] Found {len(exp_list)} experiments\n")

    all_results = []

    for exp_name in exp_list:
        print(f"Processing: {exp_name}")

        exp_path = os.path.join(exp_root, exp_name)
        df = plot_experiment(exp_name, exp_path,
                             os.path.join(args.plots_dir, exp_name))

        if df is not None:
            all_results.append((exp_name, df))
            print(f"  [✓] Done")
        else:
            print(f"  [✗] Metrics missing")

    # ---- Comparison plots ----
    comp_dir = os.path.join(args.plots_dir, "comparisons")
    os.makedirs(comp_dir, exist_ok=True)

    rate_data = defaultdict(list)
    cores_data = defaultdict(list)

    for exp_name, df in all_results:
        parts = exp_name.split("_")

        if parts[0] == "rate":
            rate_data[parts[1]].append(df["throughput"].mean())

        if parts[2] == "cores":
            cores_data[parts[3]].append(df["throughput"].mean())

    # rate plot
    if rate_data:
        rates = sorted(rate_data.keys(), key=float)
        vals = [sum(rate_data[r]) / len(rate_data[r]) for r in rates]
        plot_bars(rates, vals,
                  "Throughput vs Producer Rate",
                  "Rate (msgs/sec)",
                  "Throughput (rows/sec)",
                  os.path.join(comp_dir, "rate_throughput.png"))

    # cores plot
    if cores_data:
        cores = sorted(cores_data.keys(), key=int)
        vals = [sum(cores_data[c]) / len(cores_data[c]) for c in cores]
        plot_bars(cores, vals,
                  "Throughput vs Spark Cores",
                  "Cores",
                  "Throughput (rows/sec)",
                  os.path.join(comp_dir, "cores_throughput.png"))

    print(f"\n[✓] Comparison plots saved to {comp_dir}")
    print("All done!")


if __name__ == "__main__":
    main()