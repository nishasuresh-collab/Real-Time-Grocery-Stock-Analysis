import subprocess
import time
import os
import csv
import shutil
import matplotlib.pyplot as plt

FEED_COUNTS = [1, 3, 5, 10] # to test scalability by sinding in multiple feeds
RUN_DURATION = 120 # seconds allocated to run each scalability test
RESULT_DIR = "scalability_results"
LOG_FILE = "src/spark_metrics_logs/batch_metrics.log"

os.makedirs(RESULT_DIR, exist_ok=True)

def clean_spark_dirs():
    # Remove Spark logs and checkpoints before each run
    print("Removing Spark logs and checkpoints...")
    paths = [
        "src/spark_checkpoint",
        "src/spark_metrics_logs",
        "stream_output_json"
    ]
    for path in paths:
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"[Cleanup] Removed: {path}")


def run_yolo_pipeline(num_feeds):
    # Start YOLO + feed simulator pipeline
    print(f"[Start] YOLO pipeline with {num_feeds} feeds")
    return subprocess.Popen(
        ["python", "-m", "src.realtime_grozi_pipeline", "--num_feeds", str(num_feeds)],
        stdout=None,
        stderr=None
    )


def run_spark_processor():
    print("[Start] PySpark stream processor")
    return subprocess.Popen(
        ["python", "pyspark_stream_processor.py"],
        cwd="src",
        stdout=None,
        stderr=None
    )



def kill_process(proc, name):
    # To terminate subprocesses
    if proc and proc.poll() is None:
        print(f"[Kill] Terminating {name}...")
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        print(f"[Kill] {name} stopped.")


def extract_metrics():
    # Run metrics extractor script to captire the metric for each feed run
    result = subprocess.run(
        ["python", "extract_metrics.py", "--log", LOG_FILE],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    print(result.stdout)
    return result.stdout


def parse_metric_value(text, key):
    for line in text.splitlines():
        if key in line:
            try:
                return float(line.split(":")[1].strip())
            except:
                return None
    return None


if __name__ == "__main__":
    results = []

    for num in FEED_COUNTS:
        print("\n" + "=" * 60)
        print(f"[Experiment] Running with NUM_FEEDS = {num}")
        print("=" * 60)

        clean_spark_dirs()

        # Run YOLO + Spark in parallel
        spark_proc = run_spark_processor()
        time.sleep(10)
        yolo_proc = run_yolo_pipeline(num)

        print(f"[Wait] Running for {RUN_DURATION} seconds...")
        
        print("[Time Before Sleep]", time.strftime("%Y-%m-%d %H:%M:%S"))
        time.sleep(RUN_DURATION)
        print("[Time After Sleep ]", time.strftime("%Y-%m-%d %H:%M:%S"))


        # Terminating both pipelines
        kill_process(yolo_proc, "YOLO pipeline")
        kill_process(spark_proc, "Spark processor")

        # Extract metrics
        print("Extracting metrics...")
        metrics_text = extract_metrics()

        # Parse metrics
        mean_det  = parse_metric_value(metrics_text, "Mean Detections")
        mean_lat  = parse_metric_value(metrics_text, "Mean Latency (ms)")
        mean_conf = parse_metric_value(metrics_text, "Mean Confidence")
        mean_uniq = parse_metric_value(metrics_text, "Mean Unique Prod")

        results.append([num, mean_det, mean_lat, mean_conf, mean_uniq])

        # Save detailed per-feed output
        with open(f"{RESULT_DIR}/results_feeds_{num}.txt", "w") as f:
            f.write(metrics_text)

        time.sleep(10)

    # Saving all feed rmetric extracted to csv for plotting
    csv_path = f"{RESULT_DIR}/scalability_summary.csv"
    with open(csv_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "Feeds",
            "Mean Detections",
            "Mean Latency (ms)",
            "Mean Confidence",
            "Mean Unique Prod"
        ])
        writer.writerows(results)

    print(f"[Saved] Combined CSV: {csv_path}")

    # Plotting the extracted metrics
    feeds = [r[0] for r in results]
    det_values = [r[1] for r in results]
    lat_values = [r[2] for r in results]

    # Latency plot
    plt.figure()
    plt.plot(feeds, lat_values, marker="o")
    plt.title("Latency vs Number of Feeds")
    plt.xlabel("Number of Feeds")
    plt.ylabel("Mean Latency (ms)")
    plt.grid(True)
    plt.savefig(f"{RESULT_DIR}/latency_vs_feeds.png", dpi=150)

    # Detection count plot
    plt.figure()
    plt.plot(feeds, det_values, marker="o")
    plt.title("Detections per Microbatch vs Number of Feeds")
    plt.xlabel("Number of Feeds")
    plt.ylabel("Mean Detections")
    plt.grid(True)
    plt.savefig(f"{RESULT_DIR}/detections_vs_feeds.png", dpi=150)

    print(f"[Saved] Plots saved to {RESULT_DIR}/")
