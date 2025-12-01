import re
import argparse
import numpy as np

LOG_PATTERN = re.compile(
    r"""
    (?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s*\|\s*
    Batch\s+(?P<batch>\d+)\s*\|\s*
    Detections:\s*(?P<det>\d+)\s*\|\s*
    AvgConf:\s*(?P<conf>[0-9.]+)\s*\|\s*
    AvgLatencyMs:\s*(?P<lat>[\d.]+|NA)\s*\|\s*
    UniqueProducts:\s*(?P<uniq>\d+)
    """,
    re.VERBOSE,
)

def safe_mean(arr):
    return round(float(np.mean(arr)), 3) if arr else None

def extract(path):
    detections = []
    confidences = []
    latencies = []
    uniques = []

    for line in open(path):
        m = LOG_PATTERN.match(line.strip())
        if not m:
            continue
       
        detections.append(int(m.group("det")))
        confidences.append(float(m.group("conf")))
        uniques.append(int(m.group("uniq")))

        lat = m.group("lat")
        if lat != "NA":
            latencies.append(float(lat))

    print("\n==== METRICS SUMMARY ====")
    print("Mean Detections  :", safe_mean(detections))
    print("Mean Latency (ms):", safe_mean(latencies))
    print("Mean Confidence  :", safe_mean(confidences))
    print("Mean Unique Prod :", safe_mean(uniques))
    print("==========================\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True)
    args = parser.parse_args()
    extract(args.log)