import matplotlib.pyplot as plt

# Fill these after running extract_metrics.py for each feed count
FEEDS = [1, 3, 5]

# Replace with your actual values
MEAN_LATENCY = [100, 350, 700]      # example numbers
MEAN_DETECTIONS = [2, 6, 11]        # example numbers

def plot_latency():
    plt.figure()
    plt.plot(FEEDS, MEAN_LATENCY, marker="o")
    plt.title("Latency vs Number of Feeds")
    plt.xlabel("Number of Feeds")
    plt.ylabel("Mean Latency (ms)")
    plt.grid(True)
    plt.savefig("latency_vs_feeds.png", dpi=150)

def plot_detections():
    plt.figure()
    plt.plot(FEEDS, MEAN_DETECTIONS, marker="o")
    plt.title("Detections per Microbatch vs Number of Feeds")
    plt.xlabel("Number of Feeds")
    plt.ylabel("Detections per Microbatch")
    plt.grid(True)
    plt.savefig("detections_vs_feeds.png", dpi=150)

if __name__ == "__main__":
    plot_latency()
    plot_detections()