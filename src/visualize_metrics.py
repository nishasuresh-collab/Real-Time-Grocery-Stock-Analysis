import os
import re
import matplotlib.pyplot as plt
from datetime import datetime
from collections import defaultdict

METRICS_LOG_FILE = "spark_metrics_logs/batch_metrics.log"
OUTPUT_PLOT_DIR = "spark_metrics_plots"

def parse_metrics_log(log_file):
    """Parse the batch metrics log file"""
    if not os.path.exists(log_file):
        print(f"[Error] Log file not found: {log_file}")
        return None
    
    timestamps = []
    batch_ids = []
    detections = []
    confidences = []
    unique_products = []
    
    with open(log_file, "r") as f:
        for line in f:
            # Skip header lines
            if "Stream started" in line or "=" in line:
                continue
            
            # Parse log line
            # Format: 2024-01-15 10:30:45 | Batch 0 | Detections: 10 | Avg Confidence: 0.8543 | Unique Products: 3
            match = re.match(
                r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \| Batch (\d+) \| '
                r'Detections: (\d+) \| Avg Confidence: ([\d.]+) \| Unique Products: (\d+)',
                line.strip()
            )
            
            if match:
                timestamp_str, batch_id, detection_count, avg_conf, unique_prod = match.groups()
                timestamps.append(datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S"))
                batch_ids.append(int(batch_id))
                detections.append(int(detection_count))
                confidences.append(float(avg_conf))
                unique_products.append(int(unique_prod))
    
    return {
        "timestamps": timestamps,
        "batch_ids": batch_ids,
        "detections": detections,
        "confidences": confidences,
        "unique_products": unique_products
    }

def create_visualizations(metrics):
    """Create simple visualization plots"""
    if not metrics or len(metrics["batch_ids"]) == 0:
        print("[Warning] No metrics data to visualize")
        return
    
    os.makedirs(OUTPUT_PLOT_DIR, exist_ok=True)
    
    # Create a figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('PySpark Structured Streaming - Real-time Metrics', fontsize=16, fontweight='bold')
    
    # Plot 1: Detections per batch
    axes[0, 0].plot(metrics["batch_ids"], metrics["detections"], 
                     marker='o', linestyle='-', linewidth=2, markersize=6, color='#2E86AB')
    axes[0, 0].set_xlabel('Batch ID', fontsize=11)
    axes[0, 0].set_ylabel('Number of Detections', fontsize=11)
    axes[0, 0].set_title('Detections per Micro-batch', fontsize=12, fontweight='bold')
    axes[0, 0].grid(True, alpha=0.3)
    
    # Plot 2: Average confidence per batch
    axes[0, 1].plot(metrics["batch_ids"], metrics["confidences"], 
                     marker='s', linestyle='-', linewidth=2, markersize=6, color='#A23B72')
    axes[0, 1].set_xlabel('Batch ID', fontsize=11)
    axes[0, 1].set_ylabel('Average Confidence', fontsize=11)
    axes[0, 1].set_title('Average Confidence per Micro-batch', fontsize=12, fontweight='bold')
    axes[0, 1].set_ylim([0, 1.0])
    axes[0, 1].grid(True, alpha=0.3)
    
    # Plot 3: Unique products per batch
    axes[1, 0].bar(metrics["batch_ids"], metrics["unique_products"], 
                    color='#F18F01', alpha=0.7, edgecolor='black')
    axes[1, 0].set_xlabel('Batch ID', fontsize=11)
    axes[1, 0].set_ylabel('Unique Products', fontsize=11)
    axes[1, 0].set_title('Unique Products per Micro-batch', fontsize=12, fontweight='bold')
    axes[1, 0].grid(True, alpha=0.3, axis='y')
    
    # Plot 4: Cumulative detections over time
    cumulative_detections = []
    total = 0
    for det in metrics["detections"]:
        total += det
        cumulative_detections.append(total)
    
    axes[1, 1].plot(metrics["batch_ids"], cumulative_detections, 
                     marker='D', linestyle='-', linewidth=2, markersize=6, color='#06A77D')
    axes[1, 1].set_xlabel('Batch ID', fontsize=11)
    axes[1, 1].set_ylabel('Cumulative Detections', fontsize=11)
    axes[1, 1].set_title('Cumulative Detection Count', fontsize=12, fontweight='bold')
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save plot
    output_path = os.path.join(OUTPUT_PLOT_DIR, "streaming_metrics.png")
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"\n[Visualization] Plot saved to: {output_path}")
    
    # Show plot
    plt.show()
    
    # Print summary statistics
    print("\n" + "="*60)
    print("STREAMING METRICS SUMMARY")
    print("="*60)
    print(f"Total Batches Processed: {len(metrics['batch_ids'])}")
    print(f"Total Detections: {sum(metrics['detections'])}")
    print(f"Average Detections per Batch: {sum(metrics['detections']) / len(metrics['detections']):.2f}")
    print(f"Average Confidence: {sum(metrics['confidences']) / len(metrics['confidences']):.4f}")
    print(f"Max Unique Products in Batch: {max(metrics['unique_products'])}")
    print("="*60 + "\n")

def main():
    """Main execution function"""
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
