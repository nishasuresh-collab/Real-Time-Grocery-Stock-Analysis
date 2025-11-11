# PySpark Structured Streaming for GroZi-120 Detection Pipeline

This module implements the **PySpark Stream Ingestion** milestone for processing real-time product detection data.

## Overview

The PySpark streaming pipeline continuously reads JSON detection outputs from the YOLO inference pipeline and processes them in micro-batches, demonstrating:

- **Real-time data ingestion** from JSON files
- **Micro-batch processing** with configurable trigger intervals
- **Basic metrics computation** (detection counts, confidence scores, unique products)
- **Checkpointing** for fault tolerance
- **Performance monitoring** through logs and visualizations

## Files

1. **`pyspark_stream_processor.py`** - Full PySpark Structured Streaming application (requires Hadoop setup on Windows)
2. **`pyspark_simple_processor.py`** - Simplified batch processor (Windows-compatible, no Hadoop needed) ⭐
3. **`windows_setup_helper.py`** - Windows setup assistant for Hadoop configuration
4. **`visualize_metrics.py`** - Visualization script for streaming metrics
5. **Output directories**:
   - `spark_checkpoint/` - Checkpoint data for fault tolerance
   - `spark_metrics_logs/` - Batch processing metrics logs (from stream processor)
   - `spark_metrics_logs_simple/` - Metrics from simple processor
   - `spark_metrics_plots/` - Generated visualization plots

⭐ **Recommended for Windows users**: Use `pyspark_simple_processor.py` to avoid Hadoop setup issues!

## Prerequisites

```bash
pip install pyspark
pip install matplotlib
pip install torch torchvision
pip install ultralytics
```

## Usage

### Quick Start (Windows-Friendly)

**For Windows users**, use the simple processor to avoid Hadoop issues:

```bash
# Terminal 1: Run detection pipeline
python realtime_grozi_pipeline.py

# Terminal 2: Run simple PySpark processor
python pyspark_simple_processor.py
```

The simple processor uses batch processing instead of streaming and works reliably on all platforms!

### Step 1: Run the Complete Pipeline

Run the detection pipeline which includes feed simulation and YOLO inference:

```bash
python realtime_grozi_pipeline.py
```

This will:
- Clean output directories
- Start simulating image feeds from GroZi dataset
- Run YOLO detection on streamed images
- Generate JSON detection outputs in `stream_output_json/`

### Step 2: Run PySpark Stream Processor (in a separate terminal)

While the detection pipeline is running, start the PySpark streaming processor:

```bash
python pyspark_stream_processor.py
```

This will:
- Initialize Spark session
- Read JSON files from `stream_output_json/` directory
- Process detections in micro-batches (default: 2-second intervals)
- Log metrics to console and `spark_metrics_logs/batch_metrics.log`
- Show real-time statistics per batch

### Step 3: Visualize Metrics

After collecting some data, you can visualize the streaming metrics:

```bash
python visualize_metrics.py
```

This generates plots showing:
- Detections per micro-batch
- Average confidence per batch
- Unique products per batch
- Cumulative detection counts

## Configuration

### PySpark Stream Processor Settings

Edit `pyspark_stream_processor.py` to modify:

```python
# Input/Output directories
JSON_INPUT_DIR = "stream_output_json"
CHECKPOINT_DIR = "spark_checkpoint"
METRICS_LOG_DIR = "spark_metrics_logs"

# Streaming parameters
WINDOW_DURATION = "10 seconds"      # Window size for aggregations
SLIDE_DURATION = "5 seconds"        # Sliding interval
TRIGGER_INTERVAL = "2 seconds"      # Micro-batch trigger interval
```

### Key Parameters Explained

- **TRIGGER_INTERVAL**: How often PySpark checks for new data and processes a micro-batch
- **WINDOW_DURATION**: Time window for windowed aggregations (if enabled)
- **maxFilesPerTrigger**: Maximum JSON files to process per batch (default: 10)

## Output Examples

### Console Output (Per Batch)

```
======================================================================
[Batch 3] Processed at 2024-01-15 14:32:10
  Total Detections: 8
  Average Confidence: 0.8756
  Unique Products: 4
======================================================================

[Batch 3] Top products in this batch:
+---------------------------+--------+-----+--------+
|product_name               |class_id|count|avg_conf|
+---------------------------+--------+-----+--------+
|Coca Cola Classic 12oz Can |15      |3    |0.9123  |
|Pepsi Regular 12oz Can     |42      |2    |0.8654  |
|Sprite 12oz Can            |67      |2    |0.8432  |
|Fanta Orange 12oz Can      |23      |1    |0.8234  |
+---------------------------+--------+-----+--------+
```

### Metrics Log File

Location: `spark_metrics_logs/batch_metrics.log`

```
Stream started at 2024-01-15 14:30:00
======================================================================
2024-01-15 14:30:05 | Batch 0 | Detections: 10 | Avg Confidence: 0.8543 | Unique Products: 3
2024-01-15 14:30:07 | Batch 1 | Detections: 12 | Avg Confidence: 0.8721 | Unique Products: 5
2024-01-15 14:30:09 | Batch 2 | Detections: 8 | Avg Confidence: 0.8456 | Unique Products: 4
```

## Performance Metrics

The pipeline tracks key streaming metrics:

1. **Throughput**:
   - Input rows per second
   - Processed rows per second
   - Detections per micro-batch

2. **Latency**:
   - Batch duration (trigger execution time)
   - Processing time per batch

3. **Quality Metrics**:
   - Average confidence scores
   - Unique product diversity
   - Detection patterns over time

## Testing Fault Tolerance

To test checkpointing and recovery:

1. Start the pipeline normally
2. Let it process a few batches
3. Press `Ctrl+C` to stop
4. Restart `python pyspark_stream_processor.py`
5. The pipeline should resume from the checkpoint

## Advanced Features (Optional)

### Enable Windowed Aggregations

Uncomment in `pyspark_stream_processor.py`:

```python
windowed_query = create_windowed_aggregation_query(spark)
```

This enables time-based windowed statistics with sliding windows.

### Adjust Parallelism

Modify Spark configuration:

```python
.config("spark.sql.shuffle.partitions", "4")  # Increase for more parallelism
```

### Filter Low Confidence Detections

Already implemented - filters detections with confidence < 0.1:

```python
filtered_stream = parsed_stream.filter(col("confidence") > 0.1)
```

## Troubleshooting

### Windows-Specific Issues

#### Issue: Hadoop NativeIO UnsatisfiedLinkError

This is the most common Windows error with PySpark. You have **three solutions**:

**Solution 1: Use the Simple Processor (Recommended for Quick Start)**
```bash
python pyspark_simple_processor.py
```
This version uses batch processing instead of streaming and avoids Hadoop file system issues entirely.

**Solution 2: Install Hadoop Winutils**
```bash
# Run the Windows setup helper
python windows_setup_helper.py
```
This will guide you through downloading and configuring winutils.exe.

**Manual steps:**
1. Download winutils.exe from: https://github.com/cdarlint/winutils
2. Create `C:\hadoop\bin\` directory
3. Place `winutils.exe` and `hadoop.dll` in `C:\hadoop\bin\`
4. Set environment variable:
   - Variable: `HADOOP_HOME`
   - Value: `C:\hadoop`
5. Restart terminal/IDE
6. Run: `python pyspark_stream_processor.py`

**Solution 3: Use WSL2 or Docker**
- Install WSL2 (Windows Subsystem for Linux)
- Run the entire project in the Linux environment
- No Hadoop issues!

### General Troubleshooting

### Issue: "No JSON files found"

**Solution**: Ensure `realtime_grozi_pipeline.py` is running and generating JSON outputs.

### Issue: "Java not found" or Spark errors

**Solution**: Install Java JDK 8 or 11:
```bash
sudo apt-get install openjdk-11-jdk
```

### Issue: Checkpoint directory errors

**Solution**: Delete checkpoint directory to start fresh:
```bash
rm -rf spark_checkpoint/
```

## Project Structure

```
.
├── realtime_grozi_pipeline.py      # Detection pipeline with YOLO
├── feed_simulator.py               # Image feed simulator
├── pyspark_stream_processor.py     # PySpark streaming (THIS MODULE)
├── visualize_metrics.py            # Metrics visualization
├── stream_output_json/             # JSON detection outputs (input)
├── spark_checkpoint/               # PySpark checkpoints
├── spark_metrics_logs/             # Metrics logs (output)
└── spark_metrics_plots/            # Visualization plots (output)
```

## Future Enhancements

This basic implementation can be expanded to include:

- [ ] More sophisticated aggregations (hourly/daily summaries)
- [ ] Multiple concurrent streaming queries
- [ ] Integration with streaming dashboards (e.g., Grafana)
- [ ] Kafka integration for production streaming
- [ ] Machine learning on streaming data
- [ ] Alert triggers for anomalies
- [ ] Scalability tests with multiple executors

## References

- [PySpark Structured Streaming Documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Micro-batch Processing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers)
- [Checkpointing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing)
