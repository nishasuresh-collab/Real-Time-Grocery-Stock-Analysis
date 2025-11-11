import os
import sys
import time
import platform
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, window, current_timestamp, 
    from_json, to_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, DoubleType, TimestampType
)

# Configuration
JSON_INPUT_DIR = "stream_output_json"
CHECKPOINT_DIR = "spark_checkpoint"
METRICS_LOG_DIR = "spark_metrics_logs"
WINDOW_DURATION = "10 seconds"
SLIDE_DURATION = "5 seconds"
TRIGGER_INTERVAL = "2 seconds"

# Define schema for incoming JSON detection records
detection_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("filename", StringType(), True),
    StructField("class_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_upc", StringType(), True),
    StructField("confidence", DoubleType(), True)
])

def setup_windows_environment():
    """
    Setup Windows-specific environment for PySpark.
    This fixes the Hadoop NativeIO error on Windows.
    """
    if platform.system() != "Windows":
        return True
    
    print("\n[Windows] Detecting Windows environment...")
    
    # Check if HADOOP_HOME is set
    hadoop_home = os.environ.get("HADOOP_HOME")
    
    if hadoop_home:
        winutils_path = os.path.join(hadoop_home, "bin", "winutils.exe")
        if os.path.exists(winutils_path):
            print(f"[Windows] ✓ Using HADOOP_HOME: {hadoop_home}")
            print(f"[Windows] ✓ winutils.exe found at: {winutils_path}")
            return True
        else:
            print(f"[Windows] ✗ HADOOP_HOME is set but winutils.exe not found at: {winutils_path}")
    
    # If not set or winutils not found, create a workaround
    print("\n" + "="*70)
    print("[Windows] Hadoop winutils.exe not properly configured!")
    print("="*70)
    print("\nTo fix this permanently:")
    print("1. Download winutils.exe from:")
    print("   https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.1/bin/winutils.exe")
    print("2. Create directory: C:\\hadoop\\bin\\")
    print("3. Place winutils.exe in C:\\hadoop\\bin\\")
    print("4. Add to System Environment Variables:")
    print("   Variable: HADOOP_HOME")
    print("   Value: C:\\hadoop")
    print("5. Restart your terminal/IDE")
    print("\nTrying alternative approach (may have limitations)...")
    print("="*70 + "\n")
    
    # Create a temporary hadoop home in current directory
    try:
        temp_hadoop = os.path.join(os.getcwd(), "temp_hadoop")
        temp_bin = os.path.join(temp_hadoop, "bin")
        os.makedirs(temp_bin, exist_ok=True)
        
        # Set environment variable
        os.environ["HADOOP_HOME"] = temp_hadoop
        print(f"[Windows] Created temporary HADOOP_HOME: {temp_hadoop}")
        print("[Windows] Note: Some features may be limited without proper winutils.exe")
        
        return True
    except Exception as e:
        print(f"[Windows] ✗ Failed to create temporary HADOOP_HOME: {e}")
        return False

def initialize_spark():
    """Initialize Spark session with Windows-compatible configurations"""
    
    # Setup Windows environment first
    if not setup_windows_environment():
        print("\n[Error] Could not setup Windows environment properly.")
        print("[Info] You may encounter issues. Consider installing winutils.exe")
    
    print(f"\n[Spark] Initializing Spark session...")
    
    # Configure Spark with Windows-friendly settings
    builder = SparkSession.builder \
        .appName("GroZi-120 Detection Stream Processor") \
        .master("local[*]")
    
    # Add Windows-specific configurations
    if platform.system() == "Windows":
        builder = builder \
            .config("spark.sql.warehouse.dir", os.path.abspath("./spark-warehouse")) \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "127.0.0.1")
    
    # Common configurations
    builder = builder \
        .config("spark.sql.streaming.schemaInference", "false") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.ui.enabled", "false")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"[Spark] ✓ Initialized successfully")
    print(f"[Spark] Version: {spark.version}")
    print(f"[Spark] Master: {spark.sparkContext.master}")
    print(f"[Spark] Cores: {spark.sparkContext.defaultParallelism}")
    
    return spark

def setup_directories():
    """Create necessary directories for checkpointing and metrics"""
    dirs = [JSON_INPUT_DIR, CHECKPOINT_DIR, METRICS_LOG_DIR]
    
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)
    
    print(f"\n[Setup] Directories ready:")
    print(f"  Input: {os.path.abspath(JSON_INPUT_DIR)}")
    print(f"  Checkpoint: {os.path.abspath(CHECKPOINT_DIR)}")
    print(f"  Metrics: {os.path.abspath(METRICS_LOG_DIR)}")

def log_metrics(batch_df, batch_id):
    """
    Custom function to log metrics for each micro-batch.
    This demonstrates basic processing and metric extraction.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] Empty batch at {timestamp}")
        return
    
    try:
        # Collect basic statistics
        total_detections = batch_df.count()
        avg_confidence = batch_df.agg(avg("confidence")).collect()[0][0]
        unique_products = batch_df.select("product_name").distinct().count()
        
        # Log to console
        print(f"\n{'='*70}")
        print(f"[Batch {batch_id}] Processed at {timestamp}")
        print(f"  Total Detections: {total_detections}")
        print(f"  Average Confidence: {avg_confidence:.4f}")
        print(f"  Unique Products: {unique_products}")
        print(f"{'='*70}\n")
        
        # Write metrics to log file
        log_file = os.path.join(METRICS_LOG_DIR, f"batch_metrics.log")
        with open(log_file, "a") as f:
            f.write(f"{timestamp} | Batch {batch_id} | "
                    f"Detections: {total_detections} | "
                    f"Avg Confidence: {avg_confidence:.4f} | "
                    f"Unique Products: {unique_products}\n")
        
        # Show top products in this batch
        print(f"[Batch {batch_id}] Top products in this batch:")
        product_counts = batch_df.groupBy("product_name", "class_id") \
            .agg(count("*").alias("count"), avg("confidence").alias("avg_conf")) \
            .orderBy(col("count").desc())
        
        product_counts.show(5, truncate=False)
        
    except Exception as e:
        print(f"[Batch {batch_id}] Error processing batch: {e}")

def create_streaming_query(spark):
    """
    Create and configure the main streaming query that:
    1. Reads JSON files from the input directory
    2. Parses the detection data
    3. Computes basic aggregations
    4. Logs metrics for each micro-batch
    """
    
    print(f"\n[Stream] Configuring streaming query...")
    print(f"[Stream] Reading from: {os.path.abspath(JSON_INPUT_DIR)}")
    
    # Ensure input directory exists
    if not os.path.exists(JSON_INPUT_DIR):
        os.makedirs(JSON_INPUT_DIR, exist_ok=True)
        print(f"[Stream] Created input directory: {JSON_INPUT_DIR}")
    
    try:
        # Read JSON files as a stream
        raw_stream = spark.readStream \
            .format("json") \
            .schema(detection_schema) \
            .option("maxFilesPerTrigger", 10) \
            .load(JSON_INPUT_DIR)
        
        print(f"[Stream] ✓ Stream source configured")
        
        # Parse timestamp and add processing time
        parsed_stream = raw_stream \
            .withColumn("event_time", to_timestamp(col("timestamp"))) \
            .withColumn("processing_time", current_timestamp())
        
        # Basic transformation: filter out low confidence detections
        filtered_stream = parsed_stream.filter(col("confidence") > 0.1)
        
        print(f"[Stream] ✓ Transformations applied")
        print(f"[Stream] Starting query with trigger interval: {TRIGGER_INTERVAL}")
        
        # Write stream with foreachBatch to enable custom metric logging
        query = filtered_stream.writeStream \
            .foreachBatch(log_metrics) \
            .outputMode("update") \
            .option("checkpointLocation", CHECKPOINT_DIR) \
            .trigger(processingTime=TRIGGER_INTERVAL) \
            .start()
        
        print(f"[Stream] ✓ Query started successfully")
        print(f"[Stream] Checkpoint: {os.path.abspath(CHECKPOINT_DIR)}")
        
        return query
        
    except Exception as e:
        print(f"\n[Error] Failed to create streaming query: {e}")
        
        if platform.system() == "Windows" and "NativeIO" in str(e):
            print("\n[Windows Fix] If you see Hadoop/NativeIO errors, try:")
            print("  1. Download winutils.exe from: https://github.com/cdarlint/winutils")
            print("  2. Create C:\\hadoop\\bin\\ directory")
            print("  3. Place winutils.exe in C:\\hadoop\\bin\\")
            print("  4. Set environment variable: HADOOP_HOME=C:\\hadoop")
            print("  5. Restart your terminal/IDE")
        
        raise

def print_stream_status(query, interval=10):
    """Periodically print streaming query status"""
    last_progress_time = time.time()
    
    while query.isActive:
        time.sleep(interval)
        
        try:
            status = query.status
            progress = query.lastProgress
            
            current_time = time.time()
            
            if progress and (current_time - last_progress_time) >= interval:
                print(f"\n[Status] Stream Active | "
                      f"Batch: {progress['batchId']} | "
                      f"Input Rows: {progress['numInputRows']} | "
                      f"Time: {progress.get('durationMs', {}).get('triggerExecution', 'N/A')} ms")
                last_progress_time = current_time
                
        except Exception as e:
            print(f"[Status] Error getting status: {e}")

def main():
    """Main execution function"""
    print("\n" + "="*70)
    print("PySpark Structured Streaming - GroZi Detection Processor")
    print("="*70 + "\n")
    
    # Setup
    setup_directories()
    
    try:
        spark = initialize_spark()
    except Exception as e:
        print(f"\n[Error] Failed to initialize Spark: {e}")
        print("\nCommon fixes:")
        print("  - Ensure Java JDK 8 or 11 is installed")
        print("  - On Windows, install winutils.exe (see above)")
        print("  - Check that ports 4040, 4041 are not in use")
        return
    
    # Initialize metrics log file
    log_file = os.path.join(METRICS_LOG_DIR, f"batch_metrics.log")
    with open(log_file, "w") as f:
        f.write(f"Stream started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*70 + "\n")
    
    print(f"\n[Metrics] Log file: {os.path.abspath(log_file)}")
    
    # Create streaming queries
    try:
        main_query = create_streaming_query(spark)
    except Exception as e:
        print(f"\n[Error] Failed to start streaming query: {e}")
        spark.stop()
        return
    
    print("\n" + "="*70)
    print("[Stream] Streaming query is running!")
    print("="*70)
    print("\nMonitoring for JSON files in:", os.path.abspath(JSON_INPUT_DIR))
    print("Press Ctrl+C to stop.\n")
    
    try:
        # Monitor stream status
        while main_query.isActive:
            time.sleep(5)
            progress = main_query.lastProgress
            
            if progress:
                num_rows = progress['numInputRows']
                batch_id = progress['batchId']
                duration = progress.get('durationMs', {}).get('triggerExecution', 'N/A')
                
                if num_rows > 0:
                    print(f"[Progress] Batch {batch_id} | "
                          f"Rows: {num_rows} | "
                          f"Duration: {duration} ms")
    
    except KeyboardInterrupt:
        print("\n\n[Main] Shutting down gracefully...")
        
    finally:
        if main_query:
            main_query.stop()
            print("[Main] ✓ Streaming query stopped")
        
        if spark:
            spark.stop()
            print("[Main] ✓ Spark session closed")
        
        print(f"\n[Main] Metrics saved to: {os.path.abspath(log_file)}")
        print("\n" + "="*70)
        print("Session ended successfully")
        print("="*70 + "\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[Fatal Error] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
