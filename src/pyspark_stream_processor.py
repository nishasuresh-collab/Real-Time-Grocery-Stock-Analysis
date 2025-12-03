#!/usr/bin/env python3
"""
PySpark Structured Streaming job for GroZi pipeline.

This version is minimal, stable, WSL-compatible, and designed to work
with run_pipeline.py and run_all_experiments.py.

It receives parameters:
    --input-dir
    --checkpoint-dir
    --metrics-log
    --trigger
    --duration
"""

import argparse
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T


# ------------------- ARG PARSER ------------------- #

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", required=True)
    p.add_argument("--checkpoint-dir", required=True)
    p.add_argument("--metrics-log", required=True)
    p.add_argument("--trigger", type=str, default="2 seconds")
    p.add_argument("--duration", type=int, default=0)
    return p.parse_args()


# ------------------- SPARK SETUP ------------------- #

def build_spark_session():
    spark = (
        SparkSession.builder
        .appName("GroZiStreamProcessor")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ------------------- SCHEMA ------------------- #

def get_schema():
    return T.StructType([
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("filename", T.StringType(), True),
        T.StructField("class_id", T.IntegerType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField("product_upc", T.StringType(), True),
        T.StructField("confidence", T.DoubleType(), True),
    ])


# ------------------- METRICS SETUP ------------------- #

def init_metrics_log(path: Path):
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write("batch_id,timestamp,num_rows,unique_products,"
                    "avg_conf,avg_latency_ms,processing_time_ms\n")


# ------------------- FOREACH BATCH ------------------- #

def foreach_batch(metrics_path: Path):
    def process(df, batch_id):
        start = time.time()

        count = df.count()
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        if count == 0:
            with open(metrics_path, "a") as f:
                f.write(f"{batch_id},{now},0,0,,,\n")
            return

        cols = df.columns

        # Unique products
        unique = df.select("product_upc").distinct().count() if "product_upc" in cols else 0

        # Avg confidence
        avg_conf = None
        if "confidence" in cols:
            avg_conf = df.agg(F.avg("confidence")).collect()[0][0]

        # Latency
        avg_latency_ms = None
        if "timestamp" in cols:
            df2 = df.withColumn("evt", F.to_timestamp("timestamp"))
            df2 = df2.withColumn("now_ts", F.current_timestamp())
            lat = df2.select(
                F.avg((F.col("now_ts").cast("long") - F.col("evt").cast("long")) * 1000)
                .alias("lat")
            ).collect()[0]["lat"]
            avg_latency_ms = lat

        proc_ms = (time.time() - start) * 1000

        with open(metrics_path, "a") as f:
            f.write(
                f"{batch_id},{now},{count},{unique},"
                f"{'' if avg_conf is None else avg_conf},"
                f"{'' if avg_latency_ms is None else avg_latency_ms},"
                f"{proc_ms}\n"
            )

        print(f"[Spark] Batch {batch_id} | rows={count} | "
              f"unique={unique} | avg_conf={avg_conf} | "
              f"latency_ms={avg_latency_ms} | proc_ms={proc_ms:.2f}",
              flush=True)

    return process


# ------------------- MAIN ------------------- #

def main():
    args = parse_args()

    input_dir = Path(args.input_dir)
    checkpoint = Path(args.checkpoint_dir)
    metrics_path = Path(args.metrics_log)

    print(f"[Spark] Input dir      = {input_dir}")
    print(f"[Spark] Checkpoint dir = {checkpoint}")
    print(f"[Spark] Metrics log    = {metrics_path}")
    print(f"[Spark] Trigger        = {args.trigger}")
    print(f"[Spark] Duration (s)   = {args.duration}")

    init_metrics_log(metrics_path)
    spark = build_spark_session()

    schema = get_schema()

    df = (
        spark.readStream
        .schema(schema)
        .json(str(input_dir))
    )

    query = (
        df.writeStream
        .foreachBatch(foreach_batch(metrics_path))
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint))
        .trigger(processingTime=args.trigger)
        .start()
    )

    print("[Spark] Streaming query started.", flush=True)

    if args.duration > 0:
        query.awaitTermination(args.duration)
        print("[Spark] Duration reached; stopping.")
        query.stop()
    else:
        query.awaitTermination()

    print("[Spark] Query terminated.")
    spark.stop()


if __name__ == "__main__":
    main()
