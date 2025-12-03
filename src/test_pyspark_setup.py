#!/usr/bin/env python3
"""
Quick test script to verify PySpark installation and basic streaming setup
"""

import os
import sys
import json
from datetime import datetime

def test_pyspark_import():
    """Test if PySpark is properly installed"""
    print("\n[Test 1] Testing PySpark import...")
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, count, avg
        print("✓ PySpark imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Failed to import PySpark: {e}")
        print("  Install with: pip install pyspark")
        return False

def test_spark_session():
    """Test if Spark session can be created"""
    print("\n[Test 2] Testing Spark session creation...")
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("Test") \
            .master("local[1]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        print(f"✓ Spark session created successfully")
        print(f"  Version: {spark.version}")
        print(f"  Master: {spark.sparkContext.master}")
        spark.stop()
        return True
    except Exception as e:
        print(f"✗ Failed to create Spark session: {e}")
        return False

def test_directory_structure():
    """Test if required directories exist or can be created"""
    print("\n[Test 3] Testing directory structure...")
    dirs = [
        "stream_output_json",
        "spark_checkpoint",
        "spark_metrics_logs",
        "spark_metrics_plots"
    ]
    
    all_ok = True
    for d in dirs:
        if os.path.exists(d):
            print(f"✓ Directory exists: {d}")
        else:
            try:
                os.makedirs(d, exist_ok=True)
                print(f"✓ Directory created: {d}")
            except Exception as e:
                print(f"✗ Failed to create directory {d}: {e}")
                all_ok = False
    
    return all_ok

def create_test_json_files(num_files=3):
    """Create sample JSON files for testing"""
    print(f"\n[Test 4] Creating {num_files} test JSON files...")
    
    test_dir = "stream_output_json"
    os.makedirs(test_dir, exist_ok=True)
    
    products = [
        {"id": 1, "name": "Coca Cola Classic 12oz Can", "upc": "0049000050103"},
        {"id": 2, "name": "Pepsi Regular 12oz Can", "upc": "0012000161155"},
        {"id": 3, "name": "Sprite 12oz Can", "upc": "0049000028904"}
    ]
    
    try:
        for i in range(num_files):
            product = products[i % len(products)]
            test_data = {
                "timestamp": datetime.now().isoformat(),
                "filename": f"test_feed_A_{i}.png",
                "class_id": product["id"],
                "product_name": product["name"],
                "product_upc": product["upc"],
                "confidence": 0.85 + (i * 0.03)
            }
            
            filepath = os.path.join(test_dir, f"test_detection_{i}.json")
            with open(filepath, "w") as f:
                json.dump(test_data, f, indent=2)
            
            print(f"✓ Created: {filepath}")
        
        return True
    except Exception as e:
        print(f"✗ Failed to create test files: {e}")
        return False

def test_json_reading():
    """Test if PySpark can read the JSON files"""
    print("\n[Test 5] Testing JSON reading with PySpark...")
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        
        spark = SparkSession.builder \
            .appName("JSONTest") \
            .master("local[1]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("filename", StringType(), True),
            StructField("class_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("product_upc", StringType(), True),
            StructField("confidence", DoubleType(), True)
        ])
        
        df = spark.read \
            .schema(schema) \
            .json("stream_output_json")
        
        count = df.count()
        print(f"✓ Successfully read {count} JSON records")
        
        if count > 0:
            print("\nSample data:")
            df.select("product_name", "confidence").show(3, truncate=False)
        
        spark.stop()
        return True
    except Exception as e:
        print(f"✗ Failed to read JSON files: {e}")
        return False

def cleanup_test_files():
    """Clean up test files"""
    print("\n[Cleanup] Removing test files...")
    test_dir = "stream_output_json"
    try:
        if os.path.exists(test_dir):
            for f in os.listdir(test_dir):
                if f.startswith("test_detection_"):
                    filepath = os.path.join(test_dir, f)
                    os.remove(filepath)
                    print(f"✓ Removed: {filepath}")
        return True
    except Exception as e:
        print(f"✗ Cleanup failed: {e}")
        return False

def main():
    print("="*70)
    print("PySpark Streaming Setup - Test Suite")
    print("="*70)
    
    tests = [
        ("PySpark Import", test_pyspark_import),
        ("Spark Session", test_spark_session),
        ("Directory Structure", test_directory_structure),
        ("Test JSON Creation", lambda: create_test_json_files(3)),
        ("JSON Reading", test_json_reading),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ Test '{test_name}' failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status} - {test_name}")
    
    print(f"\n{passed}/{total} tests passed")
    
    # Cleanup
    cleanup_test_files()
    
    if passed == total:
        print("\n✓ All tests passed! PySpark streaming is ready to use.")
        print("\nNext steps:")
        print("  1. Run: python realtime_grozi_pipeline.py")
        print("  2. In another terminal, run: python pyspark_stream_processor.py")
        print("  3. Visualize: python visualize_metrics.py")
        return 0
    else:
        print("\n✗ Some tests failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
