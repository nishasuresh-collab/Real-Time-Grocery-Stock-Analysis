#!/usr/bin/env python3
import os
import sys
import time
import json
import torch
import threading
from datetime import datetime
from ultralytics import YOLO
import argparse

# -------------------------------------------------------------------
# FIX 1: Ensure the src/ directory is always importable
# -------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from constants import *
from feed_simulator import run_all_feeds
from helper import clean_dir, reset_log
# -------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", type=str, default="stream_output_json")
    p.add_argument("--duration", type=int, default=0)
    p.add_argument("--rate", type=float, default=1.0)
    return p.parse_args()

MODEL_PATH = "runs/classify/grozi_cls_v8n_aug10/weights/best.pt"
UPC_INDEX_PATH = "UPC_index.txt"

SLEEP_INTERVAL = 1.0

def load_upc_mapping(path):
    mapping = {}
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    i = 1
    while i < len(lines):
        try:
            idx = int(lines[i])
            upc = lines[i + 1]
            name = lines[i + 2]
            mapping[idx] = {"upc": upc, "name": name}
            i += 3
        except Exception:
            i += 1
    return mapping

def classify_and_save(model, img_path, upc_map, json_dir):
    try:
        results = model.predict(img_path, imgsz=224, verbose=False)
        preds = results[0].probs
        top_idx = int(preds.top1)
        top_conf = float(preds.top1conf)
        raw_label = results[0].names[top_idx]

        try:
            class_id = int(raw_label)
        except ValueError:
            class_id = top_idx + 1

        product = upc_map.get(class_id, {"upc": "NA", "name": f"Product {class_id}"})
        output = {
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "filename": os.path.basename(img_path),
            "class_id": class_id,
            "product_name": product["name"],
            "product_upc": product["upc"],
            "confidence": round(top_conf, 4),
        }

        os.makedirs(json_dir, exist_ok=True)
        out_path = os.path.join(json_dir, os.path.basename(img_path).replace(".png", ".json"))

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        print(f"[Infer] Class {class_id:03d} | {product['name']} | Confidence: {top_conf:.3f}")

    except Exception as e:
        print(f"[Infer] ERROR processing {img_path}: {e}")

def inference_watcher(model, upc_map, stop_event, output_dir, json_dir):
    seen = set()
    print(f"[Watcher] Watching folder: {output_dir}")
    while not stop_event.is_set():
        images = [f for f in os.listdir(output_dir) if f.lower().endswith((".png", ".jpg"))]
        for img in images:
            img_path = os.path.join(output_dir, img)
            if img_path not in seen:
                classify_and_save(model, img_path, upc_map, json_dir)
                seen.add(img_path)
        time.sleep(SLEEP_INTERVAL)

def main():
    args = parse_args()
    json_dir = args.output_dir

    clean_dir(OUTPUT_DIR)
    reset_log(LOG_FILE)
    os.makedirs(json_dir, exist_ok=True)

    upc_map = load_upc_mapping(UPC_INDEX_PATH)
    print(f"Loaded {len(upc_map)} products")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = YOLO(MODEL_PATH)
    print(f"Using {device.upper()}")

    stop_event = threading.Event()

    feed_thread = threading.Thread(target=run_all_feeds, daemon=True)
    feed_thread.start()

    inf_thread = threading.Thread(
        target=inference_watcher,
        args=(model, upc_map, stop_event, OUTPUT_DIR, json_dir),
        daemon=True,
    )
    inf_thread.start()

    t0 = time.time()
    try:
        while time.time() - t0 < args.duration:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    stop_event.set()
    print("[Main] Shutdown complete.")

if __name__ == "__main__":
    main()
