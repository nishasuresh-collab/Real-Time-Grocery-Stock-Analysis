import os
import time
import json
import torch
import threading
import shutil
from datetime import datetime
from ultralytics import YOLO
from src.constants import *
from src.feed_simulator import run_all_feeds
from src.helper import clean_dir, reset_log

import argparse
from src.constants import ALL_FEEDS


MODEL_PATH = "runs/classify/grozi_cls_v8n_aug10/weights/best.pt"
UPC_INDEX_PATH = "UPC_index.txt"
JSON_OUT_DIR = "stream_output_json"
SLEEP_INTERVAL = 1.0

parser = argparse.ArgumentParser()
parser.add_argument("--num_feeds", type=int, default=1, help="Number of camera feeds to simulate")
args = parser.parse_args()

# Validate and build FEEDS
NUM_FEEDS = min(max(args.num_feeds, 1), len(ALL_FEEDS))
FEEDS = {
    f"feed_{i+1}": ALL_FEEDS[i]
    for i in range(NUM_FEEDS)
}

print(f"[Config] Using {NUM_FEEDS} feeds: {FEEDS}")

print(f"[Config] Using {NUM_FEEDS} feeds:")
for k, v in FEEDS.items():
    print(f"  {k} -> {v}")


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

def classify_and_save(model, img_path, upc_map):
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
            "confidence": round(top_conf, 4)
        }

        os.makedirs(JSON_OUT_DIR, exist_ok=True)
        json_path = os.path.join(JSON_OUT_DIR, os.path.splitext(os.path.basename(img_path))[0] + ".json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        print(f"[Infer] Class {class_id:03d} | {product['name']} | Confidence: {top_conf:.3f}")

    except Exception as e:
        print(f"[Infer] Error processing {img_path}: {e}")

def inference_watcher(model, upc_map, stop_event):
    seen = set()
    print(f"[Watcher] Watching folder: {OUTPUT_DIR}")
    while not stop_event.is_set():
        imgs = [f for f in os.listdir(OUTPUT_DIR) if f.lower().endswith((".png", ".jpg"))]
        for img in imgs:
            img_path = os.path.join(OUTPUT_DIR, img)
            if img_path not in seen:
                classify_and_save(model, img_path, upc_map)
                seen.add(img_path)
        time.sleep(SLEEP_INTERVAL)

def main():
    clean_dir(OUTPUT_DIR)
    reset_log(LOG_FILE)
    os.makedirs(JSON_OUT_DIR, exist_ok=True)

    upc_map = load_upc_mapping(UPC_INDEX_PATH)
    print(f"Loaded {len(upc_map)} products from UPC_index.txt")

    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = YOLO(MODEL_PATH)
    print(f"Using {device.upper()} ({torch.cuda.get_device_name(0) if device=='cuda' else 'CPU only'})")

    stop_event = threading.Event()
    feed_thread = threading.Thread(target=run_all_feeds, args=(FEEDS,), daemon=True)
    feed_thread.start()
    print("[Main] Feed simulation started.")

    inf_thread = threading.Thread(target=inference_watcher, args=(model, upc_map, stop_event), daemon=True)
    inf_thread.start()
    print("[Main] Inference watcher started.")

    try:
        while feed_thread.is_alive():
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n[Main] Shutting down...")
        stop_event.set()

if __name__ == "__main__":
    main()