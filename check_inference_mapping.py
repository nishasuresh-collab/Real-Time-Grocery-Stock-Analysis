import os
from ultralytics import YOLO
import random
from pathlib import Path

# Try to locate model and dataset
proj = Path(__file__).parent
model_path = proj / "runs" / "classify" / "grozi_cls_v8n_aug10" / "weights" / "best.pt"
data_val_dir = proj / "grozi_classification" / "val"

if not model_path.exists():
    print("Model not found at", model_path)
    raise SystemExit(1)
if not data_val_dir.exists():
    print("Validation folder not found at", data_val_dir)
    raise SystemExit(1)

model = YOLO(str(model_path))
print("Loaded model:", model_path)

# collect up to 20 sample images from val, one per class where available
samples = []
for class_dir in sorted(data_val_dir.iterdir()):
    if not class_dir.is_dir():
        continue
    imgs = list(class_dir.glob("*.png")) + list(class_dir.glob("*.jpg"))
    if not imgs:
        continue
    samples.append((class_dir.name, random.choice(imgs)))
    if len(samples) >= 20:
        break

if not samples:
    print("No validation images found to test.")
    raise SystemExit(1)

from datetime import datetime
print(f"Testing {len(samples)} samples from {data_val_dir}")
for gt_folder, img_path in samples:
    gt_id = None
    try:
        gt_id = int(gt_folder)
    except Exception:
        gt_id = None

    res = model.predict(str(img_path), imgsz=224, verbose=False)
    r0 = res[0]
    # get top index and conf defensively
    top_idx = None
    top_conf = None
    probs = getattr(r0, 'probs', None)
    if probs is not None and hasattr(probs, 'top1'):
        top_idx = int(probs.top1)
        top_conf = float(probs.top1conf)
    else:
        top_idx = int(getattr(r0, 'top1', 0))
        top_conf = float(getattr(r0, 'top1conf', 0.0))

    names = getattr(r0, 'names', {}) or {}
    raw_label = names[top_idx] if top_idx in names else str(top_idx + 1)
    try:
        class_id = int(raw_label)
    except Exception:
        class_id = top_idx + 1

    ok = (gt_id is not None and class_id == gt_id)

    print(f"[{datetime.now().isoformat()}] img={img_path.name} gt={gt_folder} | top_idx={top_idx} raw_label={raw_label} -> class_id={class_id} conf={top_conf:.3f} MATCH={ok}")

print("Done.")
