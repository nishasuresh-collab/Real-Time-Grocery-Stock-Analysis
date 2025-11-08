import os
import shutil
import random
import csv

# === PATHS ===
root = "inSitu"
index_file = "UPC_index.txt"
output = "grozi_classification"

os.makedirs(f"{output}/train", exist_ok=True)
os.makedirs(f"{output}/val", exist_ok=True)

# === READ INDEX FILE ===
product_map = {}
with open(index_file, "r", encoding="utf-8") as f:
    lines = [line.strip() for line in f if line.strip()]
i = 1
while i < len(lines):
    try:
        idx = int(lines[i])
        upc = lines[i + 1]
        name = lines[i + 2]
        product_map[idx] = {"upc": upc, "name": name}
        i += 3
    except:
        i += 1

print(f"Loaded {len(product_map)} products.")

# === SPLIT IMAGES CONSISTENTLY ===
train_ratio = 0.8
for idx in sorted(product_map.keys()):  # numeric order from index file
    folder = str(idx)
    src = os.path.join(root, folder, "video")
    if not os.path.isdir(src):
        print(f"Skipping {folder} (missing video folder)")
        continue

    images = [os.path.join(src, f) for f in os.listdir(src) if f.endswith(".png")]
    if not images:
        print(f"Skipping {folder} (no images)")
        continue

    random.shuffle(images)
    split = int(len(images) * train_ratio)

    for i, path in enumerate(images):
        target = "train" if i < split else "val"
        dest_dir = os.path.join(output, target, folder)  # folder = numeric string
        os.makedirs(dest_dir, exist_ok=True)
        shutil.copy(path, dest_dir)

print("Dataset prepared with consistent class ordering.")

# === SAVE CLASS MAPPING (for reference) ===
mapping_file = os.path.join(output, "class_mapping.csv")
with open(mapping_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["class_id", "product_name", "upc"])
    for idx, pdata in sorted(product_map.items()):
        writer.writerow([idx, pdata["name"], pdata["upc"]])

print(f"Mapping saved to {mapping_file}")

# === WRITE data.yaml to fix class ordering for YOLO training ===
data_yaml = os.path.join(output, "data.yaml")
with open(data_yaml, "w", encoding="utf-8") as f:
    f.write(f"path: {os.path.abspath(output)}\n\n")
    f.write("train: train\n")
    f.write("val: val\n\n")
    f.write(f"nc: {len(product_map)}\n\n")
    f.write("names:\n")
    # Ensure numeric (not alphabetical) ordering by writing explicit index->folder mapping
    for i, class_id in enumerate(sorted(product_map.keys())):
        # map dataset index (0-based) to the numeric folder name (as a string)
        f.write(f"  {i}: \"{str(class_id)}\"\n")

print(f"Wrote dataset config to {data_yaml} (enforces numeric class ordering)")
