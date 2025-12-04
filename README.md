Real-Time Grocery Stock Analysis

Requirements
- Python 3.8 or newer
- pip
- Recommended Python packages: ultralytics, opencv-python, pandas, pyyaml
- (Optional) PyTorch installed for model training/inference. Install PyTorch following the instructions at https://pytorch.org for your platform.

Install dependencies (example, run in PowerShell)
```powershell
pip install --upgrade pip
pip install ultralytics opencv-python pandas pyyaml
```

Basic steps to run
1. Prepare data (if required):
```powershell
python prepare_grozi_yolo.py
```
This converts the Grozi dataset into YOLO-style files (labels and `data.yaml`) used for training.

3. finetune yolo (if required):
```powershell
python train_grozi_yolo.py
```
This finetunes yolo on the 120 grozi classes.

2. Run the real-time pipeline:
```powershell
python src\realtime_grozi_pipeline.py
```
This runs the pipeline that reads input feeds and performs detection/classification.

Files:

- `prepare_grozi_yolo.py` : Convert Grozi dataset into YOLO format and produce `data.yaml` and label files for yolo finetuning.
- `grozi_classification/data.yaml` : Dataset configuration used by YOLO for train/val paths and class names.
- `grozi_classification/class_mapping.csv` : Mapping between class ids and class names used for classification.
- `train_yolo_grozi.py` : code to finetune yolo

Files in `src` with realtime pipeline:
- `constants.py` : Holds configuration constants used across the project.
- `feed_simulator.py` : Simulates camera/feed input for testing the pipeline.
- `helper.py` : Utility functions used by the pipeline (IO, formatting, small helpers).
- `realtime_grozi_pipeline.py` : Main pipeline that wires detection, classification and output streaming.

TODO:

Current `realtime_grozi_pipeline.py` stores json outputs in `/stream_output_json`. The code should be changed to continue with real-time json streaming in PySpark.
