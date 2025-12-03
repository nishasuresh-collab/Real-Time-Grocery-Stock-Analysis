import os
import torch
from ultralytics import YOLO
from multiprocessing import freeze_support

def train_model():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Training on: {device.upper()} ({torch.cuda.get_device_name(0) if device=='cuda' else 'CPU only'})")

    # Load base YOLO classification model
    model = YOLO("yolov8n-cls.pt")
    # resolve an absolute path to the grozi_classification folder (containing data.yaml)
    repo_root = os.path.abspath(os.path.dirname(__file__))
    data_dir = os.path.abspath(os.path.join(repo_root, "grozi_classification"))
    data_yaml = os.path.join(data_dir, "data.yaml")
    if not os.path.exists(data_yaml):
        raise FileNotFoundError(f"data.yaml not found at {data_yaml}. Run prepare_grozi_yolo.py first.")

    print(f"Using dataset directory: {data_dir}")
    data_yaml = data_yaml.replace("\\", "/")
    # Train model
    model.train(
        # use explicit data.yaml created by prepare_grozi_yolo to guarantee numeric ordering
    # pass the dataset directory (Ultralytics will read data.yaml inside it)
    data=data_dir,
        epochs=10,
        imgsz=224,
        batch=32,
        device=device,
        workers=0,
        project="runs/classify",
        name="grozi_cls_v8n_aug",
        patience=10,
        auto_augment="randaugment",
        erasing=0.4,
        translate=0.2,
        scale=0.5,
        fliplr=0.5,
        dropout=0.2,
        verbose=True
    )

    # Validate
    print("\nRunning validation on best model...")
    metrics = model.val(data=data_dir)

    print("\nValidation Metrics Summary:")
    top1 = metrics.results_dict.get("metrics/accuracy_top1", None)
    top5 = metrics.results_dict.get("metrics/accuracy_top5", None)
    if top1 is not None:
        print(f"Top-1 Accuracy: {top1:.4f}")
    if top5 is not None:
        print(f"Top-5 Accuracy: {top5:.4f}")

    # Note: class name ordering is controlled by grozi_classification/data.yaml written by
    # prepare_grozi_yolo.py. There is no post-training remapping here because the dataset
    # config already guarantees numeric (not alphabetical) ordering of classes.

    # Export to TorchScript
    model.export(format="torchscript")
    print("Exported model to TorchScript format for inference.")

if __name__ == "__main__":
    freeze_support()
    train_model()