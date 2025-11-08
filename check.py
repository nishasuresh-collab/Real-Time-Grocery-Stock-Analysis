from ultralytics import YOLO
model = YOLO("runs/classify/grozi_cls_v8n_aug/weights/best.pt")
print(model.names)

import os

train_root = "grozi_classification/train"
classes = sorted(os.listdir(train_root), key=lambda x: int(x))
print(classes[:20]) 
print("Total classes:", len(classes))
