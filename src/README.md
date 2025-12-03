
# 📦 Real-Time Grocery Stock Analysis — Experiment Pipeline

This project simulates **multiple camera feeds**, generates real-time **product classification JSON events**, and processes them using **Spark Structured Streaming**.
All experiments automatically measure **throughput**, **latency**, and **processing time**, then generate comparison plots.

---

## 🔄 Pipeline Overview

### **1. Producer (Inference)**

`realtime_grozi_pipeline.py`:

* Runs YOLO classification on 10 simulated feeds
* Writes JSON events into:

  ```
  experiments/<run_id>/json_stream/
  ```

### **2. Spark Stream Processor**

`pyspark_stream_processor.py`:

* Reads JSON files as a real-time stream
* Computes micro-batch metrics
* Saves metrics into:

  ```
  experiments/<run_id>/batch_metrics.log
  ```

### **3. Plotting**

`plot_experiments.py`:

* Loads metrics
* Generates per-experiment graphs
* Creates comparison plots across different rates, cores, and triggers
* Plots are saved under:

  ```
  src/plots/
  ```

---

## ▶️ Running All Experiments

Clear old runs:

```bash
rm -rf experiments
mkdir experiments
```

Run all experiment configurations:

```bash
python3 src/run_all_experiments.py
```

This runs combinations of:

* **Rates:** 0.2, 0.5, 1.0
* **Cores:** 1 and 8
* **Trigger:** 2 seconds

Each experiment creates its own folder inside `experiments/`.

---

## 📈 Generating Plots

```bash
python3 src/plot_experiments.py \
    --exp-dir experiments \
    --plots-dir src/plots \
    --latest
```

Important comparison plots will be written to:

```
src/plots/comparisons/
```

---

## 🧪 Validating an Experiment

Check that the producer generated JSON:

```bash
ls experiments/<run_id>/json_stream | head
```

Check Spark metrics:

```bash
cat experiments/<run_id>/batch_metrics.log
```

If both contain data, the run succeeded.

---

## ➕ Adding More Experiments

Edit the arrays inside:

```
src/run_all_experiments.py
```

For example:

```python
RATES = [0.1, 0.2, 0.5, 1.0, 2.0]
CORES = [1, 2, 4, 8]
TRIGGERS = ["1 second", "2 seconds", "5 seconds"]
```

Then run:

```bash
python3 src/run_all_experiments.py
python3 src/plot_experiments.py --exp-dir experiments --plots-dir src/plots --latest
```

---

## 📊 What the Current Results Show

* **Spark scales well**: throughput almost doubles from 1 → 8 cores
* **Producer rate doesn’t dominate**: throughput remains stable across rates
* **System bounded more by compute** than by input rate

