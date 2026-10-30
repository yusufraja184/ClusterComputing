# Mobile Call Network Analysis – Apache Spark

Cluster-computing project that analyses mobile call records as a graph using PySpark.

See **REPORT_GUIDE.md**, **CODE_EXPLANATION.md** (line-by-line) and **VIVA_GUIDE.md** for a full explanation, technologies and viva Q&A.

## Tasks covered
| # | Task | Where |
|---|------|-------|
| 1 | Load call records dataset | Step 1 |
| 2 | Build graph (nodes = users, edges = calls) | Step 2 |
| 3 | Most frequently called person | Step 3 |
| 4 | Isolated / low-activity users | Step 4 |
| 5 | Visualize the network | Step 5 |

## Dataset format (CSV)
`call_id, caller_id, receiver_id, call_start, duration_sec`
Use your own file with this schema, or generate synthetic data.

## Setup
```bash
pip install -r requirements.txt     # needs Java 8/11/17 installed
python generate_dataset.py --users 1000 --records 50000
```

## Run
```bash
# local mode
python mobile_call_network.py

# on a Spark cluster
spark-submit --master spark://<master>:7077 mobile_call_network.py \
    --input hdfs:///data/call_records.csv --output /tmp/out
```
Settings (TOP_N, ISOLATED_THRESHOLD, ...) are at the top of `mobile_call_network.py`.

## Method
- **Graph**: `vertices` = distinct users; `edges` = one directed edge per (caller, receiver)
  with `num_calls` and `total_duration`. Everything is Spark DataFrames, so it scales across the cluster.
- **Most called person**: group calls by `receiver_id`, count, sort descending.
- **Isolated users**: `total_calls = calls_made + calls_received`; users with
  `total_calls <= threshold` (default 2) are flagged.
- **Visualization**: Spark aggregates, then only the busiest `MAX_NODES_TO_DRAW` users
  (plus some isolated ones) are collected to the driver and drawn with NetworkX/Matplotlib.

## Output (`output/`)
- `call_network.png` – network graph (red = top called, orange = isolated, blue = regular)
- `top_called.png` – top-10 bar chart
- `activity_distribution.png` – histogram of calls per user
- `most_called.csv`, `isolated_users/` – result tables

## Possible extensions
GraphFrames (PageRank, connected components), time-window analysis, community detection.
