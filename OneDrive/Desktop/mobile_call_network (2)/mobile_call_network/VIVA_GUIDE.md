# VIVA GUIDE – Mobile Call Network using Apache Spark

## 1. One-line summary
"I used Apache Spark (PySpark) to treat mobile call records as a graph, where users are nodes and calls are edges,
and found the most-called person, the isolated users, and visualized the network."

## 2. Problem statement (tasks)
1. Load a dataset of mobile call records
2. Build a graph: nodes = users, edges = call connections
3. Find the most frequently called person
4. Find isolated users (very few calls made/received)
5. Visualize the network

## 3. Technologies used and WHY
| Technology | What it is | Why I used it |
|---|---|---|
| **Apache Spark** | Distributed data-processing engine for clusters | Splits big data across many machines and processes in parallel, so it scales to millions of calls |
| **PySpark** | Python API of Spark | Write Spark programs in Python |
| **Spark DataFrame / Spark SQL** | Table-like distributed data structure | Easy `groupBy`, `join`, `count`; optimized automatically by Spark (Catalyst optimizer) |
| **Python 3** | Programming language | Simple, works with Spark and plotting libraries |
| **NetworkX** | Python graph library | Builds and lays out the graph for drawing |
| **Matplotlib** | Plotting library | Draws and saves the images (PNG) |
| **CSV** | File format | Common format for call records |
| **Java (JVM)** | Runtime | Spark runs on the JVM, so Java must be installed |

## 4. Files in the project
- `generate_dataset.py` – creates a synthetic dataset (50,000 calls, 1,000 users) because real telecom data is private
- `data/call_records.csv` – the generated dataset
- `mobile_call_network.py` – the main Spark program (5 steps)
- `requirements.txt` – libraries to install
- `output/` – results created after running (images + CSVs)

Dataset columns: `call_id, caller_id, receiver_id, call_start, duration_sec`

## 5. What I did – step by step
**Dataset:** Generated users U00001–U01000. A few users were made very popular (weights 1, 1/2, 1/3 ...) and 100 "rare" users were given only 1 call each
so the program has isolated users to find.

**Step 1 – Load:** `spark.read.csv(...)` reads the file into a DataFrame. I cleaned it: removed null users, self-calls and 0-second calls.
`.cache()` keeps data in memory for reuse.

**Step 2 – Build graph:**
- *Vertices* = distinct users (union of caller and receiver columns, then `distinct()`).
- *Edges* = `groupBy(caller, receiver)` giving number of calls and total duration for each pair. Direction is caller → receiver.
- *Degree table* = for each user, calls made, calls received, total (using left joins + `fillna(0)`).

**Step 3 – Most called person:** `groupBy(receiver_id).count()` then `orderBy(desc)`. This equals the **in-degree** of a node in the graph.

**Step 4 – Isolated users:** total calls = made + received (this is the **degree** of the node). Users with degree ≤ 2 are isolated / low-activity.

**Step 5 – Visualize:** Spark picks the 150 busiest users + 30 isolated users, only this small sample is collected to the driver,
NetworkX makes the graph, Matplotlib draws it. Red = most called, orange = isolated, blue = normal; node size = calls received.
Also made a bar chart (top 10) and a histogram of call activity.

## 6. Key concepts to know
- **Graph:** set of nodes (vertices) and edges. Here a *directed* graph (caller → receiver).
- **In-degree:** calls received. **Out-degree:** calls made. **Degree:** both together.
- **Isolated node:** a node with no (or very few) connections.
- **Cluster computing:** many computers working together as one system. Spark = driver + executors (workers) coordinated by a cluster manager.
- **Driver:** runs the main program and plans the work. **Executors:** do the actual processing on data partitions.
- **Partition:** a chunk of data processed by one task in parallel.
- **Transformation vs Action:** transformations (`filter`, `groupBy`, `join`) are *lazy* – nothing runs yet. Actions (`count`, `show`, `collect`) trigger execution.
- **Lazy evaluation + DAG:** Spark builds a plan (Directed Acyclic Graph) and optimizes it before running.
- **cache():** stores a DataFrame in memory so it is not recomputed.
- **collect():** brings data from the cluster to the driver (only use on small data).
- **local[\*]:** run Spark on one machine using all cores (simulated cluster).

## 7. Likely viva questions and answers
**Q1. What is Apache Spark?**
An open-source distributed computing engine that processes large datasets in parallel across a cluster, mostly in memory, which makes it much faster than Hadoop MapReduce.

**Q2. Why Spark and not plain Python/Pandas?**
Pandas works on one machine and its memory. Real call records are billions of rows. Spark splits the data into partitions across machines and processes them in parallel.

**Q3. How did you represent the graph?**
Two DataFrames: vertices (unique users) and edges (caller, receiver, number of calls, duration). This is the same model GraphFrames/GraphX use.

**Q4. Why not GraphFrames / GraphX?**
GraphFrames needs an extra package and GraphX is Scala-only. Plain DataFrames are enough for degree-based tasks and are easier to run. GraphFrames could be added for PageRank or connected components.

**Q5. How did you find the most frequently called person?**
Grouped calls by `receiver_id`, counted rows, sorted descending, took the top. That is the node with highest in-degree.

**Q6. How do you define an isolated user?**
A user whose total calls (made + received) ≤ a threshold (2). The threshold is a parameter and can be changed.

**Q7. What is lazy evaluation?**
Spark does not execute transformations immediately; it records them in a DAG and runs only when an action like `count()` or `show()` is called, allowing optimization.

**Q8. Difference between transformation and action?**
Transformation creates a new DataFrame (lazy): `filter`, `groupBy`, `join`. Action returns a result/triggers computation: `count`, `show`, `collect`.

**Q9. Why did you use cache()?**
`calls`, `edges` and `degrees` are used several times. Caching avoids re-reading and recomputing them.

**Q10. Why `collect()` only on a sample for the plot?**
`collect()` moves data to the driver's memory. Drawing a million nodes is impossible and would crash the driver, so Spark reduces the data first.

**Q11. What does a left join do and why use it?**
Keeps all rows from the left table. Some users never make calls (or never receive), so an inner join would drop them. Nulls are then replaced with 0 via `fillna(0)`.

**Q12. How does it run on a real cluster?**
Replace local mode with `spark-submit --master spark://host:7077 mobile_call_network.py` (or YARN/Kubernetes) and put the data on HDFS/S3.

**Q13. Where did your data come from?**
Generated synthetic data (`generate_dataset.py`) with the same schema as real call detail records (CDR), because real data is confidential. Any real CSV with the same columns works.

**Q14. What are the limitations?**
Visualization shows only a sample; the isolated definition is simple (count-based); synthetic data may not reflect real behavior; direction/time not deeply analysed.

**Q15. Future work?**
PageRank to find influential users, connected components for communities, time-window analysis (peak hours), fraud/spam caller detection, streaming with Spark Structured Streaming.

## 8. How to run (demo steps)
```bash
pip install -r requirements.txt        # also needs Java installed
python generate_dataset.py             # creates data/call_records.csv
python mobile_call_network.py          # runs the whole analysis
```
Show the console output (steps 1–5) and the images in `output/` during the viva.

## 9. Sample talking script (1 minute)
"My project analyses a mobile call network with Apache Spark. I generated call records and loaded them into a Spark DataFrame.
I built a graph where each user is a node and each caller-to-receiver pair is an edge. Using groupBy I found the most frequently called person,
which is the node with the highest in-degree. I found isolated users as those with very few total calls. Finally, I visualized the network with
NetworkX and Matplotlib, colouring the most-called users red and isolated users orange. Spark makes this scalable because the data is processed in
parallel across a cluster."
