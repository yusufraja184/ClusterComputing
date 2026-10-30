# CODE EXPLANATION (line by line) – what was done where

## Big picture: which tool is used where
| Part | File | Tool used | What it does |
|---|---|---|---|
| Create dataset | `generate_dataset.py` | **Plain Python only** (no Spark) | Makes a fake CSV of 50,043 call records |
| Load data | `mobile_call_network.py` Step 1 | **PySpark** | Reads CSV into a Spark DataFrame, cleans it |
| Build graph | Step 2 | **PySpark** | Creates nodes (users) and edges (calls) |
| Most called person | Step 3 | **PySpark** | groupBy + count + sort |
| Isolated users | Step 4 | **PySpark** | filter users with few calls |
| Visualization | Step 5 | **Python libraries** NetworkX + Matplotlib | Draws graph and charts from a small sample |

Flow: `generate_dataset.py` (Python) -> `call_records.csv` -> `mobile_call_network.py` (Spark) -> `output/` (images + CSV).

---
# PART A: generate_dataset.py (Plain Python, no Spark)

**Why do we need it?** Real telecom call data is private. So we create our own file with the same columns:
`call_id, caller_id, receiver_id, call_start, duration_sec`

```python
import random
import os
```
`random` gives random numbers/choices. `os` lets us create a folder.

```python
NUM_USERS = 1000
NUM_CALLS = 50000
```
Settings: 1000 users, 50000 calls (normal users).

```python
users = []
for i in range(1, NUM_USERS + 1):
    users.append("U" + str(i).zfill(5))
```
Makes an empty list, loops from 1 to 1000, and adds ids like `U00001`. `zfill(5)` pads with zeros to 5 digits.

```python
normal_users = users[:900]
rare_users = users[900:]
```
First 900 users are normal (make many calls). Last 100 are "rare" users who will get only 1 call -> they become our isolated users.

```python
weights = []
for i in range(900):
    weights.append(1 / (i + 1))
```
Gives each normal user a popularity: user 1 -> 1, user 2 -> 0.5, user 3 -> 0.33 ... So U00001 will be called the most. (This makes the "most called person" result interesting.)

```python
def random_time():
    ...
    return f"2026-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
```
A function that builds a random date-time text like `2026-02-25 14:52:07`. `:02d` means 2 digits with leading zero.

```python
os.makedirs("data", exist_ok=True)
file = open("data/call_records.csv", "w")
file.write("call_id,caller_id,receiver_id,call_start,duration_sec\n")
```
Creates folder `data`, opens the CSV for writing, writes the header row (column names).

```python
for _ in range(NUM_CALLS):
    caller = random.choice(normal_users)
    receiver = random.choices(normal_users, weights)[0]
    if caller == receiver:
        continue
    duration = random.randint(5, 1800)
    file.write(f"{call_id},{caller},{receiver},{random_time()},{duration}\n")
    call_id += 1
```
Repeats 50000 times: pick a random caller (equal chance), pick a receiver using weights (popular users more likely), skip if both same, random duration 5 s to 30 min, write one row, increase call_id.

```python
for rare in rare_users:
    other = random.choice(normal_users)
    if random.random() < 0.5:
        caller, receiver = rare, other
    else:
        caller, receiver = other, rare
    file.write(...)
```
For each of the 100 rare users write exactly ONE call: half the time the rare user is the caller, half the time the receiver.

```python
file.close()
```
Saves and closes the file.

**Result:** `data/call_records.csv` with ~50,043 rows. 100 users have exactly 1 call (isolated), U00001 is the most called.

---
# PART B: mobile_call_network.py (PySpark)

## Imports and settings
```python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
```
`SparkSession` = entry point to Spark. `F` = Spark's built-in functions (`F.count`, `F.sum`, `F.col`, `F.desc`).

```python
INPUT_FILE = "data/call_records.csv"
OUTPUT_DIR = "output"
TOP_N = 10
ISOLATED_THRESHOLD = 2
MAX_NODES_TO_DRAW = 150
os.makedirs(OUTPUT_DIR, exist_ok=True)
```
Settings in one place: input file, output folder, show top 10, isolated means <= 2 calls, draw max 150 nodes (more is unreadable). Creates the output folder.

## Start Spark
```python
spark = (SparkSession.builder.appName("MobileCallNetwork").master("local[*]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
```
- `appName` = name of the application.
- `master("local[*]")` = run on this computer using all CPU cores (acts like a mini cluster). On a real cluster you give the cluster address instead.
- `getOrCreate()` = start Spark (or reuse running one).
- `setLogLevel("ERROR")` = only show errors, hide noise.

## STEP 1: Load dataset
```python
calls = spark.read.csv(INPUT_FILE, header=True, inferSchema=True)
```
Reads the CSV into a **DataFrame** (a distributed table). `header=True` = first row is column names. `inferSchema=True` = Spark guesses the data types (numbers, text).

```python
calls = (calls.dropna(subset=["caller_id", "receiver_id"])
              .filter(F.col("caller_id") != F.col("receiver_id"))
              .filter(F.col("duration_sec") > 0))
```
Data cleaning: remove rows with missing caller/receiver; remove self-calls; remove calls of 0 seconds.

```python
calls.cache()
print("Total valid call records:", calls.count())
calls.show(5)
```
`cache()` keeps the data in memory since we use it many times. `count()` counts rows (an *action*, so Spark actually runs now). `show(5)` prints 5 rows.

## STEP 2: Construct the graph
A graph = **nodes** + **edges**.

```python
vertices = (calls.select(F.col("caller_id").alias("id"))
            .union(calls.select(F.col("receiver_id").alias("id")))
            .distinct())
```
Nodes: take all callers, rename the column to `id`; take all receivers, rename to `id`; `union` joins both lists; `distinct()` removes duplicates -> one row per user.

```python
edges = (calls.groupBy(F.col("caller_id").alias("src"), F.col("receiver_id").alias("dst"))
         .agg(F.count("*").alias("num_calls"), F.sum("duration_sec").alias("total_duration")))
```
Edges: group rows by (caller, receiver) pair. For each pair count the calls (`num_calls`) and add the talk time (`total_duration`). `src` = source (caller), `dst` = destination (receiver). Direction: caller -> receiver.

```python
vertices.cache(); edges.cache()
print("Nodes (users):", vertices.count())
print("Edges ...:", edges.count())
edges.orderBy(F.desc("num_calls")).show(5)
```
Cache, print the number of nodes and edges, show the 5 strongest connections (pairs with the most calls).

```python
calls_made = calls.groupBy("caller_id").agg(F.count("*").alias("calls_made"))
calls_received = calls.groupBy("receiver_id").agg(F.count("*").alias("calls_received"))
```
Two small tables: how many calls each user made, and how many each user received.

```python
degrees = (vertices
           .join(calls_made, vertices.id == calls_made.caller_id, "left")
           .join(calls_received, vertices.id == calls_received.receiver_id, "left")
           .select("id", "calls_made", "calls_received")
           .fillna(0)
           .withColumn("total_calls", F.col("calls_made") + F.col("calls_received")))
```
Combine into one table per user. **Left join** keeps all users even if they never made (or never received) a call; those get `null`, which `fillna(0)` turns into 0. `withColumn` adds `total_calls = made + received`. This is the **degree** of each node.

## STEP 3: Most frequently called person
```python
top_called = (calls.groupBy("receiver_id")
              .agg(F.count("*").alias("times_called"),
                   F.countDistinct("caller_id").alias("unique_callers"))
              .orderBy(F.desc("times_called"))
              .limit(TOP_N))
```
Group by receiver; count calls received (`times_called`); count different people who called them (`unique_callers`); sort highest first; keep top 10.

```python
best = top_called.first()
print(f">>> Most frequently called person: {best['receiver_id']} ...")
```
`first()` gets the top row = the answer. Expected answer on our data: **U00001**.

## STEP 4: Isolated users
```python
isolated = (degrees.filter(F.col("total_calls") <= ISOLATED_THRESHOLD).orderBy("total_calls"))
```
Keep only users whose total calls <= 2. Expected: the 100 rare users.

```python
top_called.coalesce(1).write.mode("overwrite").csv(f"{OUTPUT_DIR}/most_called", header=True)
isolated.coalesce(1).write.mode("overwrite").csv(f"{OUTPUT_DIR}/isolated_users", header=True)
```
Saves results as CSV. `coalesce(1)` = combine into one file, `mode("overwrite")` = replace old results. (Spark writes a folder containing the CSV part file.)

## STEP 5: Visualization
Why a sample? Matplotlib can't draw 1000s of nodes, and `collect()` brings data to one computer. So Spark first reduces the data.

```python
matplotlib.use("Agg")
```
Save pictures to files instead of opening windows.

```python
busiest_ids = [r["id"] for r in degrees.orderBy(F.desc("total_calls")).limit(MAX_NODES_TO_DRAW).collect()]
isolated_ids = [r["id"] for r in isolated.limit(30).collect()]
top5_ids = [r["receiver_id"] for r in top_called.limit(5).collect()]
selected = set(busiest_ids) | set(isolated_ids)
```
Pick the 150 busiest users, 30 isolated users and the top 5 most called. `collect()` brings them to normal Python lists. `selected` = busiest + isolated combined (`|` = union of sets).

```python
sample_edges = (edges.filter(F.col("src").isin(selected) & F.col("dst").isin(selected)).collect())
```
Keep only the edges whose both ends are in our sample.

```python
G = nx.Graph()
G.add_nodes_from(selected)
for e in sample_edges:
    G.add_edge(e["src"], e["dst"], weight=e["num_calls"])
```
Build the NetworkX graph: add the nodes, then add each edge.

```python
colors: red = top 5, orange = isolated, steelblue = normal
sizes = [30 + 5 * received_count.get(n, 0) for n in G.nodes]
```
Colour rule per node, and node size grows with calls received.

```python
pos = nx.spring_layout(G, seed=7, k=0.35)
nx.draw_networkx_edges / draw_networkx_nodes / draw_networkx_labels
plt.savefig(f"{OUTPUT_DIR}/call_network.png")
```
`spring_layout` decides node positions (connected nodes pulled together). Then draw edges, nodes, labels (only for top 5) and save as `call_network.png`.

Two more charts:
- `top_called.png`: horizontal bar chart of the top 10 most-called users.
- `activity_distribution.png`: histogram - how many users have how many total calls (log scale on y-axis because most users are rare, few are very busy).

```python
spark.stop()
```
Shuts Spark down.

---
# PART C: Where each concept is used (quick map for the viva)
| Concept | Where in code |
|---|---|
| DataFrame | `calls`, `vertices`, `edges`, `degrees` |
| Transformation (lazy) | `filter`, `groupBy`, `join`, `select`, `union`, `distinct`, `orderBy` |
| Action (runs now) | `count()`, `show()`, `first()`, `collect()`, `write` |
| Caching | `.cache()` |
| Graph nodes / edges | `vertices` / `edges` |
| In-degree (calls received) | `calls_received`, Step 3 |
| Degree (total calls) | `total_calls`, Step 4 |
| Cluster mode | `.master("local[*]")` |

# Expected results when you run it
- Total records ~ 50,043
- Nodes: 1000, Edges: tens of thousands (unique caller->receiver pairs)
- Most called person: U00001 (about 6,700 calls)
- Isolated users: 100 (all with 1 call)
- Output: `call_network.png`, `top_called.png`, `activity_distribution.png`, `most_called/`, `isolated_users/`

