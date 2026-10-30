"""
MOBILE CALL NETWORK ANALYSIS USING APACHE SPARK (PySpark)
=========================================================
Idea: treat every mobile user as a NODE and every call as an EDGE (a line
between caller and receiver). Together they form a GRAPH. We then use Spark
to analyse this graph in a distributed (cluster) way.

Steps in this file:
  STEP 1  Load the call records dataset
  STEP 2  Construct the graph (nodes + edges)
  STEP 3  Find the most frequently called person
  STEP 4  Find isolated users (very few calls)
  STEP 5  Visualize the network
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F          # F = Spark's built-in functions (count, sum, ...)

# ---------------------------------------------------------------------------
# SETTINGS (you can change these)
# ---------------------------------------------------------------------------
INPUT_FILE = "data/call_records.csv"   # dataset path
OUTPUT_DIR = "output"                  # where results/graphs are saved
TOP_N = 10                             # how many "most called" users to show
ISOLATED_THRESHOLD = 2                 # user with <= 2 total calls = isolated
MAX_NODES_TO_DRAW = 150                # drawing 1000s of nodes is unreadable

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# START SPARK
# SparkSession is the entry point to Spark. "local[*]" = use all CPU cores of
# this computer as if they were cluster workers. On a real cluster you pass
# --master spark://host:7077 using spark-submit instead.
# ---------------------------------------------------------------------------
spark = (SparkSession.builder
         .appName("MobileCallNetwork")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")      # hide noisy logs

# ===========================================================================
# STEP 1: LOAD THE DATASET
# Columns: call_id, caller_id, receiver_id, call_start, duration_sec
# ===========================================================================
print("\n===== STEP 1: LOAD DATASET =====")
calls = spark.read.csv(INPUT_FILE, header=True, inferSchema=True)

# Data cleaning: remove rows with missing users, calls to oneself, 0-sec calls
calls = (calls
         .dropna(subset=["caller_id", "receiver_id"])
         .filter(F.col("caller_id") != F.col("receiver_id"))
         .filter(F.col("duration_sec") > 0))

calls.cache()          # keep in memory because we reuse it many times (faster)
print("Total valid call records:", calls.count())
calls.show(5)

# ===========================================================================
# STEP 2: CONSTRUCT THE GRAPH
# A graph needs two tables:
#   VERTICES (nodes): the list of unique users
#   EDGES: connections between users
# ===========================================================================
print("\n===== STEP 2: BUILD GRAPH =====")

# Nodes = everyone who appears as a caller OR a receiver (distinct = no repeats)
vertices = (calls.select(F.col("caller_id").alias("id"))
            .union(calls.select(F.col("receiver_id").alias("id")))
            .distinct())

# Edges = one line per (caller -> receiver) pair, with how many calls and total talk time
edges = (calls.groupBy(F.col("caller_id").alias("src"),
                       F.col("receiver_id").alias("dst"))
         .agg(F.count("*").alias("num_calls"),
              F.sum("duration_sec").alias("total_duration")))

vertices.cache()
edges.cache()
print("Nodes (users):", vertices.count())
print("Edges (unique caller->receiver connections):", edges.count())
print("Strongest connections:")
edges.orderBy(F.desc("num_calls")).show(5)

# Degree table: for each user, how many calls made / received / total
calls_made = calls.groupBy("caller_id").agg(F.count("*").alias("calls_made"))
calls_received = calls.groupBy("receiver_id").agg(F.count("*").alias("calls_received"))

degrees = (vertices
           .join(calls_made, vertices.id == calls_made.caller_id, "left")
           .join(calls_received, vertices.id == calls_received.receiver_id, "left")
           .select("id", "calls_made", "calls_received")
           .fillna(0)                                   # no calls -> 0 instead of null
           .withColumn("total_calls", F.col("calls_made") + F.col("calls_received")))
degrees.cache()

# ===========================================================================
# STEP 3: MOST FREQUENTLY CALLED PERSON
# Group all calls by receiver, count them, sort highest first.
# ===========================================================================
print("\n===== STEP 3: MOST FREQUENTLY CALLED PERSON =====")
top_called = (calls.groupBy("receiver_id")
              .agg(F.count("*").alias("times_called"),
                   F.countDistinct("caller_id").alias("unique_callers"))
              .orderBy(F.desc("times_called"))
              .limit(TOP_N))
top_called.cache()
top_called.show(TOP_N)

best = top_called.first()
print(f">>> Most frequently called person: {best['receiver_id']} "
      f"({best['times_called']} calls from {best['unique_callers']} different callers)")

# ===========================================================================
# STEP 4: ISOLATED USERS
# Isolated = total calls (made + received) is very small (<= threshold).
# ===========================================================================
print("\n===== STEP 4: ISOLATED USERS =====")
isolated = (degrees.filter(F.col("total_calls") <= ISOLATED_THRESHOLD)
            .orderBy("total_calls"))
isolated.cache()
print(f"Users with <= {ISOLATED_THRESHOLD} calls: {isolated.count()}")
isolated.show(15)

# Save the small result tables as CSV files
top_called.toPandas().to_csv(f"{OUTPUT_DIR}/most_called.csv", index=False)

isolated.toPandas().to_csv(f"{OUTPUT_DIR}/isolated_users.csv", index=False)

# ===========================================================================
# STEP 5: VISUALIZE
# Spark handles big data; Matplotlib/NetworkX cannot draw millions of nodes.
# So we let Spark pick the busiest users + some isolated ones, bring ONLY that
# small sample to the driver with .collect(), and draw it.
# ===========================================================================
print("\n===== STEP 5: VISUALIZATION =====")
import matplotlib
matplotlib.use("Agg")                 # save images to files (no pop-up window needed)
import matplotlib.pyplot as plt
import networkx as nx

busiest_ids = [r["id"] for r in degrees.orderBy(F.desc("total_calls"))
               .limit(MAX_NODES_TO_DRAW).collect()]
isolated_ids = [r["id"] for r in isolated.limit(30).collect()]
top5_ids = [r["receiver_id"] for r in top_called.limit(5).collect()]
selected = set(busiest_ids) | set(isolated_ids)

# Keep only edges where both ends are in our sample
sample_edges = (edges.filter(F.col("src").isin(selected) & F.col("dst").isin(selected))
                .collect())
received_count = {r["id"]: r["calls_received"]
                  for r in degrees.filter(F.col("id").isin(selected)).collect()}

# Build the graph in NetworkX
G = nx.Graph()
G.add_nodes_from(selected)
for e in sample_edges:
    G.add_edge(e["src"], e["dst"], weight=e["num_calls"])

# Colours: red = top 5 called, orange = isolated, blue = normal
colors = []
for n in G.nodes:
    if n in top5_ids:
        colors.append("red")
    elif n in isolated_ids:
        colors.append("orange")
    else:
        colors.append("steelblue")
sizes = [30 + 5 * received_count.get(n, 0) for n in G.nodes]   # bigger = more calls received

# Figure 1: the network graph
plt.figure(figsize=(14, 11))
pos = nx.spring_layout(G, seed=7, k=0.35)       # decides where each node sits
nx.draw_networkx_edges(G, pos, alpha=0.2)
nx.draw_networkx_nodes(G, pos, node_color=colors, node_size=sizes)
nx.draw_networkx_labels(G, pos, labels={n: n for n in top5_ids}, font_size=8)
plt.title("Mobile Call Network  (red = most called, orange = isolated, blue = normal)")
plt.axis("off")
plt.savefig(f"{OUTPUT_DIR}/call_network.png", dpi=150, bbox_inches="tight")
plt.close()

# Figure 2: bar chart of top called users
top_rows = top_called.collect()
plt.figure(figsize=(9, 5))
plt.barh([r["receiver_id"] for r in top_rows][::-1],
         [r["times_called"] for r in top_rows][::-1], color="red")
plt.xlabel("Number of times called")
plt.title(f"Top {TOP_N} Most Frequently Called Users")
plt.savefig(f"{OUTPUT_DIR}/top_called.png", dpi=150, bbox_inches="tight")
plt.close()

# Figure 3: how many users have how many calls
hist = degrees.groupBy("total_calls").count().orderBy("total_calls").collect()
plt.figure(figsize=(9, 5))
plt.bar([r["total_calls"] for r in hist], [r["count"] for r in hist])
plt.yscale("log")
plt.xlabel("Total calls (made + received)")
plt.ylabel("Number of users (log scale)")
plt.title("Call Activity Distribution")
plt.savefig(f"{OUTPUT_DIR}/activity_distribution.png", dpi=150, bbox_inches="tight")
plt.close()

print(f"Saved 3 images and 2 CSV result folders in '{OUTPUT_DIR}/'")
spark.stop()
