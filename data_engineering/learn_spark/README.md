# Apache Spark Certification Roadmap (Databricks-focused)

**Target:** Databricks Certified Associate Developer for Apache Spark  
**Environment:** Databricks Community Edition  
**Language:** Python (PySpark)

---

## Exam at a Glance

| Domain | Weight |
|---|---|
| DataFrame / Dataset API | 30% |
| Apache Spark Architecture | 20% |
| Spark SQL | 20% |
| Troubleshooting & Tuning | 10% |
| Structured Streaming | 10% |
| Spark Connect | 5% |
| Pandas API on Spark | 5% |

---

## WEEK 1 — Foundations (Days 1–7)

**Goal:** Build the core mental model — architecture, lazy evaluation, DataFrame API basics, Spark SQL, reading/writing data.  
**Notebooks:** `Week1/`

| Day | Title | Key topics |
|---|---|---|
| 1 | What is Apache Spark? | Spark vs Hadoop, SparkSession, createDataFrame, show/printSchema/count, select/filter, RDD→DataFrame→Dataset |
| 2 | Runtime Architecture | Driver/Executors/Cluster Manager, Jobs→Stages→Tasks, narrow vs wide transformations, deployment modes, fault tolerance |
| 3 | Lazy Evaluation & Catalyst | Transformations vs Actions, DAG, Catalyst optimizer phases, Tungsten, explain() modes, AQE intro |
| 4 | Column Operations | col(), lit(), expr(), withColumn, withColumnRenamed, cast, when/otherwise, isNull/isNotNull, alias |
| 5 | Filtering, Sorting & Null Handling | filter/where (AND/OR/NOT), sample(), collect/take/show, distinct, dropDuplicates, orderBy, limit, dropna/fillna |
| 6 | Spark SQL | createOrReplaceTempView, global temp views, spark.sql(), mixing SQL + DataFrame API, spark.catalog |
| 7 | Reading & Writing Data | CSV/JSON/Parquet options, StructType schema, write modes, coalesce vs repartition, partitionBy, mini project |

---

## WEEK 2 — Core Transformations (Days 8–14)

**Goal:** Translate real ETL logic into Spark — the bulk of the exam (DataFrame API is 30%+ of questions).  
**Notebooks:** `Week2/`

| Day | Title | Key topics |
|---|---|---|
| 8 | Aggregations & GroupBy | groupBy, agg, count/sum/avg/min/max/countDistinct, multiple aggs, filter after groupBy, pivot |
| 9 | Joins | inner/left/right/full/cross/semi/anti joins, join on multiple keys, broadcast joins, join pitfalls |
| 10 | String & Date Functions | substring, upper/lower, split, regexp_replace, to_date, date_diff, date_add, date_format |
| 11 | Math & Collection Functions | round, abs, array/map/struct types, explode, collect_list, size, array_contains |
| 12 | UDFs (User Defined Functions) | udf() decorator, return types, performance cost, pandas UDFs (vectorised) |
| 13 | Window Functions | rank, dense_rank, row_number, lag, lead, partitionBy + orderBy in windows, running totals |
| 14 | Caching & Persistence | cache(), persist(), storage levels, unpersist(), when to cache, coalesce vs repartition deep dive |

---

## WEEK 3 — Real Data + Streaming (Days 15–21)

**Goal:** Work like a real data engineer — build pipelines, understand Spark UI, add streaming.  
**Notebooks:** `Week3/`

| Day | Title | Key topics |
|---|---|---|
| 15 | Batch Pipeline Project | End-to-end: read raw → clean → join → aggregate → write Delta/Parquet |
| 16 | Spark UI & Troubleshooting | Jobs/Stages/Tasks tabs, DAG visualiser, identify skew, spill, shuffle size |
| 17 | Memory Management & Accumulators | Driver/Executor memory, off-heap, accumulators, broadcast variables |
| 18 | Pandas API on Spark | ps.DataFrame, pandas ↔ Spark conversion, when to use, limitations |
| 19 | Intro to Structured Streaming | Batch vs streaming, readStream, writeStream, output modes, checkpointing |
| 20 | Streaming Aggregations | Windowing (tumbling/sliding), watermarks, stateful processing |
| 21 | Streaming Lab | End-to-end streaming pipeline with rate source or file source |

---

## WEEK 4 — Optimization + Delta + Exam Prep (Days 22–30)

**Goal:** Move from "it works" to "production-ready" — tune, certify.  
**Notebooks:** `Week4/`

| Day | Title | Key topics |
|---|---|---|
| 22 | Optimization Deep Dive | Partitioning strategy, skew handling, predicate pushdown, projection pushdown |
| 23 | AQE & Dynamic Partition Pruning | Adaptive Query Execution internals, DPP, coalesce shuffle partitions |
| 24 | Delta Lake | Save/read Delta, ACID transactions, schema enforcement, upsert (MERGE), time travel |
| 25 | Delta Lake Advanced | OPTIMIZE, ZORDER, VACUUM, Change Data Feed, Medallion architecture |
| 26 | Spark Connect | Spark Connect architecture, thin client model, deployment use cases |
| 27 | Unity Catalog & Governance | Data lineage, access control, three-level namespace |
| 28 | Final Project | E-commerce pipeline: ingest → clean → join → KPIs → Delta output |
| 29 | Weak Areas Review | Joins, aggregations, explain plans, common exam traps |
| 30 | Certification Practice | Full mock exam conditions — all 7 domains, time-boxed |
