# 🧭 30-Day Spark Roadmap (Databricks-focused)

👉 **Environment:** Databricks Community Edition  
👉 **Storage layer later:** Delta Lake

---

## 🟢 WEEK 1 — Foundations (Days 1–7)

👉 **Goal:** Understand Spark + DataFrames (core mental model)

**Day 1–2 — Introduction to Apache Spark**
- What Spark is (distributed compute)
- Driver vs Executors
- Lazy evaluation

**Day 3 — Spark Runtime Architecture**
- Jobs, stages, tasks
- Basic execution flow

**Day 4 — Spark in Databricks**
- Notebooks
- Clusters (conceptually)
- Running your first job

**Day 5–6 — DataFrames & SQL**
- From curriculum: Introduction to Spark DataFrames and SQL
- Hands-on:
  - Load CSV
  - `select`, `filter`, `withColumn`
  - Basic SQL queries

**Day 7 — Reading & Writing Data**
- CSV / JSON / Parquet
- Write outputs

💡 **Mini-task:** Read raw data → write cleaned version

---

## 🟡 WEEK 2 — Core Transformations (Days 8–14)

👉 **Goal:** Translate ETL logic into Spark

**Day 8 — Distributed Programming Fundamentals**
- Transformations vs actions
- Immutability

**Day 9–10 — Basic ETL with DataFrame API**
- From curriculum: Basic ETL with the DataFrame API — Flight Data ETL
- Hands-on:
  - Clean messy dataset
  - Cast types
  - Handle nulls

**Day 11–12 — Grouping & Aggregations**
- From curriculum: Grouping and Aggregating Data
- Hands-on:
  - `groupBy`, `agg`
  - Multiple aggregations
  - Sorting results

💡 **Lab idea:** Revenue by country / Avg transactions per user

**Day 13 — Relational Operations**
- Joins (inner, left, right)
- Hands-on: Join customers + transactions

**Day 14 — Complex Data Types**
- Arrays, structs
- Hands-on: Explode arrays, work with nested JSON

---

## 🔵 WEEK 3 — Real Data + Streaming (Days 15–21)

👉 **Goal:** Work like a real data engineer

**Day 15–16 — Analyze Transaction Data**
- From curriculum: Analyzing Transaction Data with DataFrames
- Build: KPIs (total revenue, top users, trends)

**Day 17 — Mini Project (Batch Pipeline)**
- End-to-end: Read raw → Clean → Join → Aggregate → Write output

**Day 18 — Intro to Streaming**
- From curriculum: Introduction to Stream Processing
- Concepts: Batch vs streaming

**Day 19–20 — Structured Streaming**
- From curriculum: Spark Structured Streaming
- Hands-on:
  - Read streaming data (rate source or files)
  - Simple aggregation

**Day 21 — Window Aggregations (Streaming)**
- Sliding windows
- Time-based aggregations

💡 **Example:** Events per minute

---

## 🔴 WEEK 4 — Optimization + Delta (Days 22–30)

👉 **Goal:** Move from "it works" → "it's production-ready"

**Day 22 — Spark + Databricks Deep Dive**
- Execution plan (`explain()`)
- DAG understanding

**Day 23–24 — Delta Lake**
- From curriculum: Using Apache Spark with Delta Lake
- Hands-on:
  - Save as Delta
  - Update / merge
  - Time travel (optional)

**Day 25 — Optimization Basics**
- From curriculum: Optimizing Apache Spark
- Learn: Partitioning, Caching, Shuffle basics

**Day 26 — Optimization Lab**
- Compare: With vs without cache, different join strategies

**Day 27–28 — Final Project**
- Combine everything: Batch + streaming (optional), Delta output, Aggregations, Clean pipeline
- Example: E-commerce pipeline — ingest → clean → join → KPIs

**Day 29 — Review Weak Areas**
- Joins, Aggregations, Execution

**Day 30 — Certification-style Practice**
- Focus on: DataFrame API, Transformations, Spark behavior
