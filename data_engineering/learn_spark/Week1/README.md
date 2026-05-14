# Week 1 — Apache Spark Fundamentals

Core concepts for the Databricks Certified Data Engineer Associate exam.

| Day | Notebook | Topics |
|-----|----------|--------|
| 1 | `day_1.ipynb` | What is Spark, SparkSession, DataFrame basics, RDD → DataFrame → Dataset |
| 2 | `day_2.ipynb` | Cluster architecture (driver / executors), Jobs → Stages → Tasks, narrow vs wide transformations, deployment modes |
| 3 | `day_3.ipynb` | Lazy evaluation, DAG, Catalyst optimizer, `explain()`, Adaptive Query Execution |
| 4 | `day_4.ipynb` | Column operations — `col()`, `lit()`, `expr()`, `withColumn()`, `cast()`, `when()`/`otherwise()` |
| 5 | `day_5.ipynb` | Filtering (`&` `\|` `~`), sorting, null handling (`dropna`, `fillna`), `distinct()` vs `dropDuplicates()` |
| 6 | `day_6.ipynb` | Spark SQL, `createOrReplaceTempView`, global temp views, `spark.sql()`, catalog |
| 7 | `day_7.ipynb` | Reading & writing CSV / JSON / Parquet, `StructType`, write modes, `coalesce()` vs `repartition()`, `partitionBy()` |
| 7bis | `day_7bis_databricksql.ipynb` | Databricks metastore, managed vs external tables, `CREATE TABLE USING`, CTAS, persistent views, `spark.table()` |

## Interactive Resources

| Resource | Link |
|----------|------|
| Quiz (16 cert + 69 daily) | [week1_quiz.html](https://cmedar.github.io/count-electric/data_engineering/learn_spark/Week1/week1_quiz.html) |
| Recap cheatsheet | [week1_recap.html](https://cmedar.github.io/count-electric/data_engineering/learn_spark/Week1/week1_recap.html) |
