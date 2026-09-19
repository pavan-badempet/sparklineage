# Why Spark Developers Waste Thousands on Compute (And Why We Built a Pre-Flight Catalyst Linter)

*Authored by Pavan Badempet*  
*Project Repository: [github.com/pavanbadempet/sparklineage](https://github.com/pavanbadempet/sparklineage)*  
*PyPI Package: [pypi.org/project/sparklineage/](https://pypi.org/project/sparklineage/)*

---

### The Silent Crisis in Modern Data Engineering

Every engineer who has ever developed data pipelines in **Apache Spark**, **Databricks**, or **AWS EMR** has experienced this recurring nightmare:

You spend three days writing a multi-stage PySpark pipeline. You run a sanity check on a small subset, and it finishes in 40 seconds. Confident in your logic, you submit the job across a 64-node cluster on production data. 

Two hours and $400 in cloud compute later, the job abruptly crashes with:
```text
java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.sql.execution.joins.SortMergeJoinExec...
```
Or worse: the job doesn't fail, but it runs for 7 hours instead of 20 minutes because an unintended Cartesian product blew up stage shuffle sizes from 10 GB to 40 TB.

When you run `df.explain(extended=True)` to diagnose what happened, you are greeted by an unreadable wall of 800 lines of nested ASCII text:

```text
== Physical Plan ==
*(5) Project [cust_id#10L, final_revenue#18]
+- *(5) BroadcastHashJoin [cust_id#10L], [cust_id#25L], Inner, BuildRight ...
   :- *(5) Project [order_id#20L, cust_id#10L, (unit_price#12L * quantity#14L) AS item_total#15L]
   :  +- *(5) Filter isnotnull(cust_id#10L)
   :     +- *(5) FileScan parquet spark_catalog.default.orders ...
... [750 lines truncated]
```

To make sense of this, you either:
1. **Squint at terminal text** trying to trace regex patterns and attribute IDs like `#10L` and `#25L`.
2. **Wait for the job to run and check the Spark UI (`:4040`)**, which is purely post-mortem.
3. **Try enterprise governance frameworks** (OpenLineage, Spline, Marquez), which require spinning up Docker containers, JVM listener JARs, Kafka topics, and server clusters just to trace where a single column came from.

There is a glaring hole in the data engineering ecosystem: **Where is the "Ruff / ESLint" for Apache Spark? Where is the developer-loop tool that inspects your execution plan *before* you run it?**

This is why we built **`sparklineage`**.

---

## 1. What Happens Inside the Catalyst Optimizer?

To understand how `sparklineage` works, let's review how Apache Spark plans query execution:

```
[ PySpark DataFrame API ]
         │
         ▼
  Unresolved Logical Plan  (AST of relations, unresolved column strings)
         │
         ▼ (Analyzer + Catalog resolution)
   Analyzed Logical Plan   (Resolved expressions, typed schemas)
         │
         ▼ (Optimization Rules: pushdown, constant folding, pruning)
  Optimized Logical Plan   (Pruned projections, collapsed filters)
         │
         ▼ (Physical Planning + Cost/Rule-based selection)
    Physical SparkPlan     (SortMergeJoin vs. BroadcastHashJoin, Shuffle Exchanges)
```

At every step, Spark assigns every attribute an immutable integer identifier (e.g. `order_id#12L`). When you apply transformations (`withColumn`, `join`, `groupBy`), Spark builds an internal Abstract Syntax Tree (AST) where expressions reference earlier expression IDs.

---

## 2. Introducing `sparklineage`: Zero-Dependency Developer Tool

Instead of deploying heavy metadata services, `sparklineage` is a lightweight Python package with **zero external dependencies**:

```bash
pip install sparklineage
```

It brings three core capabilities directly into your IDE, Jupyter, or Databricks notebook:

### Feature 1: Pre-Flight Compiler Linter (`sl.lint`)

Before you call an action like `.write()` or `.collect()`, `sparklineage` analyzes the Catalyst AST and surfaces high-risk execution hazards:

```python
import sparklineage as sl

# Lint the DataFrame plan statically in <10ms
report = sl.lint(df)

if report.has_critical:
    raise RuntimeError("Aborted: Dangerous query plan detected!")
```

Sample Terminal Output:
```text
╭─────────────────────────────────────────────────────────────╮
│ ⚡ sparklineage v0.2.0 • Plan Diagnostic & Lineage Summary   │
╰─────────────────────────────────────────────────────────────╯

📊 Plan Metrics:
  • Plan Depth:       12 stages
  • Shuffle Stages:   3
  • Joins Analyzed:   2
  • Issues Detected:  2 (1 Critical, 1 Warnings)

🚨 Pre-Flight Compiler Warnings:
  [CRITICAL] Cartesian / Cross Join Detected
    Problem: Node 'Join' performs a Cartesian cross-join without join predicates.
    Fix:     Add an explicit join condition to avoid O(N*M) stage explosion.

  [WARNING] Un-broadcasted Join Candidate
    Problem: Execution plan utilizes SortMergeJoin with cluster-wide shuffle.
    Fix:     Wrap small dimension tables with broadcast(dim_df).
```

### Feature 2: Field-to-Field Column Lineage

Traditional lineage answers: *"Table A was written from Table B."*  
`sparklineage` answers: *"How was the column `final_revenue` computed?"*

```python
inspector = sl.inspect(df)
lineage = inspector.get_column_lineage()

print(lineage["final_revenue"].sources)
# ['orders.unit_price', 'orders.quantity', 'customers.tier']

print(lineage["final_revenue"].transformations)
# [
#   '(cast(item_total#8L as double) - discount_amount#9)',
#   '(unit_price#6L * quantity#7L)',
#   'CASE WHEN (tier#2 = VIP) THEN (cast(item_total#8L as double) * 0.2) ELSE 0.0 END'
# ]
```

### Feature 3: Interactive Visual DAG in Notebooks (Zero CDN)

With a single call, render an interactive, zoomable SVG DAG directly in a Databricks or Jupyter notebook cell:

```python
inspector.display()
```

Or export to standalone HTML:
```python
inspector.save_html("pipeline_dag.html")
```

The interactive UI allows clicking any node to inspect its Catalyst expressions and output schema, or clicking any column pill to highlight its exact upstream lineage backwards through joins and projections.

---

## 3. How We Implemented It in Pure Python

1. **JVM Introspection & String AST Fallback**:
   When running inside PySpark, `sparklineage` introspects `df._jdf.queryExecution()` to read the Scala AST nodes, expressions, and attributes. If running outside a live JVM or parsing saved plans, it falls back to a deterministic recursive string parser.
2. **Reverse Topological Trace**:
   Starting from the root projection, expression IDs are mapped through alias bindings (`expr AS alias#ID`) down to leaf scan nodes (`LogicalRelation`, `Range`, `FileScan`).
3. **Embedded SVG Engine**:
   Rather than relying on Graphviz (which requires OS-level binaries) or external JavaScript CDNs (which fail in air-gapped enterprise environments), the DAG visualizer compiles into pure inline SVG with vanilla JavaScript event listeners.

---

## 4. Conclusion & Getting Started

Modern data engineering has spent a decade building compliance tools for data auditors while neglecting the day-to-day developer loop for data engineers.

`sparklineage` bridges that gap by providing **instant, actionable feedback** before jobs hit production clusters.

To try it in your environment:
```bash
pip install sparklineage
```

```python
import sparklineage as sl

inspector = sl.inspect(my_df)
inspector.show()
inspector.display()
```

We welcome feedback, issues, and contributions from the data engineering community on GitHub:  
👉 **[https://github.com/pavanbadempet/sparklineage](https://github.com/pavanbadempet/sparklineage)**
