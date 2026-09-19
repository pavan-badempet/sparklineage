# ⚡ sparklineage

[![PyPI version](https://img.shields.io/badge/pypi-v0.2.0-blue.svg)](https://pypi.org/project/sparklineage/)
[![Author](https://img.shields.io/badge/author-Pavan%20Badempet-indigo.svg)](https://pavanbadempet.github.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)

A lightweight, zero-dependency, developer-first library for **field-to-field column lineage**, **Catalyst pre-flight compiler lints**, and **interactive DAG visualization** in Apache Spark and PySpark.

Instead of heavy enterprise collectors (which require OpenLineage/Marquez servers or agent JARs), `sparklineage` runs directly inside your notebook or Python process with **zero external infrastructure**.

Authored and maintained by **[Pavan Badempet](https://pavanbadempet.github.io)**.

---

## ⚡ Why sparklineage?

1. **🌱 Field-to-Field Column Lineage:** Traces each output DataFrame column back through joins, projections, and expressions to its upstream source columns.
2. **🚨 Pre-Flight Compiler Lints:** Catches expensive bottlenecks *before* triggering actions:
   - **Cartesian / Cross-Join Hazards**: Unintended Cartesian joins that trigger O(N*M) stage blowups.
   - **Un-broadcasted Join Candidates**: Identifies dimension tables under size thresholds that can avoid SortMergeJoin shuffles.
   - **Lineage Depth Overflow**: Warns before deep iterative pipelines trigger driver `StackOverflowError`.
   - **Partition Pushdown Invalidation**: Flags WHERE clause type-casting that breaks file/partition pruning.
   - **Redundant Shuffles**: Highlights consecutive exchange operations.
3. **🎨 Interactive Visual DAG (Zero Infrastructure):** Embeds a clean, zoomable SVG/HTML execution graph directly in Databricks and Jupyter notebooks, or exports to standalone `.html`.
4. **📊 Rich Terminal Reports:** Beautiful, colorized ANSI summaries and exportable Markdown tables.

---

## 📦 Installation

```bash
pip install sparklineage
```

---

## 🚀 Quickstart

### 1. Inspect & Visualize in a Single Line

```python
import sparklineage as sl

# Inspect any PySpark DataFrame
inspector = sl.inspect(my_df)

# Render interactive DAG inside Databricks or Jupyter cell
inspector.display()

# Or save standalone HTML
inspector.save_html("lineage_graph.html")
```

---

### 2. Pre-Flight Compiler Linter

Catch expensive mistakes before calling `.write()` or `.collect()`:

```python
from sparklineage import lint

report = lint(my_df)

if report.has_critical:
    print("Execution blocked: Critical Catalyst hazards detected!")
```

Sample Terminal Output:
```text
╭─────────────────────────────────────────────────────────────╮
│ ⚡ sparklineage v0.2.0 • Plan Diagnostic & Lineage Summary   │
╰─────────────────────────────────────────────────────────────╯

📊 Plan Metrics:
  • Plan Depth:       6 stages
  • Shuffle Stages:   2
  • Joins Analyzed:   1
  • Issues Detected:  1 (0 Critical, 1 Warnings)

🚨 Pre-Flight Compiler Warnings:
  [WARNING] Un-broadcasted Join Candidate
    Problem: Execution plan utilizes SortMergeJoin with cluster-wide shuffle.
    Fix:     Wrap dimension tables with broadcast(dim_df).

🌱 Column-Level Lineage Mapping:
  • total ⟵ Source(Range).id, Source(Range).val
    Transforms: (val#1L + 100)
```

---

### 3. Field-to-Field Column Lineage

```python
col_lineage = inspector.get_column_lineage()

for col, details in col_lineage.items():
    print(f"{col} originates from: {details.sources}")
    print(f"Transformations applied: {details.transformations}")
```

---

### 4. Backwards Compatibility (v0.1.0 API)

Full compatibility with v0.1.0 is preserved:

```python
from sparklineage import PlanInspector

inspector = PlanInspector(my_df)
inspector.check_broadcast_opportunities(threshold_mb=100)
summary = inspector.summarize()
```

---

## 📄 License

MIT License. Copyright (c) 2026 Pavan Badempet.
