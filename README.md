# ⚡ sparklineage

[![PyPI version](https://img.shields.io/badge/pypi-v0.2.0-blue.svg)](https://pypi.org/project/sparklineage/)
[![Author](https://img.shields.io/badge/author-Pavan%20Badempet-indigo.svg)](https://pavanbadempet.github.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![Rust Core](https://img.shields.io/badge/powered%20by-Rust-orange.svg)](https://www.rust-lang.org/)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-green.svg)](pyproject.toml)

A lightweight, zero-dependency, developer-first library for **field-to-field column lineage**, **Catalyst pre-flight compiler lints**, **multi-cloud security redaction**, and **interactive DAG visualization** in Apache Spark and PySpark.

Instead of heavy enterprise collectors (which require OpenLineage/Marquez servers or agent JARs), `sparklineage` runs directly inside your notebook or Python process with **zero external infrastructure**.

Authored and maintained by **[Pavan Badempet](https://pavanbadempet.github.io)**.

---

## ⚡ Why sparklineage?

1. **🌱 Field-to-Field Column Lineage:** Traces each output DataFrame column back through joins, projections, and mathematical expressions to its upstream source columns.
2. **🚨 Pre-Flight Compiler Lints:** Catches expensive bottlenecks *before* triggering actions:
   - **Cartesian / Cross-Join Hazards**: Unintended Cartesian joins that trigger O(N*M) stage blowups.
   - **Un-broadcasted Join Candidates**: Identifies dimension tables under size thresholds that can avoid SortMergeJoin shuffles.
   - **Lineage Depth Overflow**: Warns before deep iterative pipelines trigger driver `StackOverflowError`.
   - **Partition Pushdown Invalidation**: Flags WHERE clause type-casting that breaks file/partition pruning.
   - **Redundant Shuffles**: Highlights consecutive exchange operations.
3. **🔒 Multi-Cloud Security & PII Sanitization:** 
   - Automatically detects and redacts database passwords in connection URIs (`jdbc:postgresql://user:pass@host`), AWS keys (`AKIA...`), and Azure SAS tokens.
   - Masks sensitive PII literals (`ssn`, `credit_card`, `dob`, `password`) before exporting diagrams or terminal reports.
4. **🦀 Sub-Millisecond Offline Static Linter (Powered by Rust):**
   - Scans entire codebases for Spark anti-patterns (`.collect()` in loops, un-limited `.toPandas()`, explicit cross joins) in **< 1 ms** via Rayon multi-threading without booting a JVM.
5. **🎨 Zero-Dependency Interactive Visual DAG:** 
   - Embeds a clean, dark-mode, zoomable SVG/HTML execution graph directly in Databricks and Jupyter notebooks, or exports to standalone `.html`.
   - 100% self-contained: works inside strictly air-gapped VPCs with **no CDN or internet access required**.

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

# Render interactive DAG inside Databricks or Jupyter notebook
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

### 4. High-Speed Offline Code Linter (Pre-Commit / CI)

Lint Python files and notebooks for PySpark anti-patterns without starting a SparkSession:

```python
import sparklineage as sl

# Lint a code string in 0.5 milliseconds
issues = sl.lint_code("""
for batch in batches:
    df.collect()
""")

# Or scan an entire repository in parallel across all CPU cores
repo_issues = sl.lint_directory(".")
```

---

### 5. Multi-Cloud Security Sanitization

```python
# Automatically sanitizes AWS keys, JDBC passwords, and PII literals in memory
inspector.sanitize()

# Safe for public PR comments or Slack posts:
inspector.show()
inspector.to_markdown()
```

---

### 6. Backwards Compatibility (v0.1.0 API)

Full compatibility with v0.1.0 is preserved:

```python
from sparklineage import PlanInspector

inspector = PlanInspector(my_df)
inspector.check_broadcast_opportunities(threshold_mb=100)
summary = inspector.summarize()
```

---

## 📚 Technical Deep Dive

Read the full architectural breakdown:  
👉 **[Why Spark Developers Waste Thousands on Compute (And Why We Built a Pre-Flight Catalyst Linter)](docs/technical_deep_dive_article.md)**

---

## 📄 License

MIT License. Copyright (c) 2026 Pavan Badempet.
