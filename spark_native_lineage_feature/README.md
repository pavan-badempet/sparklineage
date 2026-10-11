# PySpark Native DataFrame Lineage & Pre-Flight Linter (`df.lineage`)

> **Official Apache Spark Improvement Proposal:** [SPARK-59660](https://issues.apache.org/jira/browse/SPARK-59660)  
> **Target Subsystem:** `pyspark.sql.DataFrame`, Catalyst Optimizer, Spark Connect  
> **Runtime Dependencies:** Zero external dependencies (`dependencies = []`)  
> **Compatibility:** Apache Spark 3.2, 3.3, 3.4, 3.5, and 4.x  

---

## 1. What Is This Feature?

This standalone folder encapsulates the proposed **Native DataFrame Lineage & Pre-Flight Linter** for Apache Spark.

Today, PySpark developers inspecting execution plans only have `df.explain()`, which outputs raw, hundreds-of-lines ASCII text that cannot be queried or validated programmatically. External lineage tools (OpenLineage, Spline) require heavy JVM bytecode listeners, external daemon agents, and remote database servers (Marquez).

This feature introduces a native, zero-overhead accessor directly onto `pyspark.sql.DataFrame`:

```python
# 1. Catch expensive execution hazards before triggering Spark jobs
report = df.lineage.lint()
if report.has_critical_issues():
    report.show()  # Warns on Cartesian products, missing join keys, deep lineage

# 2. Trace field-to-field column origins directly from Catalyst AST
col_trace = df.lineage.columns
# Returns: {'net_revenue': ColumnLineage(sources=['orders.unit_price', ...])}

# 3. Render standalone interactive DAG in Databricks & Jupyter notebooks
df.lineage.display()

# 4. Export documentation artifacts
mermaid_graph = df.lineage.to_mermaid()
html_document = df.lineage.to_html()
```

---

## 2. Directory Layout

This folder is structured as a self-contained feature distribution ready for review, upstream porting, or direct library usage:

```
spark_native_lineage_feature/
├── README.md                      # Feature overview, architectural rationale, and quickstart (this file)
├── SPARK_59660_SPIP.md            # The formal Apache Spark Project Improvement Proposal (SPIP)
│
├── src/                           # Pure Python zero-dependency source code
│   ├── __init__.py                # Package entrypoint & DataFrame.lineage accessor registration
│   ├── inspector.py               # LineageAccessor and PlanInspector core engine
│   ├── ast_parser.py              # Catalyst JVM execution plan and numberedTreeString parser
│   ├── column_lineage.py          # Field-to-field column lineage resolver
│   ├── linter.py                  # Pre-flight static compiler rules (Cartesian joins, depth, etc.)
│   ├── visualizer.py              # Standalone SVG/HTML interactive DAG generator (zero external JS/CSS)
│   ├── reporter.py                # ANSI colorized console and Markdown diagnostic reporter
│   ├── models.py                  # Core data models (PlanGraph, PlanNode, ColumnLineage, LintReport)
│   └── static_linter.py           # Offline secret and cloud credential scanner
│
├── examples/                      # Runnable demonstration scripts
│   └── demo_feature.py            # Complete end-to-end pipeline showcasing all capabilities
│
└── outputs/                       # Pre-generated, verifiable artifacts produced by the feature
    ├── spark_lineage_dag.html     # Live interactive DAG (double-click to open in any web browser)
    ├── diagnostic_report.md       # Markdown audit report with credential sanitization
    ├── lineage_dag.mermaid        # Clean Mermaid diagram definition
    └── preflight_lint_report.txt  # Terminal pre-flight linter findings
```

---

## 3. How to Run the Feature & Generate Outputs

You can run the demonstration script directly in any environment with Python and PySpark:

```bash
# Using uv (fastest):
uv run --with pyspark python spark_native_lineage_feature/examples/demo_feature.py

# Or using standard python:
python spark_native_lineage_feature/examples/demo_feature.py
```

### What Happens When You Run It:
1. Spins up an in-process local SparkSession.
2. Builds a realistic customer-orders-promotions pipeline with an intentional Cartesian cross-join.
3. Automatically triggers `df.lineage.lint()`, catching the hazard before any action is executed.
4. Traces column transformations for all output fields down to their source relations.
5. Emits the interactive HTML, Mermaid, Markdown, and text reports directly into `spark_native_lineage_feature/outputs/`.

---

## 4. Key Architectural Guarantees

1. **Zero External Runtime Dependencies**:
   - Written in pure Python using standard libraries.
   - The interactive visualizer generates an inline SVG + Vanilla JS diagram with a dark-slate theme, requiring no internet connection, no CDNs, and no external npm modules.

2. **Zero Overhead on Normal Execution**:
   - The `.lineage` accessor is a lazy Python property. If user pipelines do not call `df.lineage`, exactly zero CPU cycles are spent during DataFrame transformations or actions.

3. **Enterprise Credential Sanitization**:
   - Automatically detects and redacts AWS access keys (`AKIA...`), Azure SAS tokens, GCP service account credentials, and JDBC passwords before any plan details or lineage metadata are rendered or exported.

4. **Dual Forward & Backward Compatibility**:
   - **Forward (Spark 4.1+)**: Can be merged directly into `python/pyspark/sql/` as a native DataFrame attribute.
   - **Backward (Spark 3.2 - 3.5)**: Works today as a drop-in PyPI library (`pip install sparklineage` or `uv add sparklineage`).

---

## 5. Pre-Generated Example Outputs

Inspect the live files in `outputs/`:
- **`outputs/spark_lineage_dag.html`**: Open in Google Chrome, Microsoft Edge, or Safari to pan, zoom, inspect node execution details, and click individual columns to highlight their lineage paths.
- **`outputs/preflight_lint_report.txt`**: Demonstrates the linter warning about an un-optimized Cartesian cross-join.
- **`outputs/diagnostic_report.md`**: Clean markdown table showing all nodes, schemas, and column traces.
- **`outputs/lineage_dag.mermaid`**: Standard Mermaid graph syntax for embedding in GitHub pull requests or internal wikis.

---

## 6. Upstream Spark Contribution Roadmap

- **Apache Spark JIRA Ticket**: [SPARK-59660](https://issues.apache.org/jira/browse/SPARK-59660)
- **Discussion Thread**: `dev@spark.apache.org`
- **Target Repository**: `apache/spark` -> `python/pyspark/sql/lineage.py`
