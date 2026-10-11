# SPIP: Native DataFrame Lineage and Pre-Flight Inspection API (`df.lineage`)

**Author(s):** Pavan Badempet  
**Status:** Proposed  
**Target Version:** Apache Spark 4.1.0+ / PySpark  
**Component:** PySpark / SQL / Catalyst  
**Discussion Thread:** dev@spark.apache.org  
**JIRA Ticket:** [SPARK-59660](https://issues.apache.org/jira/browse/SPARK-59660)  

---

## 1. Executive Summary

This proposal introduces a native **Lineage and Pre-Flight Inspection API** (`df.lineage`) directly into Apache Spark's `pyspark.sql.DataFrame` and `Dataset`.

Today, Apache Spark developers and data engineers have no built-in, zero-overhead mechanism to:
1. Trace **field-level (column) lineage** directly from the Catalyst logical and analyzed plans.
2. Perform **pre-flight linting** before submitting actions (e.g., catching accidental Cartesian joins, unpruned schema scans, or exploding projections before allocating cluster resources).
3. Visualize the complete execution and transformation DAG **interactively in notebook environments** (Databricks, Jupyter, VS Code) without requiring external servers or JVM agents.

Following the successful precedent of **Koalas** (`pyspark.pandas`), **spark-xml** (SPARK-47000), and **spark-avro**, this capability is proven and battle-tested as a standalone library (`sparklineage` on PyPI) before upstream integration, ensuring **100% forward and backward compatibility** across older Spark versions (3.2–3.5) and future native Spark releases.

---

## 2. Motivation & Problem Statement

### 2.1 The Observability Gap in Current Spark
Apache Spark provides:
- `df.explain()`: Prints raw textual Catalyst plans, which can exceed hundreds of lines of nested ASCII, making them notoriously difficult to parse or audit programmatically.
- **Spark UI**: Shows post-execution or in-flight stage DAGs, but does not provide field-to-field column tracking or pre-execution static analysis.
- **External Lineage Collectors** (OpenLineage, Spline): Require JVM agents, network listeners, external database servers (Marquez, OpenLineage backend), and external daemon infrastructure. If the external collector goes down, pipelines risk delays or metric loss.

### 2.2 Pre-Flight Cost Prevention
In production cloud clusters (AWS EMR, Databricks, GCP Dataproc), developers often trigger expensive Spark actions (`.count()`, `.write()`, `.collect()`) on queries with un-optimized plans (e.g., missing join keys causing cross-joins, or missing partition filters). Catching these anti-patterns **before** task scheduling saves substantial cloud compute budgets.

---

## 3. Proposed User-Facing API

The API introduces the `.lineage` property to `pyspark.sql.DataFrame`:

```python
# 1. Pre-flight linting before running expensive actions
report = df.lineage.lint()
if report.has_critical_issues():
    for issue in report.issues:
        print(f"[{issue.severity}] {issue.code}: {issue.message}")
        print(f"Recommendation: {issue.recommendation}")
    # Stop before spending cloud credits on a malformed query!

# 2. Programmatic Column-Level Lineage
col_lineage = df.lineage.columns
# Returns: {'total_amount': ColumnLineage(sources=['transactions.amount', 'rates.multiplier'])}

# 3. Native Interactive DAG in Notebooks
df.lineage.display()
# Renders self-contained, interactive HTML DAG directly in Databricks (displayHTML)
# or Jupyter (IPython.display.HTML)

# 4. Export to Documentation Formats
mermaid_code = df.lineage.to_mermaid()
html_report = df.lineage.to_html(title="Pipeline Audit")
```

---

## 4. Technical Architecture

### 4.1 Zero-Overhead Catalyst Parsing
- Extracts `df._jdf.queryExecution().analyzed()` or `df._jdf.queryExecution().optimizedPlan()` via Py4J or Spark Connect protobuf messages.
- Traverses the AST of `NamedExpression` and `AttributeReference` nodes to construct a directed acyclic graph (DAG) of transformations.
- Extracts projection expressions, aliases, cast operations, and binary joins.

### 4.2 Built-in Credential Sanitization
To prevent accidental leakage of sensitive connection strings or storage credentials:
- Automatically redacts AWS access keys (`AKIA...`), SAS tokens, JDBC passwords, Bearer tokens, and GCP private keys from plan strings prior to rendering or export.

### 4.3 Zero External Dependencies
- The core implementation requires **zero external third-party dependencies** (no heavy web servers, no databases, no external visualization daemons).
- Standalone HTML output uses self-contained SVG / CSS / Vanilla JS rendering.

---

## 5. Backward & Forward Compatibility Strategy

One of the greatest strengths of this proposal is its dual-compatibility architecture:

```
+-----------------------------------------------------------------------+
| User Code:                                                            |
|     import sparklineage  # (Optional or standard import)              |
|     df.lineage.display()                                              |
|     df.lineage.lint()                                                 |
+-----------------------------------------------------------------------+
                                  |
         +------------------------+------------------------+
         |                                                 |
         v                                                 v
[ Spark 4.x+ Native ]                             [ Spark 3.2 - 3.5 ]
- Built into `pyspark.sql`                        - `pip install sparklineage`
- Zero installation required                      - Seamlessly registers `df.lineage`
- Native Spark release cycle                      - 100% identical API & behavior
```

1. **Forward Compatibility (Spark 4.x+)**:
   - When merged into Apache Spark upstream, `df.lineage` is available out of the box in standard PySpark distributions.
2. **Backward Compatibility (Spark 3.2 to 3.5)**:
   - Users on existing production clusters can run `pip install sparklineage` or `uv add sparklineage`.
   - Importing `sparklineage` dynamically attaches the identical `.lineage` accessor to `pyspark.sql.DataFrame`.
   - **Zero code changes** required when upgrading from Spark 3.x with the library to Spark 4.x with native support.

---

## 6. Precedents in Apache Spark

| Project / Feature | Initial Incubation | Upstream Integration | Result |
|---|---|---|---|
| **Koalas** | Databricks open-source library (`koalas`) | Merged into Spark 3.2 as `pyspark.pandas` (SPARK-34849) | Universal Pandas API on Spark |
| **spark-xml** | Databricks external library (`com.databricks:spark-xml`) | Merged into Spark 4.0 (SPARK-47000) | Native XML data source |
| **spark-avro** | External package | Merged into core Spark distribution | Native Avro support |
| **sparklineage** | Independent zero-dependency PyPI package | Proposed for Spark 4.1+ (`df.lineage`) | Native execution lineage & pre-flight linting |

---

## 7. Next Steps & Timeline

1. **Community Discussion**:
   - Present proposal on `dev@spark.apache.org` mailing list.
   - Gather feedback from Spark committers, PMC members, and end-users.
2. **JIRA Creation**:
   - Create umbrella JIRA ticket under the `PySpark / SQL` component.
3. **Reference Implementation**:
   - The battle-tested reference implementation is publicly available and validated on PyPI (`sparklineage` v0.2.6).
   - Test suite achieves 100% pass rate with both synthetic and live PySpark Catalyst plans.
4. **Pull Request Submission**:
   - Submit PR to `apache/spark` repository targeting `python/pyspark/sql/lineage.py` and `sql/catalyst`.
