"""
Runnable Demonstration of Native PySpark DataFrame Lineage Feature (df.lineage)
Targeted for Apache Spark Upstream (SPARK-59660).

Demonstrates:
1. Zero-dependency native `df.lineage` accessor on pyspark.sql.DataFrame
2. Pre-flight compiler checks (`df.lineage.lint()`) to catch Cartesian joins & hazards
3. Field-to-field column lineage mapping (`df.lineage.columns`)
4. Credential-sanitized diagnostic markdown export
5. Mermaid graph export (`df.lineage.to_mermaid()`)
6. Standalone interactive HTML DAG generation (`df.lineage.save_html()`)
"""

import os
import sys

# Priority import from the local feature src
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
FEATURE_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
OUTPUTS_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", "outputs"))
os.makedirs(OUTPUTS_DIR, exist_ok=True)

if FEATURE_ROOT not in sys.path:
    sys.path.insert(0, FEATURE_ROOT)

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import src as spark_lineage  # Automatically registers `df.lineage` accessor



def main():
    print("=" * 80)
    print("  SPARK-59660: Native PySpark DataFrame Lineage & Pre-Flight Linter Demo")
    print("=" * 80)

    # 1. Initialize local PySpark session
    print("\n[1/5] Initializing Local PySpark Session...")
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("SparkNativeLineageDemo")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    # 2. Build multi-table DataFrame pipeline
    print("[2/5] Constructing Multi-Table Pipeline with Transformations...")
    customers = (
        spark.range(100)
        .withColumn("customer_id", F.col("id"))
        .withColumn("segment", F.when(F.col("id") % 5 == 0, "Enterprise").otherwise("Retail"))
        .select("customer_id", "segment")
    )

    orders = (
        spark.range(1000)
        .withColumn("order_id", F.col("id"))
        .withColumn("customer_id", F.col("id") % 100)
        .withColumn("unit_price", (F.col("id") % 40) + 15)
        .withColumn("quantity", (F.col("id") % 4) + 1)
        .select("order_id", "customer_id", "unit_price", "quantity")
    )

    promotions = (
        spark.range(10)
        .withColumn("promo_id", F.col("id"))
        .withColumn("discount_pct", F.lit(0.15))
        .select("promo_id", "discount_pct")
    )

    # Intentionally join customers and orders, compute derived financial columns,
    # and cross-join promotions to showcase pre-flight hazard detection
    pipeline_df = (
        orders.join(customers, on="customer_id", how="inner")
        .withColumn("gross_amount", F.col("unit_price") * F.col("quantity"))
        .withColumn("tax_amount", F.col("gross_amount") * 0.08)
        .crossJoin(promotions)  # Demonstrates pre-flight Cartesian product detection
        .withColumn("discount_amount", F.col("gross_amount") * F.col("discount_pct"))
        .withColumn("net_revenue", F.col("gross_amount") + F.col("tax_amount") - F.col("discount_amount"))
    )

    # 3. Pre-flight compiler checks via native `df.lineage.lint()`
    print("\n[3/5] Running Pre-Flight Linter: df.lineage.lint()...")
    lint_report = pipeline_df.lineage.lint()
    lint_output_path = os.path.join(OUTPUTS_DIR, "preflight_lint_report.txt")
    
    with open(lint_output_path, "w", encoding="utf-8") as f:
        header = f"Pre-Flight Linter Analysis: {len(lint_report.issues)} issue(s) detected\n"
        print(header.strip())
        f.write(header + "=" * 60 + "\n")
        for idx, issue in enumerate(lint_report.issues, 1):
            line = f"[{idx}] {issue.severity.value}: {issue.title}\n    {issue.message}\n    Recommendation: {issue.recommendation}\n"
            print(f"  • {issue.severity.value}: {issue.title}")
            print(f"    {issue.recommendation}")
            f.write(line + "\n")

    print(f"  ✔ Saved linter report to: {lint_output_path}")

    # 4. Field-to-field column lineage via `df.lineage.columns`
    print("\n[4/5] Tracing Column-Level Lineage: df.lineage.columns...")
    column_lineage = pipeline_df.lineage.columns
    print(f"  Traced {len(column_lineage)} total output columns:")
    for col_name, lineage_meta in column_lineage.items():
        print(f"  - {col_name:18} <- sources: {lineage_meta.sources}")

    # Trace 'net_revenue' specifically
    net_rev_meta = column_lineage.get("net_revenue")
    if net_rev_meta:
        print(f"\n  Deep Dive on 'net_revenue':")
        print(f"    Direct Upstream:   {net_rev_meta.upstream_columns}")
        print(f"    Transformations:   {net_rev_meta.transformations}")

    # 5. Export documentation artifacts
    print("\n[5/5] Exporting Documentation & Visual Artifacts...")

    # A. Standalone interactive HTML DAG
    html_path = os.path.join(OUTPUTS_DIR, "spark_lineage_dag.html")
    pipeline_df.lineage.save_html(html_path)
    print(f"  ✔ [HTML DAG]       {html_path}")

    # B. Mermaid diagram definition
    mermaid_path = os.path.join(OUTPUTS_DIR, "lineage_dag.mermaid")
    mermaid_content = pipeline_df.lineage.to_mermaid()
    with open(mermaid_path, "w", encoding="utf-8") as f:
        f.write(mermaid_content)
    print(f"  ✔ [Mermaid]        {mermaid_path}")

    # C. Markdown Diagnostic Report with Credential Redaction
    md_path = os.path.join(OUTPUTS_DIR, "diagnostic_report.md")
    md_content = pipeline_df.lineage.inspector.to_markdown(sanitize=True)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_content)
    print(f"  ✔ [Markdown Audit] {md_path}")

    spark.stop()
    print("\n" + "=" * 80)
    print("  Demo Complete! All feature outputs are available in the 'outputs/' directory.")
    print("=" * 80)


if __name__ == "__main__":
    main()
