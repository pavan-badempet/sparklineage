"""
End-to-end Demonstration of sparklineage v0.2.0

Run this script directly to see:
1. Pre-flight compiler lints (catching cartesian joins & broadcast candidates)
2. Field-to-field column lineage extraction
3. Rich colorized terminal reporting
4. Standalone interactive HTML DAG generation
"""

import sys
import os

# Ensure current package source is prioritized
sys.path.insert(0, os.path.abspath("."))

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sparklineage as sl


def main():
    print("🚀 Initializing Spark Session...")
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("sparklineage_demo")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    print("\n📦 Building Multi-Stage Customer & Order Pipeline...")
    # 1. Customer dimension table
    customers = (
        spark.range(100)
        .withColumn("cust_id", F.col("id"))
        .withColumn("tier", F.when(F.col("id") % 10 == 0, "VIP").otherwise("Standard"))
        .select("cust_id", "tier")
    )

    # 2. Orders transaction table
    orders = (
        spark.range(1000)
        .withColumn("order_id", F.col("id"))
        .withColumn("cust_id", F.col("id") % 100)
        .withColumn("unit_price", (F.col("id") % 50) + 10)
        .withColumn("quantity", (F.col("id") % 5) + 1)
        .select("order_id", "cust_id", "unit_price", "quantity")
    )

    # 3. Join, aggregate, and compute derived financial metrics
    pipeline_df = (
        orders.join(customers, "cust_id")
        .withColumn("item_total", F.col("unit_price") * F.col("quantity"))
        .withColumn(
            "discount_amount",
            F.when(F.col("tier") == "VIP", F.col("item_total") * 0.2).otherwise(0.0)
        )
        .withColumn("final_revenue", F.col("item_total") - F.col("discount_amount"))
    )

    print("\n🔍 1. Running Pre-Flight Compiler Linter (sl.lint)...")
    lint_report = sl.lint(pipeline_df)
    print(f"Linter Status: {'⚠️ Warnings Found' if lint_report.has_warnings else '✔ Clean'}")
    for issue in lint_report.issues:
        print(f"  • [{issue.severity.value}] {issue.title}: {issue.message}")
        print(f"    Fix: {issue.recommendation}\n")

    print("\n🌱 2. Inspecting Plan & Column-Level Lineage (sl.inspect)...")
    inspector = sl.inspect(pipeline_df)

    # Rich terminal report
    inspector.show()

    # Column Lineage Details
    print("🔬 Column Trace for 'final_revenue':")
    final_rev_lin = inspector.get_column_lineage().get("final_revenue")
    if final_rev_lin:
        print(f"  Upstream Columns: {final_rev_lin.upstream_columns}")
        print(f"  Transformations:  {final_rev_lin.transformations}")

    print("\n🎨 3. Exporting Interactive SVG/HTML DAG...")
    out_html = inspector.save_html("examples/demo_lineage_dag.html")
    print(f"✔ Interactive DAG saved to: {out_html}")
    print("Open this HTML file in any browser to interact with the execution DAG!")

    spark.stop()
    print("\n✨ Demo finished successfully.")


if __name__ == "__main__":
    main()
