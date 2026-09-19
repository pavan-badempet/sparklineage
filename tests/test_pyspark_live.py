"""
Tests for live PySpark DataFrame integration.
"""

import pytest
import os

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


@pytest.fixture(scope="module")
def spark():
    if not HAS_PYSPARK:
        pytest.skip("PySpark not installed.")
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("sparklineage_live_tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_live_pyspark_inspect_and_lint(spark, tmp_path):
    import sparklineage as sl

    df1 = spark.range(10).withColumn("val", F.col("id") * 2)
    df2 = spark.range(10).withColumn("desc", F.concat(F.lit("item_"), F.col("id")))
    df = df1.join(df2, "id").withColumn("total", F.col("val") + 100)

    # 1. Test inspect() factory
    inspector = sl.inspect(df)
    assert inspector.graph is not None
    assert len(inspector.graph.nodes) >= 3

    # 2. Test column lineage
    col_lin = inspector.get_column_lineage()
    assert "total" in col_lin
    assert "val" in col_lin

    # 3. Test lint()
    report = inspector.lint()
    assert report.plan_depth >= 1
    assert report.joins_analyzed >= 1

    # 4. Test backwards compatibility with v0.1.0 API
    recs = inspector.check_broadcast_opportunities(threshold_mb=50)
    assert isinstance(recs, list)

    summary = inspector.summarize()
    assert "author" in summary
    assert summary["author"] == "Pavan Badempet"
    assert "recommendations" in summary

    # 5. Test markdown export
    md = inspector.to_markdown()
    assert "# ⚡ sparklineage Plan Diagnostic Report" in md

    # 6. Test HTML saving
    html_file = tmp_path / "test_lineage.html"
    out = inspector.save_html(str(html_file))
    assert os.path.exists(out)
    with open(out, "r", encoding="utf-8") as f:
        content = f.read()
    assert "sparklineage" in content
    assert "graph-canvas" in content
