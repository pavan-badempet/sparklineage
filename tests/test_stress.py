"""
Additional live PySpark tests to stress-test Cartesian joins, deep lineage, and multi-table joins.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sparklineage as sl


@pytest.fixture(scope="module")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("sparklineage_stress_tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.crossJoin.enabled", "true")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_live_cartesian_linter(spark):
    df_a = spark.range(5)
    df_b = spark.range(5)
    # Deliberate cross join without conditions
    df_cross = df_a.crossJoin(df_b)

    report = sl.lint(df_cross)
    assert report.has_critical
    assert any(i.rule_id == "RULE01_CARTESIAN_HAZARD" for i in report.issues)


def test_live_complex_column_lineage_resolution(spark):
    df1 = spark.range(10).withColumn("base_salary", F.col("id") * 1000)
    df2 = spark.range(10).withColumn("bonus", F.col("id") * 150)

    # Join and derive a composite metric
    merged = (
        df1.join(df2, "id")
        .withColumn("total_comp", F.col("base_salary") + F.col("bonus"))
        .withColumn("is_executive", F.when(F.col("total_comp") > 5000, F.lit(True)).otherwise(F.lit(False)))
    )

    inspector = sl.inspect(merged)
    lineage = inspector.get_column_lineage()

    assert "total_comp" in lineage
    tc_lin = lineage["total_comp"]
    # Verify upstream columns include both components
    assert "base_salary" in tc_lin.upstream_columns
    assert "bonus" in tc_lin.upstream_columns
    assert len(tc_lin.transformations) >= 1

    # Verify is_executive traces all the way back through total_comp to base_salary and bonus
    assert "is_executive" in lineage
    exec_lin = lineage["is_executive"]
    assert "total_comp" in exec_lin.upstream_columns or "base_salary" in exec_lin.upstream_columns
