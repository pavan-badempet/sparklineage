"""
Tests for security redaction, credential leak detection, and static code linting.
"""

import pytest
import sparklineage as sl
from sparklineage.models import PlanGraph, PlanNode, NodeType, Severity
from sparklineage.linter import SecuritySanitizationRule


def test_secret_redaction_in_plans():
    graph = PlanGraph()
    leak_node = PlanNode(
        node_id="n0",
        node_type=NodeType.SOURCE,
        name="Scan",
        details="jdbc:postgresql://dbadmin:SuperSecretPass123@prod-db.internal:5432/ehr Filter (ssn#1 == '999-00-1111')",
        expressions=["(ssn#1 == '999-00-1111')"]
    )
    graph.add_node(leak_node)
    graph.root_id = "n0"

    inspector = sl.PlanInspector()
    inspector._graph = graph

    # Test Security Rule Detection
    rule = SecuritySanitizationRule()
    issues = rule.check(graph)
    assert len(issues) >= 1
    assert any(i.severity in (Severity.CRITICAL, Severity.WARNING) for i in issues)

    # Test Sanitization
    inspector.sanitize()
    assert "SuperSecretPass123" not in graph.nodes["n0"].details
    assert "999-00-1111" not in graph.nodes["n0"].details
    assert "REDACTED" in graph.nodes["n0"].details or "********" in graph.nodes["n0"].details


def test_offline_static_code_linter():
    bad_code = """
import pyspark.sql.functions as F

for batch in range(10):
    df.collect()

df.crossJoin(dim_table)
df.filter(F.col("dt").cast("string") == "2026-09-19")
df.toPandas()
"""
    issues = sl.lint_code(bad_code)
    assert len(issues) >= 3

    rule_ids = [i["rule_id"] for i in issues]
    assert "CODE01_COLLECT_IN_LOOP" in rule_ids
    assert "CODE02_EXPLICIT_CROSS_JOIN" in rule_ids
    assert "CODE04_CAST_IN_FILTER" in rule_ids
