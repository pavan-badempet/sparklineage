"""
Tests for CatalystParser and PlanLinter using string plan trees.
"""

import pytest
from sparklineage.ast_parser import CatalystParser
from sparklineage.models import NodeType, Severity
from sparklineage.linter import PlanLinter, CartesianHazardRule, LineageDepthRule, BroadcastOpportunityRule


SAMPLE_PLAN = """
00 Project [id#0L, val#1L, desc#3, (val#1L + 100) AS total#4L]
01 +- Join Inner, (id#0L = id#2L)
02    :- Project [id#0L, (id#0L * 2) AS val#1L]
03    :  +- Range (0, 10, step=1, splits=Some(1))
04    +- Project [id#2L, concat(item_, cast(id#2L as string)) AS desc#3]
05       +- Range (0, 10, step=1, splits=Some(1))
"""

CARTESIAN_PLAN = """
00 Project [id#0L, code#5]
01 +- Join Inner
02    :- Range (0, 10, step=1, splits=Some(1))
03    +- Range (0, 5, step=1, splits=Some(1))
"""


def test_catalyst_parser_structure():
    graph = CatalystParser.parse(SAMPLE_PLAN)
    assert len(graph.nodes) >= 5
    assert graph.root_id is not None
    root = graph.nodes[graph.root_id]
    assert root.node_type == NodeType.PROJECT
    assert "total" in root.output_columns


def test_cartesian_hazard_detection():
    graph = CatalystParser.parse(CARTESIAN_PLAN)
    linter = PlanLinter([CartesianHazardRule()])
    report = linter.lint(graph)
    assert report.has_critical
    assert any(i.rule_id == "RULE01_CARTESIAN_HAZARD" for i in report.issues)


def test_lineage_depth_rule():
    graph = CatalystParser.parse(SAMPLE_PLAN)
    linter = PlanLinter([LineageDepthRule(max_safe_depth=2)])
    report = linter.lint(graph)
    assert report.has_warnings
    assert any(i.rule_id == "RULE03_LINEAGE_DEPTH_OVERFLOW" for i in report.issues)
