"""
Tests for Visualizer HTML generation and Mermaid export.
"""

from sparklineage.ast_parser import CatalystParser
from sparklineage.column_lineage import ColumnLineageResolver
from sparklineage.visualizer import Visualizer
from sparklineage.inspector import inspect

SAMPLE_PLAN = """
00 Project [id#0L, val#1L, (val#1L + 100) AS total#4L]
01 +- Range (0, 10, step=1, splits=Some(1))
"""

SAMPLE_HAZARD_PLAN = """
00 Join Inner
:- Range (0, 10, step=1, splits=Some(1))
+- Range (0, 10, step=1, splits=Some(1))
"""


def test_visualizer_generates_valid_html():
    graph = CatalystParser.parse(SAMPLE_PLAN)
    col_lin = ColumnLineageResolver.resolve(graph)
    html = Visualizer.generate_html(graph, col_lin, title="Test DAG")

    assert "<!DOCTYPE html>" in html
    assert "sparklineage" in html
    assert "graph-canvas" in html
    assert "total" in html
    assert "node-search" in html
    assert "btn-heatmap" in html
    assert "exportSVG" in html


def test_visualizer_with_hazard_plan():
    inspector = inspect(SAMPLE_HAZARD_PLAN)
    html = inspector.to_html()

    assert "hazard-banner" in html
    assert "RULE01_CARTESIAN_HAZARD" in html
    assert "CRITICAL" in html


def test_inspector_to_mermaid():
    inspector = inspect(SAMPLE_PLAN)
    mermaid = inspector.to_mermaid()

    assert "graph LR" in mermaid
    assert "PROJECT" in mermaid
    assert "SOURCE" in mermaid
    assert "-->" in mermaid
