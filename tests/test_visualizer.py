"""
Tests for Visualizer HTML generation.
"""

from sparklineage.ast_parser import CatalystParser
from sparklineage.column_lineage import ColumnLineageResolver
from sparklineage.visualizer import Visualizer

SAMPLE_PLAN = """
00 Project [id#0L, val#1L, (val#1L + 100) AS total#4L]
01 +- Range (0, 10, step=1, splits=Some(1))
"""

def test_visualizer_generates_valid_html():
    graph = CatalystParser.parse(SAMPLE_PLAN)
    col_lin = ColumnLineageResolver.resolve(graph)
    html = Visualizer.generate_html(graph, col_lin, title="Test DAG")

    assert "<!DOCTYPE html>" in html
    assert "sparklineage" in html
    assert "graph-canvas" in html
    assert "total" in html
