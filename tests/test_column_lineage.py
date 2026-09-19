"""
Tests for field-to-field column lineage resolution.
"""

from sparklineage.ast_parser import CatalystParser
from sparklineage.column_lineage import ColumnLineageResolver

SAMPLE_PLAN = """
00 Project [id#0L, val#1L, desc#3, (val#1L + 100) AS total#4L]
01 +- Join Inner, (id#0L = id#2L)
02    :- Project [id#0L, (id#0L * 2) AS val#1L]
03    :  +- Range (0, 10, step=1, splits=Some(1))
04    +- Project [id#2L, concat(item_, cast(id#2L as string)) AS desc#3]
05       +- Range (0, 10, step=1, splits=Some(1))
"""

def test_column_lineage_resolution():
    graph = CatalystParser.parse(SAMPLE_PLAN)
    lineages = ColumnLineageResolver.resolve(graph)

    assert "total" in lineages
    total_lin = lineages["total"]
    assert "val" in total_lin.upstream_columns or len(total_lin.transformations) > 0
    assert len(total_lin.transformations) >= 1
    assert "100" in total_lin.transformations[0]
