"""
sparklineage
~~~~~~~~~~~~

Lightweight PySpark execution plan analysis, field-to-field column lineage,
pre-flight compiler lints, and zero-dependency interactive DAG visualization.

Authored and maintained by Pavan Badempet <pavan9b@gmail.com>
https://pavanbadempet.gitlab.io
"""

__version__ = "0.2.6"
__author__ = "Pavan Badempet"

from .inspector import (
    PlanInspector,
    inspect,
    lint,
    LineageAccessor,
    register_pyspark_accessor,
)
from .static_linter import lint_code, lint_file, lint_directory
from .models import (
    PlanGraph,
    PlanNode,
    NodeType,
    ColumnLineage,
    LintIssue,
    LintReport,
    Severity,
)

# Automatically register `df.lineage` on `pyspark.sql.DataFrame` if PySpark is present
register_pyspark_accessor()

__all__ = [
    "PlanInspector",
    "inspect",
    "lint",
    "LineageAccessor",
    "register_pyspark_accessor",
    "lint_code",
    "lint_file",
    "lint_directory",
    "PlanGraph",
    "PlanNode",
    "NodeType",
    "ColumnLineage",
    "LintIssue",
    "LintReport",
    "Severity",
]
