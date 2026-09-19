"""
sparklineage
~~~~~~~~~~~~

Lightweight PySpark execution plan analysis, field-to-field column lineage,
pre-flight compiler lints, and zero-dependency interactive DAG visualization.

Authored and maintained by Pavan Badempet <pavan9b@gmail.com>
https://pavanbadempet.github.io
"""

__version__ = "0.2.0"
__author__ = "Pavan Badempet"

from .inspector import PlanInspector, inspect, lint
from .models import (
    PlanGraph,
    PlanNode,
    NodeType,
    ColumnLineage,
    LintIssue,
    LintReport,
    Severity,
)

__all__ = [
    "PlanInspector",
    "inspect",
    "lint",
    "PlanGraph",
    "PlanNode",
    "NodeType",
    "ColumnLineage",
    "LintIssue",
    "LintReport",
    "Severity",
]
