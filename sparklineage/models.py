"""
Data models and representations for Spark execution plans, lineage, and linting.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Optional, Any, Set


class NodeType(str, Enum):
    SOURCE = "Source"
    PROJECT = "Project"
    FILTER = "Filter"
    JOIN = "Join"
    AGGREGATE = "Aggregate"
    WINDOW = "Window"
    SORT = "Sort"
    EXCHANGE = "Exchange"
    UNION = "Union"
    LIMIT = "Limit"
    OTHER = "Other"


class Severity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class PlanNode:
    """Represents a single operation node in the Catalyst execution plan."""
    node_id: str
    node_type: NodeType
    name: str
    details: str = ""
    output_columns: List[str] = field(default_factory=list)
    expressions: List[str] = field(default_factory=list)
    children_ids: List[str] = field(default_factory=list)
    parent_ids: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PlanGraph:
    """Directed Acyclic Graph representing the full plan structure."""
    nodes: Dict[str, PlanNode] = field(default_factory=dict)
    root_id: Optional[str] = None
    sources: List[str] = field(default_factory=list)

    def add_node(self, node: PlanNode) -> None:
        self.nodes[node.node_id] = node
        if node.node_type == NodeType.SOURCE:
            if node.node_id not in self.sources:
                self.sources.append(node.node_id)

    def add_edge(self, parent_id: str, child_id: str) -> None:
        if parent_id in self.nodes and child_id in self.nodes:
            if child_id not in self.nodes[parent_id].children_ids:
                self.nodes[parent_id].children_ids.append(child_id)
            if parent_id not in self.nodes[child_id].parent_ids:
                self.nodes[child_id].parent_ids.append(parent_id)

    def get_depth(self) -> int:
        """Computes the maximum depth of the plan DAG."""
        if not self.root_id or self.root_id not in self.nodes:
            return len(self.nodes)

        memo: Dict[str, int] = {}

        def _dfs(nid: str) -> int:
            if nid in memo:
                return memo[nid]
            node = self.nodes.get(nid)
            if not node or not node.children_ids:
                return 1
            max_child = max((_dfs(cid) for cid in node.children_ids), default=0)
            depth = 1 + max_child
            memo[nid] = depth
            return depth

        return _dfs(self.root_id)


@dataclass
class ColumnOrigin:
    """Represents the upstream origin of a specific column."""
    target_column: str
    source_table_or_step: str
    source_column: str
    transformation: Optional[str] = None


@dataclass
class ColumnLineage:
    """Lineage map for an individual output column."""
    column_name: str
    data_type: str = "unknown"
    sources: List[str] = field(default_factory=list)  # e.g. ["db.table.col_a"]
    transformations: List[str] = field(default_factory=list)  # list of applied functions
    upstream_columns: Set[str] = field(default_factory=set)


@dataclass
class LintIssue:
    """A single diagnostic issue found during plan pre-flight analysis."""
    rule_id: str
    title: str
    severity: Severity
    message: str
    recommendation: str
    node_id: Optional[str] = None
    node_name: Optional[str] = None


@dataclass
class LintReport:
    """Aggregated pre-flight lint findings."""
    issues: List[LintIssue] = field(default_factory=list)
    plan_depth: int = 0
    estimated_shuffles: int = 0
    joins_analyzed: int = 0

    @property
    def has_warnings(self) -> bool:
        return any(i.severity in (Severity.WARNING, Severity.CRITICAL) for i in self.issues)

    @property
    def has_critical(self) -> bool:
        return any(i.severity == Severity.CRITICAL for i in self.issues)

    @property
    def summary_stats(self) -> Dict[str, int]:
        return {
            "critical": sum(1 for i in self.issues if i.severity == Severity.CRITICAL),
            "warnings": sum(1 for i in self.issues if i.severity == Severity.WARNING),
            "info": sum(1 for i in self.issues if i.severity == Severity.INFO),
            "total_issues": len(self.issues),
            "depth": self.plan_depth,
            "shuffles": self.estimated_shuffles,
        }
