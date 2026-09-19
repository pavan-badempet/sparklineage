"""
Catalyst AST Parser for Spark execution plans.
Traverses logical and physical plans to produce structured PlanGraph representations.
"""

import re
from typing import Any, Optional, List, Tuple
from .models import PlanGraph, PlanNode, NodeType


class CatalystParser:
    """
    Parses PySpark query execution objects into an agnostic PlanGraph.
    """

    @classmethod
    def parse(cls, df_or_plan: Any) -> PlanGraph:
        """
        Parses a PySpark DataFrame, a Catalyst JVM plan object, or a formatted tree string.
        """
        if df_or_plan is None:
            return PlanGraph()

        # Check if it's a PySpark DataFrame with JVM queryExecution
        if hasattr(df_or_plan, "_jdf"):
            try:
                qe = df_or_plan._jdf.queryExecution()
                return cls._parse_jvm_plan(qe)
            except Exception:
                # Fallback to string-based parsing if JVM traversal encounters issues
                try:
                    tree_str = df_or_plan._jdf.queryExecution().optimizedPlan().numberedTreeString()
                    return cls._parse_tree_string(tree_str)
                except Exception:
                    pass

        # If passed as string (e.g. explain string or numberedTreeString)
        if isinstance(df_or_plan, str):
            return cls._parse_tree_string(df_or_plan)

        return PlanGraph()

    @classmethod
    def _classify_node(cls, name: str) -> NodeType:
        name_lower = name.lower()
        if "join" in name_lower:
            return NodeType.JOIN
        elif "project" in name_lower:
            return NodeType.PROJECT
        elif "filter" in name_lower:
            return NodeType.FILTER
        elif "aggregate" in name_lower or "agg" in name_lower:
            return NodeType.AGGREGATE
        elif "window" in name_lower:
            return NodeType.WINDOW
        elif "sort" in name_lower:
            return NodeType.SORT
        elif "exchange" in name_lower or "shuffle" in name_lower:
            return NodeType.EXCHANGE
        elif "union" in name_lower:
            return NodeType.UNION
        elif any(s in name_lower for s in ("scan", "relation", "range", "table", "source")):
            return NodeType.SOURCE
        elif "limit" in name_lower:
            return NodeType.LIMIT
        return NodeType.OTHER

    @classmethod
    def _parse_jvm_plan(cls, qe: Any) -> PlanGraph:
        """Traverses JVM Logical & Executed Plans."""
        graph = PlanGraph()
        node_counter = 0

        # We primarily analyze optimizedPlan for lineage/logic and executedPlan for physical checks
        jvm_plan = qe.optimizedPlan()

        def _traverse(jnode: Any, parent_id: Optional[str] = None) -> str:
            nonlocal node_counter
            node_name = str(jnode.nodeName())
            node_type = cls._classify_node(node_name)
            node_id = f"node_{node_counter}"
            node_counter += 1

            details = str(jnode.simpleString(200)) if hasattr(jnode, "simpleString") else node_name

            output_cols: List[str] = []
            try:
                out_seq = jnode.output()
                for i in range(out_seq.size()):
                    attr = out_seq.apply(i)
                    output_cols.append(str(attr.name()))
            except Exception:
                pass

            expressions: List[str] = []
            try:
                expr_seq = jnode.expressions()
                for i in range(expr_seq.size()):
                    expressions.append(str(expr_seq.apply(i).simpleString(150)))
            except Exception:
                pass

            plan_node = PlanNode(
                node_id=node_id,
                node_type=node_type,
                name=node_name,
                details=details,
                output_columns=output_cols,
                expressions=expressions,
                metadata={
                    "class": jnode.getClass().getSimpleName(),
                    "num_children": jnode.children().size() if hasattr(jnode, "children") else 0,
                }
            )

            graph.add_node(plan_node)
            if graph.root_id is None:
                graph.root_id = node_id

            if parent_id:
                graph.add_edge(parent_id, node_id)

            # Recurse children
            if hasattr(jnode, "children"):
                children_seq = jnode.children()
                for i in range(children_seq.size()):
                    child = children_seq.apply(i)
                    _traverse(child, parent_id=node_id)

            return node_id

        _traverse(jvm_plan)
        return graph

    @classmethod
    def _parse_tree_string(cls, tree_str: str) -> PlanGraph:
        """Parses Catalyst numberedTreeString or explain output into a PlanGraph."""
        graph = PlanGraph()
        lines = [line.strip() for line in tree_str.strip().splitlines() if line.strip()]
        if not lines:
            return graph

        # Pattern for numbered tree string: "00 Project [id#0L, ...]" or "+- Project [id#0L, ...]"
        # Matches: (optional line number) (branch symbols) (NodeName) (optional [details])
        line_pattern = re.compile(
            r'^(?P<num>\d+)?\s*(?P<prefix>[:+\-| ]*)\s*(?P<name>[A-Za-z0-9_]+)(?:\s+(?P<details>.*))?$'
        )

        stack: List[Tuple[int, str]] = []  # (indent_level, node_id)
        node_idx = 0

        for line in lines:
            match = line_pattern.match(line)
            if not match:
                continue

            prefix = match.group("prefix") or ""
            name = match.group("name")
            details = match.group("details") or ""

            # Calculate indentation level based on prefix length and tree symbols
            indent = len(prefix)

            node_id = f"node_{node_idx}"
            node_idx += 1
            node_type = cls._classify_node(name)

            # Extract output columns and expressions from details if bracketed
            output_cols: List[str] = []
            expressions: List[str] = []
            if details.startswith("[") and "]" in details:
                inner = details[1:details.rfind("]")]
                parts = [p.strip() for p in inner.split(",") if p.strip()]
                for p in parts:
                    expressions.append(p)
                    # Extract alias if AS is present, otherwise extract attribute identifier
                    as_match = re.search(r'\bAS\s+([A-Za-z0-9_]+)', p, re.IGNORECASE)
                    if as_match:
                        output_cols.append(as_match.group(1))
                    else:
                        col_match = re.search(r'([A-Za-z0-9_]+)(?:#\d+)?', p)
                        if col_match:
                            output_cols.append(col_match.group(1))

            plan_node = PlanNode(
                node_id=node_id,
                node_type=node_type,
                name=name,
                details=details,
                output_columns=output_cols,
                expressions=expressions,
                metadata={"raw_line": line}
            )

            graph.add_node(plan_node)
            if graph.root_id is None:
                graph.root_id = node_id

            # Attach to parent in stack
            while stack and stack[-1][0] >= indent:
                stack.pop()

            if stack:
                parent_id = stack[-1][1]
                graph.add_edge(parent_id, node_id)

            stack.append((indent, node_id))

        return graph
