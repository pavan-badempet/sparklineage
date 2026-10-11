"""
Column-Level Lineage Resolver.
Traces individual DataFrame output columns back to source relations and intermediate expressions.
"""

import re
from typing import Dict, List, Set, Any, Optional
from .models import ColumnLineage, PlanGraph, NodeType


class ColumnLineageResolver:
    """
    Analyzes Catalyst expressions and attribute bindings to map field-to-field lineage.
    """

    # Matches attributes like name#123L or col#45
    ATTR_PATTERN = re.compile(r'([A-Za-z0-9_]+)#(\d+)[L]?')
    # Matches AS aliases like expr AS alias#45L
    ALIAS_PATTERN = re.compile(r'^(.*?)\s+AS\s+([A-Za-z0-9_]+)#(\d+)[L]?$')

    @classmethod
    def resolve(cls, plan_graph: PlanGraph, df: Any = None) -> Dict[str, ColumnLineage]:
        """
        Resolves field-to-field lineage for all output columns in the plan.
        """
        lineages: Dict[str, ColumnLineage] = {}

        # 1. Collect all attribute definitions and their dependencies across nodes
        # attr_id -> {"name": str, "expr": str, "deps": set_of_attr_ids, "source": Optional[str]}
        attr_definitions: Dict[str, Dict[str, Any]] = {}
        source_attrs: Dict[str, str] = {}  # attr_id -> source_node_name_or_details

        # First pass: Identify source nodes and their attributes
        for nid, node in plan_graph.nodes.items():
            if node.node_type == NodeType.SOURCE:
                source_label = node.details or node.name
                # Find any attributes introduced in this source
                for match in cls.ATTR_PATTERN.finditer(node.details):
                    aname, aid = match.group(1), match.group(2)
                    source_attrs[aid] = source_label
                    attr_definitions[aid] = {
                        "name": aname,
                        "expr": f"Source({source_label})",
                        "deps": set(),
                        "source": source_label,
                    }

        # Second pass: Parse project and aggregate expressions across nodes (bottom-up)
        # Reverse topological order or simple multi-pass
        for nid, node in plan_graph.nodes.items():
            for expr_str in node.expressions:
                alias_match = cls.ALIAS_PATTERN.match(expr_str.strip())
                if alias_match:
                    raw_expr, alias_name, aid = alias_match.group(1).strip(), alias_match.group(2), alias_match.group(3)
                    # Extract dependencies from raw_expr
                    dep_ids = {m.group(2) for m in cls.ATTR_PATTERN.finditer(raw_expr) if m.group(2) != aid}
                    attr_definitions[aid] = {
                        "name": alias_name,
                        "expr": raw_expr,
                        "deps": dep_ids,
                        "source": None,
                    }
                else:
                    # Simple attribute reference like [id#0L]
                    for match in cls.ATTR_PATTERN.finditer(expr_str):
                        aname, aid = match.group(1), match.group(2)
                        if aid not in attr_definitions:
                            attr_definitions[aid] = {
                                "name": aname,
                                "expr": aname,
                                "deps": set(),
                                "source": source_attrs.get(aid),
                            }

        # Determine target output columns from root node or DataFrame schema
        output_attr_ids: Dict[str, str] = {}  # col_name -> attr_id
        if plan_graph.root_id and plan_graph.root_id in plan_graph.nodes:
            root_node = plan_graph.nodes[plan_graph.root_id]
            for expr_str in root_node.expressions:
                alias_match = cls.ALIAS_PATTERN.match(expr_str.strip())
                if alias_match:
                    output_attr_ids[alias_match.group(2)] = alias_match.group(3)
                else:
                    for m in cls.ATTR_PATTERN.finditer(expr_str):
                        output_attr_ids[m.group(1)] = m.group(2)

        # Fallback to output_columns list if output_attr_ids empty
        if not output_attr_ids and plan_graph.root_id:
            for col in plan_graph.nodes[plan_graph.root_id].output_columns:
                # Find matching attr definition
                for aid, data in attr_definitions.items():
                    if data["name"] == col:
                        output_attr_ids[col] = aid
                        break

        # Trace each output column back to source relations and transformations
        for col_name, root_aid in output_attr_ids.items():
            sources: List[str] = []
            transformations: List[str] = []
            upstream_cols: Set[str] = set()
            visited_aids: Set[str] = set()

            def _trace(curr_aid: str):
                if curr_aid in visited_aids:
                    return
                visited_aids.add(curr_aid)

                info = attr_definitions.get(curr_aid)
                if not info:
                    return

                if info["name"] != col_name:
                    upstream_cols.add(info["name"])

                if info.get("source"):
                    source_entry = f"{info['source']}.{info['name']}"
                    if source_entry not in sources:
                        sources.append(source_entry)
                elif info.get("expr") and info["expr"] != info["name"]:
                    transformations.append(info["expr"])

                for dep_id in info.get("deps", []):
                    _trace(dep_id)

            _trace(root_aid)

            # If no explicit source discovered, look for closest matches in sources
            if not sources and plan_graph.sources:
                for s_nid in plan_graph.sources:
                    s_node = plan_graph.nodes[s_nid]
                    sources.append(s_node.name)

            lineages[col_name] = ColumnLineage(
                column_name=col_name,
                sources=sources,
                transformations=transformations,
                upstream_columns=upstream_cols,
            )

        return lineages
