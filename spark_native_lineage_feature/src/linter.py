"""
Pre-Flight Catalyst Compiler Linter for PySpark DataFrames.
Detects performance hazards, skew vulnerabilities, and lineage depth overflow before execution.
"""

from typing import List, Dict, Any, Optional
from .models import PlanGraph, PlanNode, NodeType, LintIssue, LintReport, Severity


class BaseRule:
    """Base class for all pre-flight plan rules."""
    rule_id: str = "BASE"
    title: str = "Base Rule"

    def check(self, graph: PlanGraph, df: Any = None) -> List[LintIssue]:
        raise NotImplementedError


class CartesianHazardRule(BaseRule):
    """Detects accidental cartesian / cross joins."""
    rule_id: str = "RULE01_CARTESIAN_HAZARD"
    title: str = "Cartesian / Cross Join Detected"

    def check(self, graph: PlanGraph, df: Any = None) -> List[LintIssue]:
        issues = []
        for nid, node in graph.nodes.items():
            if node.node_type == NodeType.JOIN:
                details_lower = node.details.lower()
                is_cartesian = "cross" in details_lower or "cartesian" in details_lower
                # Also check if no condition exists
                has_no_condition = False
                if not any(sym in node.details for sym in ("=", ">", "<", "condition")):
                    if "inner" in details_lower or "outer" in details_lower or "cross" in details_lower:
                        has_no_condition = True

                if is_cartesian or has_no_condition:
                    issues.append(LintIssue(
                        rule_id=self.rule_id,
                        title=self.title,
                        severity=Severity.CRITICAL,
                        message=f"Node '{node.name}' performs a Cartesian cross-join ({node.details[:120]}).",
                        recommendation="Add an explicit join predicate or verify if an intentional cross-join is necessary. Unintended cross joins cause an O(N*M) stage explosion.",
                        node_id=nid,
                        node_name=node.name
                    ))
        return issues


class BroadcastOpportunityRule(BaseRule):
    """Checks for SortMergeJoins where a broadcast could eliminate network shuffle."""
    rule_id: str = "RULE02_BROADCAST_OPPORTUNITY"
    title: str = "Un-broadcasted Join Candidate"

    def __init__(self, threshold_mb: int = 100):
        self.threshold_mb = threshold_mb

    def check(self, graph: PlanGraph, df: Any = None) -> List[LintIssue]:
        issues = []
        # Check executed or physical plan string if available
        plan_str = ""
        if df is not None and hasattr(df, "_jdf"):
            try:
                plan_str = df._jdf.queryExecution().executedPlan().toString()
            except Exception:
                pass

        if not plan_str:
            # Fallback to checking graph node details
            plan_str = " ".join(n.details for n in graph.nodes.values())

        if "SortMergeJoin" in plan_str and "BroadcastHashJoin" not in plan_str:
            issues.append(LintIssue(
                rule_id=self.rule_id,
                title=self.title,
                severity=Severity.WARNING,
                message=f"Execution plan utilizes SortMergeJoin with cluster-wide shuffle. If one side is a dimension table under ~{self.threshold_mb}MB, a broadcast join eliminates network transfer.",
                recommendation="Wrap dimension tables with `pyspark.sql.functions.broadcast(dim_df)` or verify `spark.sql.autoBroadcastJoinThreshold`.",
            ))
        return issues


class LineageDepthRule(BaseRule):
    """Warns when transformation DAG depth risks driver StackOverflowError."""
    rule_id: str = "RULE03_LINEAGE_DEPTH_OVERFLOW"
    title: str = "Excessive Lineage Depth"

    def __init__(self, max_safe_depth: int = 18):
        self.max_safe_depth = max_safe_depth

    def check(self, graph: PlanGraph, df: Any = None) -> List[LintIssue]:
        issues = []
        depth = graph.get_depth()
        if depth > self.max_safe_depth:
            issues.append(LintIssue(
                rule_id=self.rule_id,
                title=self.title,
                severity=Severity.WARNING,
                message=f"Plan lineage depth is {depth} stages (threshold: {self.max_safe_depth}). Deep un-checkpointed Catalyst trees can trigger driver StackOverflowError during optimization.",
                recommendation="Break iterative transformations by calling `df = df.checkpoint()` or persisting with `df.persist()` at major checkpoints.",
            ))
        return issues


class NonPushdownFilterRule(BaseRule):
    """Detects filter predicates that prevent partition/file skipping."""
    rule_id: str = "RULE04_NON_PUSHDOWN_FILTER"
    title: str = "Inhibited Partition Pushdown Predicate"

    def check(self, graph: PlanGraph, df: Any = None) -> List[LintIssue]:
        issues = []
        for nid, node in graph.nodes.items():
            if node.node_type == NodeType.FILTER:
                details = node.details
                # Detect common anti-patterns: casting partition column, python UDFs in filter
                if "cast(" in details.lower() and "as string" in details.lower():
                    issues.append(LintIssue(
                        rule_id=self.rule_id,
                        title=self.title,
                        severity=Severity.WARNING,
                        message=f"Filter '{details[:100]}' uses type casting which may disable partition pruning and Parquet dictionary skipping.",
                        recommendation="Filter using native schema types directly without explicit runtime casting in the WHERE clause.",
                        node_id=nid,
                        node_name=node.name
                    ))
        return issues


class RedundantExchangeRule(BaseRule):
    """Detects redundant back-to-back shuffle exchanges."""
    rule_id: str = "RULE05_REDUNDANT_EXCHANGE"
    title: str = "Redundant Shuffle Exchange"

    def check(self, graph: PlanGraph, df: Any = None) -> List[LintIssue]:
        issues = []
        for nid, node in graph.nodes.items():
            if node.node_type == NodeType.EXCHANGE:
                for cid in node.children_ids:
                    child = graph.nodes.get(cid)
                    if child and child.node_type == NodeType.EXCHANGE:
                        issues.append(LintIssue(
                            rule_id=self.rule_id,
                            title=self.title,
                            severity=Severity.INFO,
                            message=f"Consecutive shuffle exchanges detected between '{node.name}' and '{child.name}'.",
                            recommendation="Consolidate multiple `.repartition()` calls into a single repartitioning operation.",
                            node_id=nid,
                            node_name=node.name
                        ))
        return issues


class SecuritySanitizationRule(BaseRule):
    """Scans execution plans for leaked database credentials, cloud storage keys, or unmasked PII."""
    rule_id = "RULE06_SECURITY_AND_PII_LEAK"
    title = "Credential or PII Leak Detected in Execution Plan"

    def check(self, graph: PlanGraph, df: Any = None) -> List[LintIssue]:
        issues: List[LintIssue] = []
        combined_text = " ".join(f"{n.name} {n.details}" for n in graph.nodes.values())

        try:
            from . import sparklineage_core as rust_core
            raw_issues = rust_core.scan_security(combined_text)
            for rule_id, sev_str, msg in raw_issues:
                sev = Severity.CRITICAL if sev_str == "CRITICAL" else Severity.WARNING
                issues.append(LintIssue(
                    rule_id=rule_id,
                    title="Plan Security Exposure",
                    severity=sev,
                    message=msg,
                    recommendation="Remove hardcoded credentials from DataFrame options and mask PII literal values in filter clauses."
                ))
            return issues
        except ImportError:
            pass

        # Pure Python fallback
        import re
        if re.search(r'(?i)jdbc:[a-z0-9_]+://[^:]+:[^@\s]+@', combined_text):
            issues.append(LintIssue(
                rule_id="SEC01_JDBC_CREDENTIAL_LEAK",
                title="Plan Security Exposure",
                severity=Severity.CRITICAL,
                message="Hardcoded database credentials detected in connection URI inside Catalyst plan.",
                recommendation="Use secret management (e.g. Databricks secrets or Doppler) instead of hardcoding database passwords."
            ))
        if re.search(r'(?i)\b(ssn|credit_card|password)\b[#0-9A-Z_]*\s*([=><!]+)\s*[\'"][^\'"]+[\'"]', combined_text):
            issues.append(LintIssue(
                rule_id="SEC04_PII_FILTER_EXPOSURE",
                title="Plan Security Exposure",
                severity=Severity.WARNING,
                message="Sensitive PII literal unmasked in filter predicate.",
                recommendation="Sanitize literals before printing or exporting plan diagrams."
            ))
        return issues


class PlanLinter:
    """Pre-flight compiler engine executing lint rules against a Spark plan."""

    DEFAULT_RULES = [
        CartesianHazardRule(),
        BroadcastOpportunityRule(),
        LineageDepthRule(),
        NonPushdownFilterRule(),
        RedundantExchangeRule(),
        SecuritySanitizationRule(),
    ]

    def __init__(self, rules: Optional[List[BaseRule]] = None):
        self.rules = rules if rules is not None else list(self.DEFAULT_RULES)

    def lint(self, graph: PlanGraph, df: Any = None) -> LintReport:
        issues: List[LintIssue] = []
        for rule in self.rules:
            try:
                found = rule.check(graph, df=df)
                issues.extend(found)
            except Exception:
                pass

        # Calculate metrics
        shuffles = sum(1 for n in graph.nodes.values() if n.node_type == NodeType.EXCHANGE)
        joins = sum(1 for n in graph.nodes.values() if n.node_type == NodeType.JOIN)

        return LintReport(
            issues=issues,
            plan_depth=graph.get_depth(),
            estimated_shuffles=shuffles,
            joins_analyzed=joins,
        )
