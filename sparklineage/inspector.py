"""
Main PlanInspector module for analyzing PySpark query execution plans and lineage.
Authored by Pavan Badempet (https://pavanbadempet.gitlab.io)
"""

from typing import Dict, Any, List, Optional
import os
from .models import PlanGraph, ColumnLineage, LintReport
from .ast_parser import CatalystParser
from .column_lineage import ColumnLineageResolver
from .linter import PlanLinter
from .visualizer import Visualizer
from .reporter import Reporter


class PlanInspector:
    """
    Utility to inspect Spark DataFrame execution plans, diagnose shuffle bottlenecks,
    trace field-to-field column lineage, and render interactive DAGs.
    """

    def __init__(self, df: Any = None):
        self.df = df
        self.recommendations: List[str] = []
        self._graph: Optional[PlanGraph] = None
        self._column_lineage: Optional[Dict[str, ColumnLineage]] = None
        self._lint_report: Optional[LintReport] = None

    @property
    def graph(self) -> PlanGraph:
        """Lazily parsed PlanGraph."""
        if self._graph is None:
            self._graph = CatalystParser.parse(self.df)
        return self._graph

    def get_column_lineage(self) -> Dict[str, ColumnLineage]:
        """Returns field-to-field column lineage mapping."""
        if self._column_lineage is None:
            self._column_lineage = ColumnLineageResolver.resolve(self.graph, df=self.df)
        return self._column_lineage

    def lint(self) -> LintReport:
        """Runs pre-flight compiler rules to detect performance hazards."""
        if self._lint_report is None:
            linter = PlanLinter()
            self._lint_report = linter.lint(self.graph, df=self.df)
        return self._lint_report

    def check_broadcast_opportunities(self, threshold_mb: int = 100) -> List[str]:
        """
        Inspect physical plan to check if small dimension tables triggered SortMergeJoin instead of BroadcastHashJoin.
        (Preserved for v0.1.0 backwards compatibility)
        """
        if self.df is None:
            return ["No active DataFrame bound to inspector."]

        try:
            plan_str = ""
            if hasattr(self.df, "_jdf"):
                plan_str = self.df._jdf.queryExecution().executedPlan().toString()
            if "SortMergeJoin" in plan_str and "BroadcastHashJoin" not in plan_str:
                rec = f"Consider broadcasting dimension tables under {threshold_mb}MB to avoid SortMergeJoin shuffle."
                if rec not in self.recommendations:
                    self.recommendations.append(rec)
        except Exception as e:
            self.recommendations.append(f"Plan inspection failed: {str(e)}")

        return self.recommendations

    def sanitize(self) -> "PlanInspector":
        """
        Redacts credentials, passwords, and sensitive PII literals from all plan nodes in memory.
        Uses the high-speed Rust sparklineage_core engine when available.
        """
        try:
            from . import sparklineage_core as rust_core
            redact_fn = rust_core.redact_secrets
        except ImportError:
            import re
            def redact_fn(s: str) -> str:
                s = re.sub(r'(?i)(jdbc:[a-z0-9_]+://[^:]+:)([^@\s]+)(@)', r'\g<1>********\g<3>', s)
                s = re.sub(r'(?i)(s3[a-z]?://)([^:]+:)([^@\s]+)(@)', r'\g<1>\g<2>********\g<4>', s)
                s = re.sub(r'(?i)\b(ssn|credit_card|password)\b[#0-9A-Z_]*\s*([=><!]+)\s*[\'"][^\'"]+[\'"]', r"\1 \2 '***REDACTED_PII***'", s)
                return s

        for node in self.graph.nodes.values():
            node.details = redact_fn(node.details)
            node.expressions = [redact_fn(expr) for expr in node.expressions]

        return self

    def show(self, sanitize: bool = True) -> None:
        """Prints a rich, formatted terminal report with credentials safely redacted."""
        if sanitize:
            self.sanitize()
        Reporter.print_report(
            self.graph,
            self.lint(),
            self.get_column_lineage()
        )

    def to_markdown(self, sanitize: bool = True) -> str:
        """Generates a Markdown diagnostic report with credentials safely redacted."""
        if sanitize:
            self.sanitize()
        return Reporter.to_markdown(
            self.graph,
            self.lint(),
            self.get_column_lineage()
        )

    def to_html(self, title: str = "Spark Execution & Lineage DAG", sanitize: bool = True) -> str:
        """Returns standalone interactive HTML document with credentials safely redacted."""
        if sanitize:
            self.sanitize()
        return Visualizer.generate_html(
            self.graph,
            self.get_column_lineage(),
            title=title
        )

    def save_html(self, filepath: str = "spark_lineage.html") -> str:
        """Saves interactive visual DAG to an HTML file."""
        html_content = self.to_html()
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html_content)
        return os.path.abspath(filepath)

    def display(self) -> Any:
        """
        Renders the interactive DAG inline in Jupyter or Databricks notebooks.
        Falls back to saving and printing if running outside a notebook.
        """
        html_content = self.to_html()

        # Check for Databricks displayHTML
        try:
            import builtins
            if hasattr(builtins, "displayHTML"):
                return builtins.displayHTML(html_content)
        except Exception:
            pass

        # Check for IPython notebook environment
        try:
            from IPython.display import HTML, display as ipy_display
            return ipy_display(HTML(html_content))
        except ImportError:
            pass

        # Fallback to saving and showing report
        out_path = self.save_html()
        self.show()
        print(f"Interactive DAG saved to: {out_path}")
        return out_path

    def summarize(self) -> Dict[str, Any]:
        """
        Returns an overview summary of diagnostic insights.
        (Preserved and enhanced for v0.1.0 compatibility)
        """
        lint_rep = self.lint()
        all_recs = list(self.recommendations)
        for issue in lint_rep.issues:
            if issue.recommendation not in all_recs:
                all_recs.append(issue.recommendation)

        return {
            "author": "Pavan Badempet",
            "documentation": "https://pavanbadempet.gitlab.io",
            "recommendations": all_recs,
            "metrics": lint_rep.summary_stats,
            "columns": list(self.get_column_lineage().keys()),
        }


def inspect(df: Any) -> PlanInspector:
    """Factory helper to inspect a DataFrame plan and lineage."""
    return PlanInspector(df)


def lint(df: Any) -> LintReport:
    """Runs pre-flight compiler linter on a DataFrame directly."""
    return PlanInspector(df).lint()
