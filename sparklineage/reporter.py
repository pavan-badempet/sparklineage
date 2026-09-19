import sys
from typing import Dict, Any
from .models import LintReport, ColumnLineage, Severity, PlanGraph


def _safe_print(text: str) -> None:
    """Prints text safely without crashing on restricted console encodings like cp1252."""
    try:
        print(text)
    except UnicodeEncodeError:
        encoding = getattr(sys.stdout, "encoding", "ascii") or "ascii"
        clean_text = text.encode(encoding, errors="replace").decode(encoding)
        print(clean_text)


class Reporter:
    """
    Formats plan analysis, linter diagnostics, and lineage into human-readable terminal or markdown summaries.
    """

    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"

    @classmethod
    def print_report(
        cls,
        graph: PlanGraph,
        lint_report: LintReport,
        column_lineage: Dict[str, ColumnLineage] = None
    ) -> None:
        """Prints rich colorized terminal report."""
        _safe_print(f"\n{cls.BOLD}{cls.CYAN}╭─────────────────────────────────────────────────────────────╮{cls.RESET}")
        _safe_print(f"{cls.BOLD}{cls.CYAN}│ ⚡ sparklineage v0.2.0 • Plan Diagnostic & Lineage Summary   │{cls.RESET}")
        _safe_print(f"{cls.BOLD}{cls.CYAN}╰─────────────────────────────────────────────────────────────╯{cls.RESET}\n")

        # Plan Architecture Metrics
        stats = lint_report.summary_stats
        _safe_print(f"{cls.BOLD}📊 Plan Metrics:{cls.RESET}")
        _safe_print(f"  • Plan Depth:       {cls.BOLD}{stats['depth']}{cls.RESET} stages")
        _safe_print(f"  • Shuffle Stages:   {cls.BOLD}{stats['shuffles']}{cls.RESET}")
        _safe_print(f"  • Joins Analyzed:   {cls.BOLD}{lint_report.joins_analyzed}{cls.RESET}")
        _safe_print(f"  • Issues Detected:  {cls.BOLD}{stats['total_issues']}{cls.RESET} ({stats['critical']} Critical, {stats['warnings']} Warnings)\n")

        # Linter Findings
        if lint_report.issues:
            _safe_print(f"{cls.BOLD}🚨 Pre-Flight Compiler Warnings:{cls.RESET}")
            for issue in lint_report.issues:
                color = cls.RED if issue.severity == Severity.CRITICAL else (cls.YELLOW if issue.severity == Severity.WARNING else cls.CYAN)
                badge = f"[{issue.severity.value}]"
                _safe_print(f"  {color}{cls.BOLD}{badge} {issue.title}{cls.RESET}")
                _safe_print(f"    {cls.DIM}Problem:{cls.RESET} {issue.message}")
                _safe_print(f"    {cls.GREEN}{cls.BOLD}Fix:{cls.RESET}     {issue.recommendation}\n")
        else:
            _safe_print(f"  {cls.GREEN}✔ No obvious Catalyst anti-patterns detected.{cls.RESET}\n")

        # Column Lineage Highlights
        if column_lineage:
            _safe_print(f"{cls.BOLD}🌱 Column-Level Lineage Mapping:{cls.RESET}")
            for col_name, lineage in list(column_lineage.items())[:10]:
                sources_str = ", ".join(lineage.sources) or "Direct Transformation"
                _safe_print(f"  • {cls.BOLD}{col_name}{cls.RESET} ⟵ {cls.CYAN}{sources_str}{cls.RESET}")
                if lineage.transformations:
                    _safe_print(f"    {cls.DIM}Transforms: {', '.join(lineage.transformations[:2])}{cls.RESET}")
            if len(column_lineage) > 10:
                _safe_print(f"  {cls.DIM}... and {len(column_lineage) - 10} more columns.{cls.RESET}")
            _safe_print("")

    @classmethod
    def to_markdown(
        cls,
        graph: PlanGraph,
        lint_report: LintReport,
        column_lineage: Dict[str, ColumnLineage] = None
    ) -> str:
        """Generates a GitHub-flavored Markdown report."""
        lines = [
            "# ⚡ sparklineage Plan Diagnostic Report",
            "",
            "## 📊 Metrics",
            f"- **Plan Depth:** {lint_report.plan_depth} stages",
            f"- **Shuffle Stages:** {lint_report.estimated_shuffles}",
            f"- **Joins Analyzed:** {lint_report.joins_analyzed}",
            f"- **Total Issues:** {len(lint_report.issues)}",
            "",
            "## 🚨 Pre-Flight Linter Findings",
        ]

        if not lint_report.issues:
            lines.append("No critical performance or execution hazards found.")
        else:
            for issue in lint_report.issues:
                lines.append(f"### `{issue.severity.value}` {issue.title}")
                lines.append(f"- **Issue:** {issue.message}")
                lines.append(f"- **Recommendation:** {issue.recommendation}")
                lines.append("")

        if column_lineage:
            lines.extend([
                "## 🌱 Column Lineage",
                "| Column | Upstream Sources | Transformations |",
                "| :--- | :--- | :--- |",
            ])
            for col, lin in column_lineage.items():
                sources = "<br>".join(lin.sources) or "Derived"
                transforms = "<br>".join(lin.transformations) or "Identity"
                lines.append(f"| `{col}` | {sources} | {transforms} |")

        return "\n".join(lines)
