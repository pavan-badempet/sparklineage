"""
High-speed static code scanner for PySpark pipelines and notebooks.
Scans source code for performance anti-patterns and memory hazards in microseconds.
Accelerated by Rust (sparklineage_core) with pure Python fallback.
"""

import os
from typing import List, Dict, Any

try:
    from . import sparklineage_core as rust_core
except ImportError:
    rust_core = None


def lint_code(code: str) -> List[Dict[str, Any]]:
    """
    Scans a Python code string for Spark performance anti-patterns.
    Returns list of issues with rule_id, severity, line_number, and message.
    """
    if rust_core is not None:
        raw_issues = rust_core.scan_code(code)
        return [
            {
                "rule_id": r_id,
                "severity": sev,
                "line": line_no,
                "message": msg,
            }
            for r_id, sev, line_no, msg in raw_issues
        ]

    # Pure Python fallback
    issues = []
    lines = code.splitlines()
    in_loop = False

    for idx, line in enumerate(lines, 1):
        trimmed = line.strip()
        if trimmed.startswith("for ") or trimmed.startswith("while "):
            in_loop = True
        elif not line.startswith(" ") and not line.startswith("\t"):
            in_loop = False

        if ".collect()" in trimmed:
            if in_loop:
                issues.append({
                    "rule_id": "CODE01_COLLECT_IN_LOOP",
                    "severity": "CRITICAL",
                    "line": idx,
                    "message": "Calling .collect() inside a loop pulls distributed data to driver repeatedly.",
                })
            else:
                issues.append({
                    "rule_id": "CODE01_COLLECT_USAGE",
                    "severity": "INFO",
                    "line": idx,
                    "message": "Consider .take(N) or .limit(N) instead of .collect() to avoid pulling full dataset.",
                })

        if ".crossJoin(" in trimmed:
            issues.append({
                "rule_id": "CODE02_EXPLICIT_CROSS_JOIN",
                "severity": "WARNING",
                "line": idx,
                "message": "Explicit crossJoin() causes O(N*M) Cartesian explosion.",
            })

        if ".toPandas()" in trimmed and ".limit(" not in trimmed:
            issues.append({
                "rule_id": "CODE03_UNBOUNDED_TO_PANDAS",
                "severity": "WARNING",
                "line": idx,
                "message": "Calling .toPandas() without .limit() pulls all distributed data to single node.",
            })

        if ".filter(" in trimmed and "cast(" in trimmed:
            issues.append({
                "rule_id": "CODE04_CAST_IN_FILTER",
                "severity": "WARNING",
                "line": idx,
                "message": "Type casting in .filter() prevents partition pushdown and Parquet file skipping.",
            })

    return issues


def lint_file(filepath: str) -> List[Dict[str, Any]]:
    """Lints a single Python source file or notebook."""
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        code = f.read()
    return lint_code(code)


def lint_directory(dirpath: str) -> Dict[str, List[Dict[str, Any]]]:
    """Recursively scans a directory for PySpark anti-patterns in parallel across all CPU cores."""
    if rust_core is not None and hasattr(rust_core, "scan_directory_parallel"):
        raw_map = rust_core.scan_directory_parallel(dirpath)
        formatted: Dict[str, List[Dict[str, Any]]] = {}
        for rel_path, issue_tuples in raw_map.items():
            formatted[rel_path] = [
                {
                    "rule_id": r_id,
                    "severity": sev,
                    "line": line_no,
                    "message": msg,
                }
                for r_id, sev, line_no, msg in issue_tuples
            ]
        return formatted

    # Pure Python fallback
    results = {}
    for root, _, files in os.walk(dirpath):
        for f in files:
            if f.endswith(".py") or f.endswith(".ipynb"):
                full_path = os.path.join(root, f)
                issues = lint_file(full_path)
                if issues:
                    rel_path = os.path.relpath(full_path, dirpath)
                    results[rel_path] = issues
    return results
