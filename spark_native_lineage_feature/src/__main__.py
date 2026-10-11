"""
sparklineage CLI Entrypoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Provides fast, zero-JVM static code and plan linting directly from the command line.

Usage:
    sparklineage lint <path>
    sparklineage --version
"""

import os
import sys
import argparse
from sparklineage import __version__, lint_directory, lint_file


def main():
    parser = argparse.ArgumentParser(
        prog="sparklineage",
        description="Lightweight PySpark execution plan analysis, column lineage & pre-flight linter.",
    )
    parser.add_argument(
        "-v", "--version", action="version", version=f"sparklineage v{__version__}"
    )

    subparsers = parser.add_subparsers(dest="command")
    lint_parser = subparsers.add_parser("lint", help="Statically lint Python files or directories for PySpark anti-patterns")
    lint_parser.add_argument("target", help="Path to Python script or directory containing scripts")

    args = parser.parse_args()

    if args.command == "lint":
        target = args.target
        if os.path.isfile(target):
            issues = lint_file(target)
        elif os.path.isdir(target):
            issues = lint_directory(target)
        else:
            print(f"Error: Target path '{target}' does not exist.")
            sys.exit(1)

        if not issues:
            print(f"sparklineage: No PySpark anti-patterns detected in '{target}'. Clean! ✨")
            sys.exit(0)

        print(f"sparklineage: Found {len(issues)} issue(s) in '{target}':")
        for item in issues:
            if isinstance(item, dict):
                sev = f"[{item.get('severity', 'WARNING')}]"
                line = item.get("line") or item.get("line_number", "")
                fname = item.get("file") or item.get("filename", target)
                msg = item.get("message") or item.get("problem", "")
                rule = item.get("rule_id", "")
                loc = f"{fname}:{line}" if line else str(fname)
                print(f"  {sev:10} {loc:30} {rule}")
                print(f"             {msg}")
            else:
                sev = f"[{getattr(item, 'severity', 'WARNING')}]"
                print(f"  {sev:10} {getattr(item, 'problem', '')}")
                print(f"             Fix: {getattr(item, 'recommendation', '')}")
        sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
