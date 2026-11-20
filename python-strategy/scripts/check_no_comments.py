"""Forbid `#` comments in tracked Python files.

Tool directives (noqa, type:, pragma, fmt:, pyright:, ruff:, isort:, coding, shebang) are allowed.
"""

from __future__ import annotations

import io
import re
import subprocess
import sys
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIRECTIVE = re.compile(r"#\s*(noqa\b|type:|pragma\b|fmt:|pyright:|ruff:|isort:|-\*-|(vim?:.*)?coding[:=])")


def comment_lines(source: str) -> list[int]:
    lines = []
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type != tokenize.COMMENT:
            continue
        if (tok.start[0] == 1 and tok.string.startswith("#!")) or DIRECTIVE.match(tok.string):
            continue
        lines.append(tok.start[0])
    return lines


def tracked_files() -> list[str]:
    out = subprocess.run(["git", "ls-files", "*.py"], cwd=ROOT, check=True, capture_output=True, text=True)
    return sorted(out.stdout.split())


def main() -> int:
    found = 0
    for path in tracked_files():
        for line in comment_lines((ROOT / path).read_text(encoding="utf-8")):
            print(f"{path}:{line}: comment not allowed", file=sys.stderr)
            found += 1
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
