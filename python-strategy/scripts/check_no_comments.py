"""Forbid `#` comments in tracked Python files.

Tool directives (noqa, type:, pragma, fmt:, pyright:, ruff:, isort:, coding, shebang) are allowed.
Files listed in comment_baseline.json may keep at most their recorded count; the count may
only go down. Run with --update to rewrite the baseline after removing comments.
"""

from __future__ import annotations

import io
import json
import re
import subprocess
import sys
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASELINE = Path(__file__).with_name("comment_baseline.json")
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


def check(counts: dict[str, list[int]], baseline: dict[str, int]) -> list[str]:
    errors = []
    for path, lines in counts.items():
        allowed = baseline.get(path, 0)
        if len(lines) > allowed:
            where = ", ".join(f"{path}:{n}" for n in lines[:5])
            errors.append(f"{path}: {len(lines)} comments, baseline allows {allowed} ({where})")
        elif len(lines) < allowed:
            errors.append(f"{path}: baseline {allowed} is stale ({len(lines)} now); run with --update")
    errors += [f"{path}: in baseline but no longer has comments; run with --update"
               for path in baseline if path not in counts]
    return errors


def main(argv: list[str]) -> int:
    counts = {}
    for path in tracked_files():
        lines = comment_lines((ROOT / path).read_text(encoding="utf-8"))
        if lines:
            counts[path] = lines
    if "--update" in argv:
        old = json.loads(BASELINE.read_text()) if BASELINE.exists() else {}
        grown = [p for p, ls in counts.items() if len(ls) > old.get(p, 0)]
        if old and grown and "--force" not in argv:
            print("refusing to grow baseline for: " + ", ".join(grown), file=sys.stderr)
            return 1
        BASELINE.write_text(json.dumps({p: len(ls) for p, ls in counts.items()}, indent=1, sort_keys=True) + "\n")
        return 0
    errors = check(counts, json.loads(BASELINE.read_text()))
    for err in errors:
        print(err, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
