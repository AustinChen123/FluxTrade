import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "check_no_comments", Path(__file__).resolve().parents[1] / "scripts" / "check_no_comments.py"
)
assert _spec is not None and _spec.loader is not None
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


def test_comment_lines_skips_directives_and_strings() -> None:
    src = (
        "#!/usr/bin/env python\n"
        "# -*- coding: utf-8 -*-\n"
        "x = 1  # noqa: E501\n"
        "y: int = 2  # type: ignore\n"
        "s = '# not a comment'\n"
        "# real comment\n"
        "z = 3  # trailing\n"
    )
    assert mod.comment_lines(src) == [6, 7]


def test_check_ratchets_baseline() -> None:
    assert mod.check({"a.py": [1, 2]}, {"a.py": 2}) == []
    assert "baseline allows 1" in mod.check({"a.py": [1, 2]}, {"a.py": 1})[0]
    assert "stale" in mod.check({"a.py": [1]}, {"a.py": 2})[0]
    assert "baseline allows 0" in mod.check({"new.py": [3]}, {})[0]
    assert "no longer has comments" in mod.check({}, {"gone.py": 1})[0]
